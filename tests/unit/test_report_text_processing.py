from __future__ import annotations

import pytest

import app.domain.report.normalizer as normalizer_module
from app.domain.report.normalizer import LemmaNormalizer
from app.domain.report.tokenizer import (
    LineCompletedEvent,
    TextTokenizer,
    TokenCompletedEvent,
    TokenTooLongError,
)


def _collect_events(*chunks: str):
    tokenizer = TextTokenizer()
    events = []

    for chunk in chunks:
        events.extend(tokenizer.feed(chunk))

    events.extend(tokenizer.finish())
    return events


def test_tokenizer_handles_chunk_boundaries_and_final_line() -> None:
    events = _collect_events("При", "вет, ми", "р\nко", "шки и ко", "шку")

    assert events == [
        TokenCompletedEvent("привет"),
        TokenCompletedEvent("мир"),
        LineCompletedEvent(),
        TokenCompletedEvent("кошки"),
        TokenCompletedEvent("и"),
        TokenCompletedEvent("кошку"),
        LineCompletedEvent(),
    ]


def test_tokenizer_splits_hyphen_digits_and_normalizes_yo_with_crlf() -> None:
    events = _collect_events("foo-Бар42\r", "\nЁжик...HELLO")

    assert events == [
        TokenCompletedEvent("foo"),
        TokenCompletedEvent("бар"),
        LineCompletedEvent(),
        TokenCompletedEvent("ежик"),
        TokenCompletedEvent("hello"),
        LineCompletedEvent(),
    ]


def test_tokenizer_empty_file_and_blank_lines() -> None:
    assert _collect_events() == []
    assert _collect_events("\n", "\r", "\n") == [
        LineCompletedEvent(),
        LineCompletedEvent(),
    ]


def test_tokenizer_counts_last_line_without_tokens() -> None:
    assert _collect_events("  !!!") == [LineCompletedEvent()]


def test_tokenizer_fails_when_token_exceeds_limit() -> None:
    tokenizer = TextTokenizer(max_token_length=3)

    with pytest.raises(TokenTooLongError):
        tokenizer.feed("abcd")


def test_lemma_normalizer_collapses_word_forms_and_preserves_latin() -> None:
    normalizer = LemmaNormalizer(cache_size=16)

    assert normalizer.normalize("кошки") == "кошка"
    assert normalizer.normalize("КОШКУ") == "кошка"
    assert normalizer.normalize("RUNNING") == "running"
    assert normalizer.normalize("ЕЖИК") == "ежик"


def test_lemma_normalizer_skips_non_cyrillic_and_uses_cache(monkeypatch) -> None:
    class _FakeParse:
        def __init__(self, normal_form: str) -> None:
            self.normal_form = normal_form

    class _FakeMorphAnalyzer:
        def __init__(self) -> None:
            self.parse_calls: list[str] = []

        def parse(self, token: str) -> list[_FakeParse]:
            self.parse_calls.append(token)
            return [_FakeParse("Кошка")]

    fake_analyzer = _FakeMorphAnalyzer()
    monkeypatch.setattr(normalizer_module, "_get_morph_analyzer", lambda: fake_analyzer)

    normalizer = LemmaNormalizer(cache_size=2)

    assert normalizer.normalize("LATIN") == "latin"
    assert normalizer.normalize("LATIN") == "latin"
    assert fake_analyzer.parse_calls == []

    assert normalizer.normalize("Кошки") == "кошка"
    assert normalizer.normalize("Кошки") == "кошка"
    assert fake_analyzer.parse_calls == ["кошки"]
    assert normalizer.cache_info().maxsize == 2
