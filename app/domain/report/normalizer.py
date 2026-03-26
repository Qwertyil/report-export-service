from __future__ import annotations

from functools import lru_cache

from pymorphy3 import MorphAnalyzer  # type: ignore[import-untyped]


def _normalize_token_text(token: str) -> str:
    return token.lower().replace("ё", "е")


def _contains_cyrillic(token: str) -> bool:
    return any(char == "ё" or "а" <= char <= "я" for char in token.lower())


@lru_cache(maxsize=1)
def _get_morph_analyzer() -> MorphAnalyzer:
    return MorphAnalyzer()


class LemmaNormalizer:
    def __init__(self, cache_size: int) -> None:
        if cache_size < 1:
            raise ValueError("cache_size must be positive")

        self._normalize_cached = lru_cache(maxsize=cache_size)(self._normalize_uncached)

    def normalize(self, token: str) -> str:
        return self._normalize_cached(token)

    def cache_info(self):
        return self._normalize_cached.cache_info()

    def _normalize_uncached(self, token: str) -> str:
        normalized_token = _normalize_token_text(token)
        if not _contains_cyrillic(normalized_token):
            return normalized_token

        lemma = _get_morph_analyzer().parse(normalized_token)[0].normal_form
        return _normalize_token_text(lemma)
