from __future__ import annotations

from dataclasses import dataclass


class TokenTooLongError(Exception):
    """A token exceeded the configured tokenizer limit."""


def _normalize_token_char(char: str) -> str:
    normalized = char.lower()
    if normalized == "ё":
        return "е"
    return normalized


@dataclass(frozen=True, slots=True)
class TokenCompletedEvent:
    token: str


@dataclass(frozen=True, slots=True)
class LineCompletedEvent:
    pass


TokenizerEvent = TokenCompletedEvent | LineCompletedEvent


class TextTokenizer:
    """Stateful tokenizer for decoded text chunks."""

    def __init__(self, *, max_token_length: int = 100_000) -> None:
        if max_token_length < 1:
            raise ValueError("max_token_length must be positive")
        self._pending_token_chars: list[str] = []
        self._pending_carriage_return = False
        self._line_has_content = False
        self._max_token_length = max_token_length

    def feed(self, text: str) -> list[TokenizerEvent]:
        events: list[TokenizerEvent] = []

        for char in text:
            if self._consume_pending_carriage_return(char, events):
                continue

            if char == "\r":
                self._emit_token(events)
                self._pending_carriage_return = True
                self._line_has_content = False
                continue

            if char == "\n":
                self._emit_token(events)
                events.append(LineCompletedEvent())
                self._line_has_content = False
                continue

            self._line_has_content = True

            if char.isalpha():
                if len(self._pending_token_chars) >= self._max_token_length:
                    raise TokenTooLongError("token length exceeds max_token_length")
                self._pending_token_chars.append(_normalize_token_char(char))
                continue

            self._emit_token(events)

        return events

    def finish(self) -> list[TokenizerEvent]:
        events: list[TokenizerEvent] = []

        self._consume_pending_carriage_return(None, events)
        self._emit_token(events)

        if self._line_has_content:
            events.append(LineCompletedEvent())
            self._line_has_content = False

        return events

    def _consume_pending_carriage_return(
        self,
        next_char: str | None,
        events: list[TokenizerEvent],
    ) -> bool:
        if not self._pending_carriage_return:
            return False

        self._pending_carriage_return = False
        events.append(LineCompletedEvent())

        return next_char == "\n"

    def _emit_token(self, events: list[TokenizerEvent]) -> None:
        if not self._pending_token_chars:
            return

        events.append(TokenCompletedEvent("".join(self._pending_token_chars)))
        self._pending_token_chars.clear()
