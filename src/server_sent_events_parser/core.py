"""Parse a Server-Sent Events (SSE) stream into discrete events.

The parser follows the WHATWG HTML specification for event stream
interpretation.  It is deliberately a *pull* parser: callers feed it raw
bytes and ask for the next complete event.  This keeps it usable with any
I/O strategy (sockets, files, test doubles) without the library owning a
thread or an event loop.

Two decisions are worth calling out because they are easy to get wrong:

* Lines are split on LF, CR, or CRLF.  The spec allows all three, and a
  stream that uses bare CR is legal.
* A leading colon marks a comment and is ignored entirely.  Comment lines
  are the standard keep-alive mechanism, so treating them as data would
  corrupt every long-lived connection.
"""

from __future__ import annotations

from typing import Iterable, Iterator, List, Optional

__all__ = ["Event", "EventParser", "parse_stream", "parse_string"]


class Event:
    """A single dispatched Server-Sent Event.

    ``data`` is the joined payload with trailing newlines stripped, as the
    specification requires.  ``retry`` is an integer number of milliseconds
    or ``None`` when the stream did not supply one.
    """

    __slots__ = ("event", "data", "id", "retry")

    def __init__(
        self,
        event: str = "message",
        data: str = "",
        id: Optional[str] = None,
        retry: Optional[int] = None,
    ) -> None:
        self.event = event
        self.data = data
        self.id = id
        self.retry = retry

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Event):
            return NotImplemented
        return (
            self.event == other.event
            and self.data == other.data
            and self.id == other.id
            and self.retry == other.retry
        )

    def __hash__(self) -> int:
        return hash((self.event, self.data, self.id, self.retry))

    def __repr__(self) -> str:
        return (
            "Event(event={!r}, data={!r}, id={!r}, retry={!r})".format(
                self.event, self.data, self.id, self.retry
            )
        )


class EventParser:
    """Incremental parser for a Server-Sent Events stream.

    Feed bytes with :meth:`feed`; receive zero or more :class:`Event`
    objects.  Incomplete trailing data is buffered until the next feed or
    until :meth:`close` flushes it.
    """

    def __init__(self) -> None:
        self._buffer = ""
        # Per-event state, reset after each dispatch.
        self._data_lines: List[str] = []
        self._event_type = ""
        self._last_id: Optional[str] = None
        self._retry: Optional[int] = None
        # ``id`` persists across events; ``retry`` does not reset either.
        self._current_id: Optional[str] = None

    def feed(self, chunk: bytes) -> List[Event]:
        """Consume a chunk of bytes and return any completed events.

        The chunk may split a line, a field, or even a UTF-8 code point;
        the parser buffers until it has a complete line.
        """
        if not isinstance(chunk, (bytes, bytearray)):
            raise TypeError("chunk must be bytes")
        # Decode incrementally so a multi-byte character split across two
        # chunks does not raise.  ``errors="replace"`` matches the spec's
        # tolerance for malformed input.
        self._buffer += bytes(chunk).decode("utf-8", errors="replace")
        events: List[Event] = []
        while True:
            line, sep, rest = _split_line(self._buffer)
            if not sep:
                break
            self._buffer = rest
            event = self._process_line(line)
            if event is not None:
                events.append(event)
        return events

    def close(self) -> List[Event]:
        """Flush any buffered data as a final event.

        A stream that ends without a trailing blank line still dispatches
        the pending event, per the spec's end-of-file handling.
        """
        events: List[Event] = []
        if self._buffer:
            # A final line without a terminator is still a line.
            line = self._buffer
            self._buffer = ""
            event = self._process_line(line)
            if event is not None:
                events.append(event)
        event = self._dispatch()
        if event is not None:
            events.append(event)
        return events

    def _process_line(self, line: str) -> Optional[Event]:
        if line == "":
            return self._dispatch()
        if line.startswith(":"):
            # Comment / keep-alive.  Ignored by design.
            return None
        if ":" in line:
            field, _, value = line.partition(":")
            # A single leading space after the colon is stripped.
            if value.startswith(" "):
                value = value[1:]
        else:
            field, value = line, ""
        self._apply_field(field, value)
        return None

    def _apply_field(self, field: str, value: str) -> None:
        if field == "event":
            self._event_type = value
        elif field == "data":
            self._data_lines.append(value)
        elif field == "id":
            # The spec says a NUL in an id is ignored.
            if "\x00" not in value:
                self._current_id = value
        elif field == "retry":
            if value.isdigit():
                self._retry = int(value)
        # Unknown fields are ignored, as required.

    def _dispatch(self) -> Optional[Event]:
        # An event with no data lines is not dispatched, even if it had an
        # event name or id.  This is the spec's rule and it matters: a
        # stray blank line should not produce an empty event.
        if not self._data_lines:
            self._reset_event_state()
            return None
        data = "\n".join(self._data_lines)
        # Strip exactly one trailing newline, per the spec.
        if data.endswith("\n"):
            data = data[:-1]
        event = Event(
            event=self._event_type or "message",
            data=data,
            id=self._current_id,
            retry=self._retry,
        )
        self._reset_event_state()
        return event

    def _reset_event_state(self) -> None:
        self._data_lines = []
        self._event_type = ""
        self._retry = None
        # ``_current_id`` intentionally persists: the spec says the last
        # event ID is remembered for reconnection.


def _split_line(text: str):
    """Return (line, separator, remainder) for the first line in *text*.

    Recognises LF, CR, and CRLF.  When no terminator is present, separator
    is the empty string and the whole text is returned as the line.
    """
    for i, ch in enumerate(text):
        if ch == "\n":
            return text[:i], "\n", text[i + 1 :]
        if ch == "\r":
            if i + 1 < len(text):
                if text[i + 1] == "\n":
                    return text[:i], "\r\n", text[i + 2 :]
                return text[:i], "\r", text[i + 1 :]
            # CR at the very end: could be CRLF split across chunks.
            # Treat it as a terminator; the next chunk's LF will be
            # consumed as an empty line, which the spec tolerates.
            return text[:i], "\r", text[i + 1 :]
    return text, "", ""


def parse_stream(stream: Iterable[bytes]) -> Iterator[Event]:
    """Yield events from an iterable of byte chunks.

    Convenience wrapper around :class:`EventParser` for callers who just
    want to iterate.  The final event is flushed when the iterable ends.
    """
    parser = EventParser()
    for chunk in stream:
        for event in parser.feed(chunk):
            yield event
    for event in parser.close():
        yield event


def parse_string(text: str) -> List[Event]:
    """Parse a complete SSE document held in memory.

    Useful for tests and for parsing a cached response body.
    """
    parser = EventParser()
    events = parser.feed(text.encode("utf-8"))
    events.extend(parser.close())
    return events
