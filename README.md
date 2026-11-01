# server-sent-events-parser

Parses a Server-Sent Events stream, decoding event name, data payloads, IDs, and retry intervals.

```python
from server_sent_events_parser import EventParser

parser = EventParser()
for chunk in [b"event: tick\n", b"data: 1\n", b"\n"]:
    for event in parser.feed(chunk):
        print(event.event, event.data, event.id, event.retry)
for event in parser.close():
    print(event)
```

If you already hold the whole response body, `parse_string` returns a list
of events directly, and `parse_stream` yields events from any iterable of
byte chunks:

```python
from server_sent_events_parser import parse_string, parse_stream

print(parse_string("data: hello\n\ndata: world\n\n"))
print(list(parse_stream([b"data: a\n\n", b"data: b\n\n"])))
```

## Why this exists

Most SSE clients are built into HTTP libraries and only work with that
library's response object.  This package is the opposite: it is a pure
state machine over bytes, so it works with `socket`, `http.client`, a
file handle, or a test fixture without dragging in a networking stack.

The trade-off is that you own the I/O loop.  There is no reconnect logic,
no keep-alive timer, and no thread.  The parser only turns bytes into
`Event` objects; deciding when to reconnect and what to do with the last
ID is the caller's job.  That keeps the library small enough to audit and
usable from both sync and async code.

## The awkward edge

Chunk boundaries do not respect line or character boundaries.  A caller
that reads 1 byte at a time will hand the parser a partial UTF-8 code
point, and a caller that reads in 64 KiB blocks will hand it half a line.
The parser buffers both cases, so `feed` never raises on a partial
character and never emits a half-line.  The one thing it cannot recover
from is a stream that ends mid-line: `close` treats the leftover bytes as
a final line, which matches the specification's end-of-file handling.

A second edge worth knowing: `id` values persist across events (the
specification remembers the last ID for reconnection) while `retry` does
not.  If you want a per-event retry, read `event.retry` and do not assume
it is carried forward.

## API

- `Event` — attributes `event` (str, defaults to `"message"`), `data`
  (str), `id` (str or `None`), `retry` (int milliseconds or `None`).
  Supports `==` and `hash`.
- `EventParser()` — `feed(chunk: bytes) -> list[Event]` and
  `close() -> list[Event]`.
- `parse_stream(chunks) -> Iterator[Event]` — accepts any iterable of
  bytes.
- `parse_string(text: str) -> list[Event]` — parses a complete document.
