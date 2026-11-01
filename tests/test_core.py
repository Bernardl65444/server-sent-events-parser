import unittest

from server_sent_events_parser import (
    Event,
    EventParser,
    parse_stream,
    parse_string,
)


class TestParseString(unittest.TestCase):
    def test_single_event(self):
        events = parse_string("data: hello\n\n")
        self.assertEqual(events, [Event(data="hello")])

    def test_multiline_data(self):
        events = parse_string("data: one\ndata: two\n\n")
        self.assertEqual(events, [Event(data="one\ntwo")])

    def test_event_name(self):
        events = parse_string("event: ping\ndata: {}\n\n")
        self.assertEqual(events, [Event(event="ping", data="{}")])

    def test_id_persists_across_events(self):
        events = parse_string("id: 7\ndata: a\n\ndata: b\n\n")
        self.assertEqual(events[0].id, "7")
        # The id is remembered for reconnection, so the second event
        # carries it too even though it was not repeated.
        self.assertEqual(events[1].id, "7")

    def test_retry_does_not_persist(self):
        events = parse_string("retry: 1000\ndata: a\n\ndata: b\n\n")
        self.assertEqual(events[0].retry, 1000)
        self.assertIsNone(events[1].retry)

    def test_comment_lines_ignored(self):
        events = parse_string(": keep-alive\ndata: x\n\n")
        self.assertEqual(events, [Event(data="x")])

    def test_blank_line_without_data_not_dispatched(self):
        # An event name alone must not produce an event.
        events = parse_string("event: ping\n\n")
        self.assertEqual(events, [])

    def test_crlf_line_endings(self):
        events = parse_string("data: a\r\ndata: b\r\n\r\n")
        self.assertEqual(events, [Event(data="a\nb")])

    def test_bare_cr_line_endings(self):
        events = parse_string("data: a\rdata: b\r\r")
        self.assertEqual(events, [Event(data="a\nb")])

    def test_field_without_colon(self):
        # A bare "data" line contributes an empty string.
        events = parse_string("data\n\n")
        self.assertEqual(events, [Event(data="")])

    def test_leading_space_stripped_once(self):
        events = parse_string("data:  two spaces\n\n")
        self.assertEqual(events, [Event(data=" two spaces")])

    def test_non_numeric_retry_ignored(self):
        events = parse_string("retry: soon\ndata: x\n\n")
        self.assertIsNone(events[0].retry)

    def test_id_with_nul_ignored(self):
        events = parse_string("id: bad\x00id\ndata: x\n\n")
        self.assertIsNone(events[0].id)

    def test_unknown_field_ignored(self):
        events = parse_string("color: blue\ndata: x\n\n")
        self.assertEqual(events, [Event(data="x")])

    def test_trailing_newline_stripped(self):
        # The spec strips exactly one trailing newline from the payload.
        events = parse_string("data: a\ndata:\n\n")
        self.assertEqual(events[0].data, "a")

    def test_final_event_without_blank_line(self):
        events = parse_string("data: tail")
        self.assertEqual(events, [Event(data="tail")])


class TestEventParserChunking(unittest.TestCase):
    def test_split_field_across_chunks(self):
        parser = EventParser()
        self.assertEqual(parser.feed(b"da"), [])
        self.assertEqual(parser.feed(b"ta: hi\n"), [])
        events = parser.feed(b"\n")
        self.assertEqual(events, [Event(data="hi")])

    def test_split_utf8_codepoint(self):
        # The euro sign is three bytes; split it across two chunks.
        parser = EventParser()
        payload = "data: €\n\n".encode("utf-8")
        events = parser.feed(payload[:1])
        events.extend(parser.feed(payload[1:]))
        self.assertEqual(events, [Event(data="€")])

    def test_close_flushes_pending(self):
        parser = EventParser()
        parser.feed(b"data: pending")
        self.assertEqual(parser.close(), [Event(data="pending")])

    def test_close_is_idempotent(self):
        parser = EventParser()
        parser.feed(b"data: x\n\n")
        self.assertEqual(parser.close(), [])
        self.assertEqual(parser.close(), [])

    def test_feed_rejects_str(self):
        parser = EventParser()
        with self.assertRaises(TypeError):
            parser.feed("data: x\n\n")


class TestParseStream(unittest.TestCase):
    def test_iterates_chunks(self):
        chunks = [b"data: a\n", b"\n", b"data: b\n\n"]
        self.assertEqual(
            list(parse_stream(chunks)),
            [Event(data="a"), Event(data="b")],
        )


class TestEventEquality(unittest.TestCase):
    def test_equality_and_hash(self):
        a = Event(event="e", data="d", id="1", retry=5)
        b = Event(event="e", data="d", id="1", retry=5)
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))

    def test_inequality(self):
        self.assertNotEqual(Event(data="a"), Event(data="b"))

    def test_not_equal_to_other_types(self):
        self.assertNotEqual(Event(data="a"), "a")


if __name__ == "__main__":
    unittest.main()
