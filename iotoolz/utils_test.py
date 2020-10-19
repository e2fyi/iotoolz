import io

import iotoolz.utils


def test_peek_stream():
    stream = io.StringIO("Hello world.")
    with iotoolz.utils.peek_stream(stream, peek=6) as pstream:
        assert pstream.read() == "world."
    assert pstream.tell() == 0
    assert pstream.read() == "Hello world."
