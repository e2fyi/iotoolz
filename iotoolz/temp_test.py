import gc

import pytest

from iotoolz.temp import TempStream


def test_tempstream_schemas():
    assert TempStream.supported_schemas == {"tmp", "temp"}


def test_tempstream_default():

    url = "tmp://foo.bar"
    expected_str = "hello\nworld"

    stream = TempStream(url, data=expected_str, mode="wb")
    assert stream.mode == "r"
    assert stream.read() == expected_str

    stream.seek(0)
    assert list(stream) == ["hello\n", "world"]

    stream.seek(0)
    assert stream.readline() == "hello\n"
    assert stream.readline() == "world"

    stream.seek(0)
    assert stream.read(5) == "hello"
    assert stream.read() == "\nworld"


def test_tempstream_binary():

    url = "tmp://foo.bar"
    expected_str = b"hello\nworld"

    stream = TempStream(url, expected_str, mode="w", chunk_size=5)
    assert stream.mode == "rb"
    assert stream.read() == expected_str

    stream.seek(0)
    assert list(stream) == [b"hello", b"\nworl", b"d"]

    stream.seek(0)
    assert stream.readline() == b"hello\n"
    assert stream.readline() == b"world"

    stream.seek(0)
    assert stream.read(5) == b"hello"
    assert stream.read() == b"\nworld"


def test_tempstream_write():

    url = "tmp://foo.bar"
    expected_str = "hello\nworld"

    with pytest.raises(IOError):
        stream = TempStream(url, expected_str)
        stream.write("foo bar"), "expect to be readonly"

    with pytest.raises(IOError):
        stream = TempStream(url, expected_str, mode="w")
        stream.write("foo bar")
        assert stream.mode == "r", "should be readonly if initial data is provided"

    stream = TempStream(url, mode="w")
    stream.write("foo bar")
    stream.seek(0)
    assert stream.read() == "foo bar"

    stream = TempStream(url, mode="wb")
    stream.write(b"foo bar")
    stream.seek(0)
    assert stream.read() == b"foo bar"


def test_tempstream_iter_dir():

    gc.collect()

    t1 = TempStream("tmp://foo/bar/data.txt", data="hello world1")
    t2 = TempStream("tmp://foo/data.txt", data="hello world2")
    t3 = TempStream("tmp://foo/bar/")

    assert list(t2.iter_dir()) == [t1, t2, t3]
    assert list(t3.iter_dir()) == [t1, t3]
    assert list(t3.iter_dir())[0].read() == "hello world1"


def test_tempstream_weakref():

    t1 = TempStream("tmp://foo/bar/", data="foo bar")
    TempStream("tmp://foo/bar/data.txt", data="hello world1")
    gc.collect()

    assert list(t1.iter_dir()) == [t1]

    t2 = TempStream.open("tmp://foo/bar/")
    assert t1 == t2
    assert t2.read() == "foo bar"


def test_pipe_basic():

    source = TempStream("tmp://source", data="foo bar")
    sink = source.pipe(TempStream("tmp://sink", mode="w"))
    sink.seek(0)
    assert sink.read() == "foo bar"


def test_pipe():
    source = TempStream("tmp://source", mode="wb")
    sink = source.pipe(TempStream("tmp://sink", mode="w"))
    source.write(b"foo")
    source.write(b" ")
    source.write(b"bar")
    sink.seek(0)
    assert sink.read() == "foo bar"
