import io
import os.path

import pytest
import requests
import requests.exceptions
import requests_mock

from iotoolz import StreamInfo
from iotoolz.streams import (
    Stream,
    exists,
    glob,
    iter_dir,
    mkdir,
    open_stream,
    rmdir,
    set_buffer_rollover_size,
    set_schema_kwargs,
    stats,
    unlink,
)


def test_streams(tmpdir):

    assert open_stream == Stream, "Stream is alias of open_stream"

    # test mkdir
    dirpath = tmpdir / "data"
    assert not os.path.exists(dirpath)
    mkdir(dirpath)
    assert os.path.exists(dirpath)

    # test reading and writing
    filepath = tmpdir / "data" / "foo.txt"
    stream = Stream(filepath, "rw")
    stream.write("hello world")
    stream.save()
    assert os.path.isfile(filepath)
    assert Stream(filepath).read() == "hello world"

    # append to buffer
    stream.seek(0)  # go to start of buffer
    assert stream.read() == "hello world"
    stream.seek(0, whence=2)
    stream.write("\nline2")  # append to buffer
    stream.save()
    stream.close()
    assert stream.closed
    assert Stream(filepath).read() == "hello world\nline2"

    # list files
    assert not exists(dirpath / "example.py")
    with open_stream(dirpath / "example.py", "w") as stream:
        stream.write("hello")
    assert exists(dirpath / "example.py")
    assert list(iter_dir(dirpath)) == [Stream(filepath), Stream(dirpath / "example.py")]

    # glob files
    assert list(glob(dirpath / "*.py")) == [Stream(dirpath / "example.py")]

    # unlink files
    unlink(dirpath / "example.py")
    assert not exists(dirpath / "example.py")
    unlink(dirpath / "example.py")  # shld not raise exception
    with pytest.raises(FileNotFoundError):
        unlink(dirpath / "example.py", missing_ok=False)

    # unlink files
    assert exists(Stream(filepath))
    unlink(Stream(filepath))
    assert not exists(Stream(filepath))

    # rmdir
    mkdir(dirpath / "foo")
    open_stream(dirpath / "foo" / "abc.txt", "w").save("hello", close=True)
    assert len(list(iter_dir(dirpath / "foo"))) > 0
    rmdir(dirpath / "foo")
    assert len(list(iter_dir(dirpath))) == 0


def test_buffer_rollover(tmpdir):

    set_buffer_rollover_size(1)

    with open_stream(tmpdir / "foo.txt", mode="w") as stream:
        assert isinstance(stream._file._file, io.BytesIO)
        stream.write("hello world")
        assert isinstance(stream._file._file, io.BufferedRandom)


def test_open_http_stream():
    url = "https://foo.bar"
    expected_bin = b"hello\nworld"

    with requests_mock.Mocker() as rmock:
        rmock.put(url)
        with open_stream(url, mode="w") as stream:
            stream.write("hello world")
        assert len(rmock.request_history) == 1

    set_schema_kwargs("https", verify=False, use_post=True)

    with requests_mock.Mocker() as rmock:
        rmock.post(url)
        with open_stream(url, mode="w") as stream:
            stream.write("hello world")
        assert len(rmock.request_history) == 1

    with requests_mock.Mocker() as rmock:
        rmock.head(url, headers={"content-type": "text/plain", "ETag": "123"})
        rmock.get(url, content=expected_bin)

        stream = Stream(url, mode="rb")
        assert stream.stats() == StreamInfo(
            uri=url,
            name="foo.bar",
            content_type="text/plain",
            encoding="ISO-8859-1",
            etag="123",
        )
        assert stream.read() == expected_bin
        assert rmock.request_history[0].verify is False

    with requests_mock.Mocker() as rmock:
        rmock.head(url)
        rmock.delete(url)

        Stream(url).unlink()
        assert rmock.last_request.method == "DELETE"

    with requests_mock.Mocker() as rmock:
        rmock.head(url)
        rmock.delete(url, exc=requests.exceptions.RequestException)

        # should not throw error
        Stream(url).unlink()

        # should throw error
        with pytest.raises(requests.exceptions.RequestException):
            Stream(url).unlink(missing_ok=False)


def test_temp_stream():

    stream1 = Stream(
        "tmp://foo/bar.txt",
        "rw",
        data="foo",
        content_type="text/plain",
        encoding="utf-8",
    )
    stream1.seek(0, whence=2)
    stream1.write(" bar")
    assert stats("tmp://foo/bar.txt") == StreamInfo(
        uri="tmp://foo/bar.txt",
        name="bar.txt",
        encoding="utf-8",
        content_type="text/plain",
    )

    stream2 = Stream("tmp://foo/bar.txt")
    assert stream1 == stream2
    assert stream1.tell() == stream2.tell()
    stream2.seek(0)
    assert stream2.read() == "foo bar"

    stream3 = Stream("tmp://foo/bar.csv", data="hello")

    assert list(glob("tmp://foo/*.csv")) == [stream3]

    stream3.unlink()
    assert Stream("tmp://foo/bar.csv").read() == ""
