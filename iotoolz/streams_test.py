import io
import os.path

import requests_mock

from iotoolz.streams import (
    Stream,
    glob,
    iter_dir,
    mkdir,
    open_stream,
    set_buffer_rollover_size,
    set_schema_kwargs,
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
    with open_stream(dirpath / "example.py", "w") as stream:
        stream.write("hello")
    assert list(iter_dir(dirpath)) == [Stream(filepath), Stream(dirpath / "example.py")]

    # glob files
    assert list(glob(dirpath, "*.py")) == [Stream(dirpath / "example.py")]


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
        rmock.get(url, content=expected_bin)

        stream = Stream(url, mode="rb")
        assert stream.read() == expected_bin
        assert rmock.request_history[0].verify is False
