import requests  # noqa
import requests_mock

from iotoolz.http import HttpStream


def test_httpstream_read_default():
    url = "https://foo.bar"
    expected_bin = b"hello\nworld"

    with requests_mock.Mocker() as rmock:
        rmock.get(url, content=expected_bin)

        stream = HttpStream(url)
        assert stream.read() == expected_bin.decode()

        stream.seek(0)
        assert list(stream) == ["hello\n", "world"]

        stream.seek(0)
        assert stream.readline() == "hello\n"
        assert stream.readline() == "world"

        stream.seek(0)
        assert stream.read(5) == "hello"
        assert stream.read() == "\nworld"


def test_httpstream_read_bin():
    url = "https://foo.bar"
    expected_bin = b"hello\nworld"

    with requests_mock.Mocker() as rmock:
        rmock.get(url, content=expected_bin)

        stream = HttpStream(url, mode="rb", chunk_size=5, verify=False)
        assert stream.read() == expected_bin
        assert rmock.request_history[0].verify is False

        stream.seek(0)
        assert list(stream) == [b"hello", b"\nworl", b"d"]

        stream.seek(0)
        assert stream.readline() == b"hello\n"
        assert stream.readline() == b"world"

        stream.seek(0)
        assert stream.read(5) == b"hello"
        assert stream.read() == b"\nworld"


def test_httpstream_write_put(mocker):
    url = "https://foo.bar"
    uploaded_content = None

    def side_effect(url, *args, **kwargs):
        nonlocal uploaded_content
        uploaded_content = kwargs["data"].read()
        return mocker.MagicMock()

    mocker.patch("requests.put", side_effect=side_effect)
    with HttpStream(url, mode="wb") as stream:
        stream.write("hello\n")
        stream.write("world")

    assert uploaded_content == b"hello\nworld"


def test_httpstream_write_post(mocker):
    url = "https://foo.bar"
    uploaded_content = None

    def side_effect(url, *args, **kwargs):
        nonlocal uploaded_content
        uploaded_content = kwargs["data"].read()
        return mocker.MagicMock()

    mocker.patch("requests.post", side_effect=side_effect)
    with HttpStream(url, mode="wb", use_post=True) as stream:
        stream.write("hello\n")
        stream.write("world")

    assert uploaded_content == b"hello\nworld"
