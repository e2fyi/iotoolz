import io
import tarfile

import pytest

from iotoolz.file import FileStream


@pytest.fixture(scope="session")
def sample_text(tmp_path_factory):
    data = "hello\nworld"
    text_path = tmp_path_factory.mktemp("iotoolz") / "sample_text.txt"
    with open(text_path, "w") as stream:
        stream.write(data)
    return text_path


@pytest.fixture(scope="session")
def sample_tarball(tmp_path_factory):
    data = b"hello\nworld"
    fileobj = io.BytesIO(data)
    tar_path = tmp_path_factory.mktemp("iotoolz") / "sample_tar.tar"
    with tarfile.open(tar_path, mode="w") as ref:
        info = tarfile.TarInfo(name="sample_data.txt")
        info.size = len(data)
        ref.addfile(info, fileobj)
    return tar_path


def test_filestream_supported_schemas(sample_text):
    assert FileStream.supported_schemas == {"", "file"}


def test_filestream_read_text(sample_text):
    expected_bin = "hello\nworld"

    stream = FileStream(sample_text)
    assert stream.read() == expected_bin
    assert stream.encoding == "utf-8"
    assert stream.content_type == "text/plain"

    stream.seek(0)
    assert list(stream) == ["hello\n", "world"]

    stream.seek(0)
    assert stream.readline() == "hello\n"
    assert stream.readline() == "world"

    stream.seek(0)
    assert stream.read(5) == "hello"
    assert stream.read() == "\nworld"


def test_filestream_read_tar(sample_tarball):
    expected_bin = b"hello\nworld"

    stream = FileStream(sample_tarball, mode="rb")
    with tarfile.open(fileobj=stream, mode="r") as ref:
        member = ref.getmember("sample_data.txt")
        tfile = ref.extractfile(member)
        assert tfile.read() == expected_bin

    assert stream.encoding == "utf-8"
    assert stream.content_type == "application/x-tar"


def test_filestream_write(tmpdir):
    expected_bin = b"hello\nworld"

    dst_path = tmpdir / "iotoolz" / "example.txt"
    with FileStream(dst_path, mode="wb") as stream:
        stream.write(expected_bin)

    with open(dst_path, "rb") as stream:
        assert stream.read() == expected_bin
