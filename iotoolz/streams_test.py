import os.path

from iotoolz.streams import Stream, glob, iter_dir, mkdir, open_stream


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
