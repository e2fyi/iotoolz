from iotoolz.extensions import MinioStream


def test_minio_path():

    stream = MinioStream("minio://endpoint/bucket/folder/key.ext")
    assert stream.bucket == "bucket"
    assert stream.key == "folder/key.ext"
