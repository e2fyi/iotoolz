import os

import boto3
import pytest
from moto import mock_s3


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


def test_s3stream(s3):
    from iotoolz.extensions.s3 import S3Stream

    s3.create_bucket(Bucket="somebucket")
    extra_args = {
        "Tagging": "key1=value1&key2=value2",
        "Metadata": {"meta-key": "meta-value"},
    }

    assert not S3Stream("s3://somebucket/foo/bar.txt").exists()
    with S3Stream(
        "s3://somebucket/foo/bar.txt?StorageClass=REDUCED_REDUNDANCY",
        mode="w",
        content_type="text/plain",
        encoding="utf-8",
        **extra_args,
    ) as stream:
        stream.write("hello world")

    assert S3Stream("s3://somebucket/foo/bar.txt").exists()

    stats = S3Stream("s3://somebucket/foo/bar.txt").stats()
    assert stats.name == "bar.txt"
    assert stats.content_type == "text/plain"
    assert stats.encoding == "utf-8"

    with S3Stream("s3://somebucket/foo/bar.txt", mode="r") as stream:
        assert stream.read() == "hello world"
        assert stream.info.extras["Metadata"] == {"meta-key": "meta-value"}
        assert stream.info.extras["StorageClass"] == "REDUCED_REDUNDANCY"

    resp = s3.get_object_tagging(Bucket="somebucket", Key="foo/bar.txt")
    assert resp["TagSet"] == [
        {"Key": "key1", "Value": "value1"},
        {"Key": "key2", "Value": "value2"},
    ]


def test_s3stream_pathops(s3):
    from iotoolz.extensions.s3 import S3Stream

    s3.create_bucket(Bucket="somebucket")
    s3.create_bucket(Bucket="somebucket")
    S3Stream("s3://somebucket/foo/bar.txt", "w").save("foobar", close=True)

    assert S3Stream("s3://somebucket/foo/bar.txt").exists()
    assert S3Stream("s3://somebucket/foo/bar.txt").is_file()
    S3Stream("s3://somebucket/foo/bar.txt").unlink()
    assert not S3Stream("s3://somebucket/foo/bar.txt").exists()


def test_s3stream_dirops(s3):
    from iotoolz.extensions.s3 import S3Stream

    s3.create_bucket(Bucket="somebucket")
    S3Stream("s3://somebucket/foo/bar.txt", "w").save("foobar", close=True)
    S3Stream("s3://somebucket/foo/bar/text.txt", "w").save("foobar2", close=True)
    S3Stream("s3://somebucket/hello_world.txt", "w").save("hello", close=True)
    S3Stream("s3://somebucket/data.json", "w").save("{}", close=True)

    assert S3Stream("s3://somebucket/foo/").is_dir()
    assert list(S3Stream("s3://somebucket/foo/").iter_dir()) == [
        S3Stream("s3://somebucket/foo/bar.txt"),
        S3Stream("s3://somebucket/foo/bar/text.txt"),
    ]
    assert list(S3Stream("s3://somebucket/").glob("*/*.txt")) == [
        S3Stream("s3://somebucket/foo/bar.txt"),
        S3Stream("s3://somebucket/foo/bar/text.txt"),
    ]
    assert list(S3Stream("s3://somebucket/").glob("*.txt")) == [
        S3Stream("s3://somebucket/foo/bar.txt"),
        S3Stream("s3://somebucket/foo/bar/text.txt"),
        S3Stream("s3://somebucket/hello_world.txt"),
    ]

    S3Stream("s3://somebucket/foo/").rmdir()
    assert list(S3Stream("s3://somebucket/").glob("*.txt")) == [
        S3Stream("s3://somebucket/hello_world.txt"),
    ]
