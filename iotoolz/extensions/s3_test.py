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
        **extra_args
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

    assert list(S3Stream("s3://somebucket/foo/").iter_dir()) == [
        S3Stream("s3://somebucket/foo/bar.txt")
    ]
