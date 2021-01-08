"""This module is for streams that requires optional dependencies - i.e. boto3 for S3Stream."""
import importlib
import importlib.util

from iotoolz.extensions._not_implemented import mock_stream

BOTO3_EXIST = importlib.util.find_spec("boto3") is not None
MINIO_EXIST = importlib.util.find_spec("minio") is not None

S3Stream = (
    importlib.import_module("iotoolz.extensions.s3").S3Stream  # type: ignore
    if BOTO3_EXIST
    else mock_stream(
        supported_schemas={"s3", "s3a", "s3n"},
        msg="S3Stream is not available because 'boto3' is not installed. "
        "You can install 'boto3' yourself or use the command 'pip install iotoolz[boto3]' "
        "when installing iotoolz.",
    )
)

MinioStream = (
    importlib.import_module("iotoolz.extensions.minio").MinioStream  # type: ignore
    if MINIO_EXIST
    else mock_stream(
        supported_schemas={"minio"},
        msg="MinioStream is not available because 'minio' is not installed. "
        "You can install 'minio' yourself or use the command 'pip install iotoolz[minio]' "
        "when installing iotoolz.",
    )
)
