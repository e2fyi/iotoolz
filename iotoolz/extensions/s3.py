"""This module implements the FileStream with python native "open" method."""
import datetime
import io
import os.path
import urllib.parse
from typing import IO, Any, Dict, Iterable, Tuple, Type, Union

try:
    import boto3
    import boto3.s3.transfer
    import botocore.errorfactory
except ImportError as error:
    raise ImportError(
        "S3Stream cannot be used because 'boto3' is not installed. "
        "You can install boto3 by running the command: 'pip install iotoolz[boto3]'"
    ) from error

from iotoolz._abc import AbcStream, StreamInfo
from iotoolz._toolz import toolz
from iotoolz.utils import guess_filename

ALLOWED_DOWNLOAD_ARGS = frozenset(boto3.s3.transfer.S3Transfer.ALLOWED_DOWNLOAD_ARGS)
ALLOWED_UPLOAD_ARGS = frozenset(boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS)
ALLOWED_HEAD_ARGS = {
    "VersionId",
    "SSECustomerAlgorithm",
    "SSECustomerKey",
    "RequestPayer",
}
ALLOWED_DELETE_ARGS = {
    "VersionId",
    "RequestPayer",
    "BypassGovernanceRetention",
    "ExpectedBucketOwner",
}


class S3Stream(AbcStream):
    """S3Stream is the stream interface to AWS S3 object store.

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer
    """

    supported_schemas = {"s3", "s3a", "s3n"}
    _default_client: boto3.client = None
    _default_transfer_config = boto3.s3.transfer.TransferConfig()
    _default_extra_upload_args: Dict[str, Any] = {}
    _default_extra_download_args: Dict[str, Any] = {}

    def __init__(
        self,
        uri: str,
        mode: str = "r",
        buffering: int = -1,
        encoding: str = None,
        newline: str = None,
        content_type: str = "",
        inmem_size: int = None,
        delimiter: Union[str, bytes] = None,
        chunk_size: int = io.DEFAULT_BUFFER_SIZE,
        client: boto3.client = None,
        multipart_threshold: int = None,
        max_concurrency: int = None,
        multipart_chunksize: int = None,
        num_download_attempts: int = None,
        max_io_queue: int = None,
        io_chunksize: int = None,
        use_threads: bool = None,
        **kwargs,
    ):
        """
        Creates a new instance of S3Stream.

        See also: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer

        Args:
            uri (str): uri string to the resource.
            mode (str, optional): same as "open" - supports depends on the actual implementation. Defaults to "r".
            buffering (int, optional): same as "open". Defaults to -1.
            encoding (str, optional): encoding used to decode bytes to str. Defaults to None.
            newline (str, optional): same as "open". Defaults to None.
            content_type (str, optional): mime type for the resource. Defaults to "".
            inmem_size (int, optional): max size before buffer rollover from mem to disk. Defaults to None (i.e. never - may raise MemoryError).
            delimiter (Union[str, bytes], optional): delimiter used for determining line boundaries. Defaults to None.
            chunk_size (int, optional): chunk size when iterating bytes stream. Defaults to io.DEFAULT_BUFFER_SIZE.
            client (boto3.client, optional): use the provided boto3 client to interface with S3. Defaults to None.
            multipart_threshold (int, optional): The transfer size threshold for which multipart uploads, downloads, and copies will automatically be triggered. Defaults to 8388608.
            max_concurrency (int, optional): The maximum number of threads that will be making requests to perform a transfer. If use_threads is set to False, the value provided is ignored as the transfer will only ever use the main thread. Defaults to 10.
            multipart_chunksize (int, optional): The partition size of each part for a multipart transfer. Defaults to 8388608.
            num_download_attempts (int, optional): The number of download attempts that will be retried upon errors with downloading an object in S3. Note that these retries account for errors that occur when streaming down the data from s3 (i.e. socket errors and read timeouts that occur after receiving an OK response from s3). Other retryable exceptions such as throttling errors and 5xx errors are already retried by botocore (this default is 5). This does not take into account the number of exceptions retried by botocore. Defaults to 5.
            max_io_queue (int, optional): The maximum amount of read parts that can be queued in memory to be written for a download. The size of each of these read parts is at most the size of io_chunksize. Defaults to 100.
            io_chunksize (int, optional): The max size of each chunk in the io queue. Currently, this is size used when read is called on the downloaded stream as well. Defaults to 262144.
            use_threads (bool, optional): If True, threads will be used when performing S3 transfers. If False, no threads will be used in performing transfers: all logic will be ran in the main thread. Defaults to True.
            **kwargs: Additional ExtraArgs which will be passed to the 'boto3.s3.transfer.S3Transfer' client.
        """
        super().__init__(
            uri,
            mode,
            buffering,
            encoding,
            newline,
            content_type,
            inmem_size,
            delimiter,
            chunk_size,
            **kwargs,
        )
        self._client = client or self._default_client or boto3.client("s3")
        self._transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=multipart_threshold
            or self._default_transfer_config.multipart_threshold,
            max_concurrency=max_concurrency
            or self._default_transfer_config.max_request_concurrency,
            multipart_chunksize=multipart_chunksize
            or self._default_transfer_config.multipart_chunksize,
            num_download_attempts=num_download_attempts
            or self._default_transfer_config.num_download_attempts,
            max_io_queue=max_io_queue
            or self._default_transfer_config.max_io_queue_size,
            io_chunksize=io_chunksize or self._default_transfer_config.io_chunksize,
            use_threads=use_threads or self._default_transfer_config.use_threads,
        )
        self._scheme, self.bucket, self.key, query, _ = urllib.parse.urlsplit(uri)
        if self.key.startswith("/"):
            self.key = self.key[1:]
        # keep last query value
        query_str = {
            key: value[-1]
            for key, value in urllib.parse.parse_qs(query).items()
            if value
        }
        kwargs = {**kwargs, **query_str}
        extra_download_args = {
            key: value for key, value in kwargs.items() if key in ALLOWED_DOWNLOAD_ARGS
        }
        self._extra_download_args: Dict[str, Any] = {
            **self._default_extra_download_args,
            **extra_download_args,
        }
        extra_upload_args = {
            key: value for key, value in kwargs.items() if key in ALLOWED_UPLOAD_ARGS
        }
        self._extra_upload_args: Dict[str, Any] = {
            **self._default_extra_upload_args,
            **extra_upload_args,
        }
        self._head_args = {
            key: value for key, value in kwargs.items() if key in ALLOWED_HEAD_ARGS
        }
        self._delete_args = {
            key: value for key, value in kwargs.items() if key in ALLOWED_DELETE_ARGS
        }

    def read_to_iterable_(
        self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
    ) -> Tuple[Iterable[bytes], StreamInfo]:
        """Downloads the S3 object to buffer with 'boto3.s3.transfer.S3Transfer'."""
        resp = self._client.head_object(
            Bucket=self.bucket, Key=self.key, **self._extra_download_args
        )
        etag = resp.get("ETag", "").strip('"')
        encoding = resp.get("ContentEncoding", self.encoding)
        content_type = resp.get("ContentType", self.content_type)
        last_modified = resp.get("LastModified")

        self._client.download_fileobj(
            self.bucket,
            self.key,
            fileobj,
            ExtraArgs=self._extra_download_args,
            Config=self._transfer_config,
        )
        fileobj.seek(0)  # reset to initial counter
        return (
            [],
            StreamInfo(
                content_type=content_type,
                encoding=encoding,
                etag=etag,
                last_modified=last_modified,
                extras=resp,
            ),
        )

    def write_from_fileobj_(
        self, uri: str, fileobj: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        """Uploads the data in the buffer with 'boto3.s3.transfer.S3Transfer'."""
        self._client.upload_fileobj(
            fileobj,
            self.bucket,
            self.key,
            ExtraArgs={
                "ContentType": self.content_type,
                "ContentEncoding": self.encoding,
                **self._extra_upload_args,
            },
            Config=self._transfer_config,
        )
        return StreamInfo(last_modified=datetime.datetime.now())

    def stats_(self) -> StreamInfo:
        resp = self._client.head_object(
            Bucket=self.bucket, Key=self.key, **self._head_args
        )
        return StreamInfo(
            content_type=resp.get("ContentType"),
            encoding=resp.get("ContentEncoding"),
            etag=resp.get("ETag", "").strip('"'),
            last_modified=resp.get("LastModified"),
            extras=resp,
        )

    def exists(self) -> bool:
        """Whether the stream points to an existing resource."""
        try:
            self.set_info(self.stats_())
            return True
        except botocore.errorfactory.ClientError:
            return False

    def is_dir(self) -> bool:
        """Whether stream points to a existing dir."""
        return self.uri.endswith("/")

    def is_file(self) -> bool:
        """Whether stream points to a existing file."""
        return self.exists()

    def unlink(self, missing_ok: bool = True, **kwargs):
        try:
            kwargs = {**self._delete_args, **kwargs}
            self._client.delete_object(
                Bucket=self.bucket, Key=self.key, **kwargs,
            )
        except botocore.errorfactory.ClientError:
            if not missing_ok:
                raise

    def rmdir(self, ignore_errors: bool = False, **kwargs) -> "S3Stream":
        """Remove the entire directory."""
        try:
            kwargs = {**self._delete_args, **kwargs}
            batched = toolz.partition_all(1000, self.iter_dir())
            for batch in batched:
                self._client.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": [{"Key": stream.key} for stream in batch]},
                    **kwargs,
                )
        except botocore.errorfactory.ClientError:
            if not ignore_errors:
                raise
        return self

    @classmethod
    def set_default_client(cls, client: boto3.client) -> Type["S3Stream"]:
        """
        Set the default boto3 client to use.

        Args:
            client (boto3.client): boto3 client object.
        """
        cls._default_client = client
        return cls

    @classmethod
    def set_default_transfer_config(
        cls,
        multipart_threshold: int = 8388608,
        max_concurrency: int = 10,
        multipart_chunksize: int = 8388608,
        num_download_attempts: int = 5,
        max_io_queue: int = 100,
        io_chunksize: int = 262144,
        use_threads: bool = True,
    ) -> Type["S3Stream"]:
        """
        Set the default transfer config.

        See also https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig

        Args:
            multipart_threshold (int, optional): The transfer size threshold for which multipart uploads, downloads, and copies will automatically be triggered. Defaults to 8388608.
            max_concurrency (int, optional): The maximum number of threads that will be making requests to perform a transfer. If use_threads is set to False, the value provided is ignored as the transfer will only ever use the main thread. Defaults to 10.
            multipart_chunksize (int, optional): The partition size of each part for a multipart transfer. Defaults to 8388608.
            num_download_attempts (int, optional): The number of download attempts that will be retried upon errors with downloading an object in S3. Note that these retries account for errors that occur when streaming down the data from s3 (i.e. socket errors and read timeouts that occur after receiving an OK response from s3). Other retryable exceptions such as throttling errors and 5xx errors are already retried by botocore (this default is 5). This does not take into account the number of exceptions retried by botocore. Defaults to 5.
            max_io_queue (int, optional): The maximum amount of read parts that can be queued in memory to be written for a download. The size of each of these read parts is at most the size of io_chunksize. Defaults to 100.
            io_chunksize (int, optional): The max size of each chunk in the io queue. Currently, this is size used when read is called on the downloaded stream as well. Defaults to 262144.
            use_threads (bool, optional): If True, threads will be used when performing S3 transfers. If False, no threads will be used in performing transfers: all logic will be ran in the main thread. Defaults to True.
        """

        cls._default_transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=multipart_threshold,
            max_concurrency=max_concurrency,
            multipart_chunksize=multipart_chunksize,
            num_download_attempts=num_download_attempts,
            max_io_queue=max_io_queue,
            io_chunksize=io_chunksize,
            use_threads=use_threads,
        )
        return cls

    @classmethod
    def set_default_download_args(cls, **kwargs) -> Type["S3Stream"]:
        """
        Set the default ExtraArgs to the 'boto3.s3.transfer.S3Transfer' download client.

        See also: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer

        The available kwargs are:

        'VersionId', 'SSECustomerAlgorithm', 'SSECustomerKey', 'SSECustomerKeyMD5',
        'RequestPayer'
        """
        cls._default_extra_download_args = {
            key: value for key, value in kwargs.items() if key in ALLOWED_DOWNLOAD_ARGS
        }
        return cls

    @classmethod
    def set_default_upload_args(cls, **kwargs) -> Type["S3Stream"]:
        """
        Set the default ExtraArgs to the 'boto3.s3.transfer.S3Transfer' upload client.

        See also: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer

        The available kwargs are:

        'ACL', 'CacheControl', 'ContentDisposition', 'ContentEncoding',
        'ContentLanguage', 'ContentType', 'Expires', 'GrantFullControl', 'GrantRead',
        'GrantReadACP', 'GrantWriteACP', 'Metadata', 'RequestPayer',
        'ServerSideEncryption', 'StorageClass', 'SSECustomerAlgorithm',
        'SSECustomerKey', 'SSECustomerKeyMD5', 'SSEKMSKeyId', 'Tagging',
        'WebsiteRedirectLocation'
        """
        cls._default_extra_upload_args = {
            key: value for key, value in kwargs.items() if key in ALLOWED_UPLOAD_ARGS
        }
        return cls

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False,
    ):
        """This method does nothing as you do not need to create a 'folder' for an object store."""
        ...

    def iter_dir_(self) -> Iterable[StreamInfo]:
        """Yields tuple of uri and the metadata in a directory."""
        continuation_token: str = ""
        if self.is_dir():
            prefix = self.key
        else:
            prefix = os.path.dirname(self.key)

        while True:
            list_kwargs = {"MaxKeys": 1000, "Prefix": prefix, "Bucket": self.bucket}
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token
            response = self._client.list_objects_v2(**list_kwargs)

            continuation_token = response.get("NextContinuationToken")
            for content in response.get("Contents", []):
                key = content.get("Key")
                if key:
                    etag = content.get("ETag", "").strip('"')
                    last_modified = content.get("LastModified")
                    uri = f"{self._scheme}://{self.bucket}/{key}"
                    yield StreamInfo(
                        uri=uri,
                        name=guess_filename(uri),
                        last_modified=last_modified,
                        etag=etag,
                        extras=content,
                    )
            if not response.get("IsTruncated"):  # At the end of the list?
                break
