"""This module implements the FileStream with python native "open" method."""
import datetime
import io
import logging
import os
import os.path
import urllib.parse
from typing import IO, Iterable, Tuple, Union

try:
    import minio
except ImportError as error:
    raise ImportError(
        "MinioStream cannot be used because 'minio' is not installed. "
        "You can install minio by running the command: 'pip install iotoolz[minio]'"
    ) from error

from iotoolz._abc import AbcStream, StreamInfo

MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get(
    "AWS_ACCESS_KEY_ID"
)
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY") or os.environ.get(
    "AWS_SECRET_ACCESS_KEY"
)
MINIO_REGION = (
    os.environ.get("MINIO_REGION")
    or os.environ.get("AWS_REGION")
    or os.environ.get("AWS_DEFAULT_REGION")
)
MINIO_SECURE_STR = os.environ.get("MINIO_SECURE", "").lower()
MINIO_SECURE = MINIO_SECURE_STR not in {"-1", "0", "false", "off"}


class MinioStream(AbcStream):
    """S3Stream is the stream interface to AWS S3 object store.

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer
    """

    supported_schemas = {"minio"}

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
        client: minio.Minio = None,
        access_key: str = None,
        secret_key: str = None,
        region: str = None,
        secure: bool = MINIO_SECURE,
        **kwargs,
    ):
        """
        Creates a new instance of MinioStream.

        See https://docs.min.io/docs/python-client-api-reference.html

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
            client (minio.Minio, optional): overwrite the client to use for minio. Defaults to None.
            access_key (str, optional): minio access key (env MINIO_ACCESS_KEY). Defaults to None.
            secret_key (str, optional): minio secret key (env MINIO_SECRET_KEY). Defaults to None.
            region (str, optional): minio region (env MINIO_REGION). Defaults to None.
            secure (bool, optional): whether to use secure connection. (env MINIO_SECURE) Defaults to True.
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

        self._scheme, self._endpoint, path, query, _ = urllib.parse.urlsplit(uri)
        path = path[1:] if path.startswith("/") else path
        self.bucket, *key_chunks = path.split("/")
        self.key = "/".join(key_chunks)

        if self.key.startswith("/"):
            self.key = self.key[1:]

        self._client = client or minio.Minio(
            self._endpoint,
            access_key=access_key or MINIO_ACCESS_KEY,
            secret_key=secret_key or MINIO_SECRET_KEY,
            region=region or MINIO_REGION,
            secure=secure if secure is not None else MINIO_SECURE,
        )
        self._query_str = {
            key: value[-1]
            for key, value in urllib.parse.parse_qs(query).items()
            if value
        }
        self._version_id = self._query_str.get("version_id")

    def read_to_iterable_(
        self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
    ) -> Tuple[Iterable[bytes], StreamInfo]:
        """Downloads a minio object."""
        resp = self._client.stat_object(
            self.bucket, self.key, version_id=self._version_id
        )
        etag = resp.etag
        content_type = resp.content_type or self.content_type
        last_modified = resp.last_modified

        try:
            response = self._client.get_object(self.bucket, self.key, self._version_id)
            for chunk in response.stream():
                fileobj.write(chunk)
        finally:
            response.close()
            response.release_conn()

        fileobj.seek(0)  # reset to initial counter
        return (
            [],
            StreamInfo(
                content_type=content_type,
                encoding=self.encoding,
                etag=etag,
                last_modified=last_modified,
            ),
        )

    def write_from_fileobj_(
        self, uri: str, fileobj: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        """Uploads the data in the buffer."""
        result = self._client.put_object(self.bucket, self.key, fileobj, size)
        return StreamInfo(etag=result.etag, last_modified=datetime.datetime.now())

    def stats_(self) -> StreamInfo:
        resp = self._client.stat_object(
            self.bucket, self.key, version_id=self._version_id
        )
        return StreamInfo(
            content_type=resp.content_type,
            etag=resp.etag,
            last_modified=resp.last_modified,
            extras={"stat": resp},
        )

    def exists(self) -> bool:
        """Whether the stream points to an existing resource."""
        try:
            self.set_info(self.stats_())
            return True
        except minio.error.MinioException:
            return False

    def is_dir(self) -> bool:
        """Whether stream points to a existing dir."""
        return self.uri.endswith("/")

    def is_file(self) -> bool:
        """Whether stream points to a existing file."""
        return self.exists()

    def unlink(self, missing_ok: bool = True, **kwargs):
        try:
            self._client.remove_object(
                self.bucket, self.key, version_id=self._version_id
            )
        except minio.error.MinioException:
            if not missing_ok:
                raise

    def rmdir(self, ignore_errors: bool = False, **kwargs) -> "MinioStream":
        """Remove the entire directory."""
        delete_object_list = map(
            lambda x: minio.deleteobjects.DeleteObject(x.object_name),  # type: ignore
            self._client.list_objects(self.bucket, self.key, recursive=True),
        )
        errors = self._client.remove_objects(self.bucket, delete_object_list)
        if ignore_errors:
            for err in errors:
                logging.warning(err)
        else:
            raise IOError("\n".join(errors))
        return self

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False,
    ):
        """This method does nothing as you do not need to create a 'folder' for an object store."""
        ...

    def iter_dir_(self) -> Iterable[StreamInfo]:
        """Yields tuple of uri and the metadata in a directory."""

        objects = self._client.list_objects(
            self.bucket, recursive=True, start_after=self.key,
        )

        return (
            StreamInfo(
                uri=f"{self._scheme}://{self._endpoint}/{self.bucket}/{obj.object_name}",
                name=obj.object_name,
                last_modified=obj.last_modified,
                etag=obj.etag,
                extras={"list": obj},
            )
            for obj in objects
        )
