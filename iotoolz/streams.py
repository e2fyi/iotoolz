"""
`iotoolz.streams` provide a `Streams` helper class for creating stream objects.
"""
import functools
import io
import pathlib
import urllib.parse
from typing import Any, Dict, Iterable, Iterator, List, Type, Union

from iotoolz._abc import AbcStream, StreamInfo
from iotoolz.extensions import MinioStream, S3Stream
from iotoolz.file import FileStream
from iotoolz.http import HttpStream
from iotoolz.temp import TempStream

AbcStreamType = Type[AbcStream]

DEFAULT_STREAMS: List[AbcStreamType] = [
    FileStream,
    TempStream,
    HttpStream,
    S3Stream,
    MinioStream,
]


class Streams:
    """
    Helper class to open streams with the corresponding AbcStream implementation
    based on the uri schema.
    """

    INMEM_SIZE: int = 0

    def __init__(self, *stream_types: AbcStreamType):
        """
        Creates a new instance of Streams.
        """
        self._schema2stream: Dict[str, AbcStreamType] = {
            "": FileStream,
            "file": FileStream,
        }
        self._schema2kwargs: Dict[str, dict] = {}
        for stream_type in stream_types:
            self.register(stream_type)

        # alias
        self.open_stream = self.open

    def set_schema_kwargs(self, schema: str, **kwargs) -> "Streams":
        """
        Set and update the default parameters for the different AbcStream implementations
        based on their schema.

        ```py
        # do not verify ssl cert for all https requests
        # by passing verify=False flag to requests inside HttpStream
        # i.e. HttpStream is implemented with requests
        Streams().set_schema_kwargs("https", verify=False).open("https://foo.bar")
        ```

        Returns:
            Streams: current Streams object.
        """
        self._schema2kwargs[schema] = kwargs
        return self

    def register(
        self,
        stream_type: AbcStreamType,
        schemas: Union[str, Iterable[str]] = None,
        **kwargs,
    ) -> "Streams":
        """
        Register an concrete AbcStream (e.g. HttpStream, FileStream, TempStream) and the
        corresponding schemas to the Streams object.

        Steams will use the appropriate registered concrete AbcStream based on the uri
        schema.

        By default, FileStream will already be registered as it is the fallback for
        any unknown uri.

        Raises:
            ValueError: schema is not provided and {stream_type} has an empty supported_schemas.

        Returns:
            Streams: current Streams object.
        """
        schemas = schemas or stream_type.supported_schemas
        if schemas is None:
            raise ValueError(
                f"schema is not provided and {stream_type} has an empty supported_schemas."
            )
        schemas = [schemas] if isinstance(schemas, str) else schemas
        for schema in schemas:
            self._schema2stream[schema] = stream_type
            self._schema2kwargs[schema] = kwargs or self._schema2kwargs.get(schema, {})
        return self

    def open(  # pylint: disable=too-many-arguments
        self,
        uri: Union[str, pathlib.Path],
        mode: str = "r",
        schema: str = None,
        data: Union[bytes, str] = None,
        fileobj: Any = None,
        buffering: int = -1,
        encoding: str = None,
        newline: str = None,
        content_type: str = "",
        inmem_size: int = None,
        delimiter: Union[str, bytes] = None,
        chunk_size: int = io.DEFAULT_BUFFER_SIZE,
        etag: str = "",
        schema_kwargs: dict = None,
        **extra_kwargs,
    ) -> AbcStream:
        """
        Open an appropriate registered stream based on the uri schema.

        For example, if HttpStream is registered for http/https schema, any uri starting
        with http or https will be opended using HttpStream.

        You can perform most IO methods on the stream (e.g. read, write, seek, ...)
        depending on the provided mode (e.g. r, rb, w, wb, ...).

        If you open the stream with a context, it will close automatically after exiting
        the context.

        The only difference with a native "open" is that changes to the stream buffer
        will not be pushed to the actual remote resource until "save" or "close" is
        called.

        Appropriate concrete AbcStreams must be registered with Streams to support the
        corresponding uris.

        ```py
        streams = Streams().register(HttpStream, {"http", "https"})

        # Get some ndjson data from a https endpoint
        with streams.open("https://foo.bar/data.ndjson", "r") as stream:
            ndjsons = [json.loads(line) for line in stream]

        # Post some binary content to a http endpoint
        with streams.open("https://foo.bar/api/data", "wb") as stream:
            stream.write(b"hello world")

        ```

        Args:
            uri (Union[str, pathlib.Path]): uri to the resource.
            mode (str, optional): same as "open". Defaults to "r".
            schema (str, optional): if provided, supersed the schema inferred from the uri. Defaults to None.
            data (Union[bytes, str], optional): if provided, create a stream with the initial data. Defaults to None.
            fileobj (Any, optional): if provided, create a stream with the data copied from the fileobj. Defaults to None.
            buffering (int, optional): same as "open". Defaults to -1.
            encoding (str, optional): encoding used to decode binary data to string. Defaults to None.
            newline (str, optional): same as "open". Defaults to None.
            content_type (str, optional): resource mime type (e.g. application/json). Defaults to "".
            inmem_size (int, optional): max data size before buffer is rollover into disk. Defaults to None (i.e. never - may result in MemoryError).
            delimiter (Union[str, bytes], optional): delimiter used for determining line boundaries. Defaults to None.
            chunk_size (int, optional): size of chunks to return in binary mode. Defaults to io.DEFAULT_BUFFER_SIZE.
            etag (str, optional): etag for the stream content. Defaults to "".
            schema_kwargs (dict, optional): dict of schema to default parameters for the corresponding AbcStreams. Defaults to None.

        Returns:
            AbcStream: concrete AbcStream based on the uri schema.
        """
        if isinstance(uri, pathlib.Path):
            schema = "file"

        if not schema:
            schema, *_ = urllib.parse.urlsplit(str(uri))

        # fallback to file system
        if schema not in self._schema2stream:
            schema = "file"

        schema_kwargs = schema_kwargs or {}
        kwargs = {
            **self._schema2kwargs.get(schema, {}),
            **schema_kwargs.get(schema, {}),
            **extra_kwargs,
        }
        stream = self._schema2stream[schema].open(
            str(uri),
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            newline=newline,
            content_type=content_type,
            inmem_size=inmem_size or self.INMEM_SIZE,
            delimiter=delimiter,
            chunk_size=chunk_size,
            etag=etag,
            **kwargs,
        )

        if data:
            # temp make it writable
            stream.mode = "wb"
            stream.write(data)
            # reset to start pos
            stream.seek(0)
            #  revert to original
            stream.mode = mode

        elif fileobj and hasattr(fileobj, "read"):
            read = functools.partial(fileobj.read, chunk_size)
            if "b" in mode:
                iter_data = iter(read, b"")
            else:
                iter_data = iter(read, "")
            # temp make it writable
            stream.mode = "wb"
            for chunk in iter_data:
                stream.write(chunk)
                # in case the mode is wrong and iter doesn't end
                if not chunk:
                    break
            # reset to start pos
            stream.seek(0)
            #  revert to original
            stream.mode = mode

        return stream

    def mkdir(
        self,
        uri: Union[pathlib.Path, str],
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ):
        """
        Create a new directory at this given path. If mode is given, it is combined with
        the process’ umask value to determine the file mode and access flags. If the path
        already exists, FileExistsError is raised.

        If parents is true, any missing parents of this path are created as needed; they
        are created with the default permissions without taking mode into account
        (mimicking the POSIX mkdir -p command).

        If parents is false (the default), a missing parent raises FileNotFoundError.

        If exist_ok is false (the default), FileExistsError is raised if the target
        directory already exists.

        If exist_ok is true, FileExistsError exceptions will be ignored (same behavior
        as the POSIX mkdir -p command), but only if the last path component is not an
        existing non-directory file.

        Args:
            uri (Union[pathlib.Path, str]): uri to create dir.
            mode (int, optional): mask mode. Defaults to 0o777.
            parents (bool, optional): If true, creates any parents if required. Defaults to False.
            exist_ok (bool, optional): If true, will not raise exception if dir already exists. Defaults to False.
        """
        return self.open(uri).mkdir(mode, parents, exist_ok)

    def iter_dir(self, uri: Union[pathlib.Path, str]) -> Iterator[AbcStream]:
        """
        If a directory, yields stream in the directory. Otherwise, yield all streams in
        the same directory as the provided uri.

        Args:
            uri (Union[pathlib.Path, str]): uri to list.

        Returns:
            Iterator["AbcStream"]: iterator of streams that matches the pattern.

        Yields:
            AbcStream: stream object
        """
        return self.open(uri).iter_dir()

    def glob(self, uri: Union[pathlib.Path, str]) -> Iterator[AbcStream]:
        """
        Yield streams that matches the provided scheme and pattern.

        Args:
            uri (Union[pathlib.Path, str]): base uri to glob.
            pattern (str, optional): unix shell style pattern. Defaults to "*".

        Returns:
            Iterator["AbcStream"]: iterator of streams that matches the pattern.

        Yields:
            AbcStream: stream object
        """
        uri_ = str(uri)
        first_star = uri_.index("*")
        return self.open(uri_[0:first_star]).glob(uri_[first_star:])

    def exists(self, uri: Union[pathlib.Path, str, AbcStream]) -> bool:
        """Whether a stream points to an existing resource."""
        if isinstance(uri, AbcStream):
            return uri.exists()
        return self.open(uri).exists()

    def stats(self, uri: Union[pathlib.Path, str, AbcStream]) -> StreamInfo:
        """Get the StreamInfo."""
        if isinstance(uri, AbcStream):
            return uri.stats()
        return self.open(uri).stats()

    def unlink(
        self,
        uri: Union[pathlib.Path, str, AbcStream],
        missing_ok: bool = True,
        **kwargs,
    ):
        """Delete and remove a stream resource."""
        if isinstance(uri, AbcStream):
            uri.unlink(missing_ok=True, **kwargs)  # type: ignore
        else:
            self.open(uri).unlink(missing_ok=missing_ok, **kwargs)

    def rmdir(
        self,
        uri: Union[pathlib.Path, str, AbcStream],
        ignore_errors: bool = False,
        **kwargs,
    ):
        """Remove the entire directory."""
        if isinstance(uri, AbcStream):
            uri.rmdir(ignore_errors=ignore_errors, **kwargs)
        else:
            self.open(uri).rmdir(ignore_errors=ignore_errors, **kwargs)

    @classmethod
    def set_buffer_rollover_size(cls, value: int):
        """
        Set the max size of the buffer before the data is rollover to disk.

        Args:
            value (int): size before rollover
        """
        cls.INMEM_SIZE = value


stream_factory = Streams(*DEFAULT_STREAMS)
open_stream = stream_factory.open
Stream = stream_factory.open
register_stream = stream_factory.register
set_schema_kwargs = stream_factory.set_schema_kwargs
set_buffer_rollover_size = stream_factory.set_buffer_rollover_size
mkdir = stream_factory.mkdir
iter_dir = stream_factory.iter_dir
glob = stream_factory.glob
exists = stream_factory.exists
stats = stream_factory.stats
unlink = delete = remove = stream_factory.unlink
rmdir = stream_factory.rmdir
