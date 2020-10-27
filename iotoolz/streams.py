"""
`iotoolz.streams` provide a `Streams` helper class for creating stream objects.
"""
import functools
import io
import pathlib
import urllib.parse
from typing import Any, Dict, Iterable, List, Type, Union

from iotoolz._abc import AbcStream
from iotoolz.extensions import S3Stream
from iotoolz.file import FileStream
from iotoolz.http import HttpStream
from iotoolz.temp import TempStream

AbcStreamType = Type[AbcStream]

DEFAULT_STREAMS: List[AbcStreamType] = [FileStream, TempStream, HttpStream, S3Stream]


class Streams:
    """
    Helper class to open streams with the corresponding AbcStream implementation
    based on the uri schema.
    """

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
        schema_kwargs: dict = None,
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
            schema_kwargs (dict, optional): dict of schema to default parameters for the corresponding AbcStreams. Defaults to None.

        Returns:
            AbcStream: concrete AbcStream based on the uri schema.
        """
        if not schema:
            if isinstance(uri, pathlib.Path):
                schema = "file"
            else:
                schema, *_ = urllib.parse.urlsplit(uri)

        # fallback to file system
        if schema not in self._schema2stream:
            schema = "file"

        schema_kwargs = schema_kwargs or {}
        kwargs = {
            **self._schema2kwargs.get(schema, {}),
            **schema_kwargs.get(schema, {}),
        }
        stream = self._schema2stream[schema](
            str(uri),
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            newline=newline,
            content_type=content_type,
            inmem_size=inmem_size,
            delimiter=delimiter,
            chunk_size=chunk_size,
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


stream_factory = Streams(*DEFAULT_STREAMS)
open_stream = stream_factory.open
register_stream = stream_factory.register
set_schema_kwargs = stream_factory.set_schema_kwargs
