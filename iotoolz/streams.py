"""
`iotoolz.streams` provide a `Streams` helper class for creating stream objects.
"""
import functools
import io
import pathlib
import urllib.parse
from typing import Any, Dict, Iterable, List, Type, Union

from iotoolz._abc import AbcStream
from iotoolz.file import FileStream
from iotoolz.http import HttpStream
from iotoolz.temp import TempStream

DEFAULT_STREAMS: List[Type[AbcStream]] = [FileStream, TempStream, HttpStream]


class Streams:
    def __init__(self, *stream_types: Type[AbcStream]):
        self._schema2stream: Dict[str, Type[AbcStream]] = {
            "": FileStream,
            "file": FileStream,
        }
        self._schema2kwargs: Dict[str, dict] = {}
        for stream_type in stream_types:
            self.register(stream_type)

    def set_schema_kwargs(self, schema: str, **kwargs) -> "Streams":
        self._schema2kwargs[schema] = kwargs
        return self

    def register(
        self,
        stream_type: Type[AbcStream],
        schemas: Union[str, Iterable[str]] = None,
        **kwargs
    ) -> "Streams":
        schemas = schemas or stream_type.supported_schemas
        if not schemas:
            raise ValueError("schema is not provided for the AbcStream type")
        schemas = [schemas] if isinstance(schemas, str) else schemas
        for schema in schemas:
            self._schema2stream[schema] = stream_type
            self._schema2kwargs[schema] = kwargs or self._schema2kwargs.get(schema, {})
        return self

    def as_stream(  # pylint: disable=too-many-arguments
        self,
        uri: Union[str, pathlib.Path],
        schema: str = None,
        data: Union[bytes, str] = None,
        fileobj: Any = None,
        mode: str = "r",
        buffering: int = -1,
        encoding: str = None,
        newline: str = None,
        content_type: str = "",
        inmem_size: int = None,
        delimiter: Union[str, bytes] = None,
        chunk_size: int = io.DEFAULT_BUFFER_SIZE,
        schema_kwargs: dict = None,
    ) -> AbcStream:
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
            **kwargs
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
as_stream = stream_factory.as_stream
register_stream = stream_factory.register
set_schema_kwargs = stream_factory.set_schema_kwargs
