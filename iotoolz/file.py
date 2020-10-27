"""This module implements the FileStream with python native "open" method."""
import io
import os
import os.path
import pathlib
import shutil
from typing import IO, Iterable, Tuple, Union

from iotoolz._abc import AbcStream, StreamInfo
from iotoolz.utils import guess_content_type_from_file


class FileStream(AbcStream):
    """FileStream is the stream interface to the local file system with python's "open" method."""

    supported_schemas = {"", "file"}

    def __init__(
        self,
        uri: Union[str, pathlib.Path],
        mode: str = "r",
        buffering: int = -1,
        encoding: str = None,
        newline: str = None,
        content_type: str = "",
        inmem_size: int = None,
        delimiter: Union[str, bytes] = None,
        chunk_size: int = io.DEFAULT_BUFFER_SIZE,
        **kwargs,
    ):
        super().__init__(
            str(uri),
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

    def read_to_iterable_(
        self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
    ) -> Tuple[Iterable[bytes], StreamInfo]:
        self._content_type = self.content_type or guess_content_type_from_file(self.uri)

        def iter_bytes() -> Iterable[bytes]:
            with open(
                self.uri, mode="rb", buffering=self.buffering, newline=self.newline,
            ) as stream:
                for chunk in stream:
                    yield chunk

        return (
            iter_bytes(),
            StreamInfo(content_type=self.content_type, encoding=self.encoding),
        )

    def write_from_fileobj_(
        self, uri: str, fileobj: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        os.makedirs(os.path.dirname(uri), exist_ok=True)
        with open(
            self.uri,
            mode=self.mode,
            buffering=self.buffering,
            encoding=self.encoding if "b" not in self.mode else None,
            newline=self.newline,
        ) as stream:
            shutil.copyfileobj(fileobj, stream)
        return StreamInfo()
