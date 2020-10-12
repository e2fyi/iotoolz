import functools
import io
import os
import os.path
import pathlib
import shutil
from typing import IO, ContextManager, Iterable, Tuple, Union

from iotoolz._abc import AbcStream, StreamInfo
from iotoolz.utils import contextualize_iter, guess_content_type_from_file


class FileStream(AbcStream):
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

    def _read_to_iterable(
        self, uri: str, chunk_size: int, **kwargs
    ) -> Tuple[ContextManager[Iterable[bytes]], StreamInfo]:
        self._content_type = self.content_type or guess_content_type_from_file(self.uri)
        stream = open(
            self.uri, mode="rb", buffering=self.buffering, newline=self.newline,
        )
        read = functools.partial(stream.read, chunk_size)
        return (
            contextualize_iter(iter(read, b""), post_hook=stream.close),
            StreamInfo(content_type=self.content_type, encoding=self.encoding),
        )

    def _write_from_fileobj(
        self, uri: str, file_: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        os.makedirs(os.path.dirname(uri), exist_ok=True)
        with open(
            self.uri,
            mode=self.mode,
            buffering=self.buffering,
            encoding=self.encoding if "b" not in self.mode else None,
            newline=self.newline,
        ) as stream:
            shutil.copyfileobj(file_, stream)
        return StreamInfo()
