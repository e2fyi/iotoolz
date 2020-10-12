"""
This module provides the abstract class for a generic IOStream.
"""
import abc
import dataclasses
import functools
import io
import os.path
import tempfile
from typing import (
    IO,
    Any,
    Callable,
    ContextManager,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from iotoolz.utils import guess_content_type_from_buffer, guess_encoding

T = TypeVar("T")


def need_sync(method: Callable[..., Any]) -> Callable[..., Any]:
    """
    need_sync is a decorator function to apply on any AbcStream method.

    Any AbcStream methods that are decorated will always attempt to read from
    the resource source into the tempfile, before executing the method.

    Args:
        method ([Callable[..., Any]]): Any AbcStream method
    """

    def _implement_sync_fileobj(self: "AbcStream", *args, **kwargs) -> Any:
        self.sync()
        return method(self, *args, **kwargs)

    return _implement_sync_fileobj


@dataclasses.dataclass
class StreamInfo:
    """dataclass to wrap around some basic info about the stream.
    """

    content_type: Optional[str] = ""
    encoding: Optional[str] = ""
    etag: Optional[str] = ""
    extras: dict = dataclasses.field(default_factory=dict)


class AbcStream(abc.ABC):  # pylint: disable=too-many-instance-attributes
    INMEM_SIZE: int = 0
    supported_schemas: Set[str] = set()

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
        **kwargs,
    ):
        self.uri = uri
        self.filename = os.path.basename(uri)
        self.mode = mode
        self.buffering = buffering
        self.newline = newline

        self._encoding = encoding or "utf-8"
        self._content_type = content_type
        self._delimiter = delimiter
        self._chunk_size = chunk_size
        self._inmem_size = inmem_size
        self._kwargs = kwargs

        self._file: IO[bytes] = tempfile.SpooledTemporaryFile(
            max_size=inmem_size or self.INMEM_SIZE,
            mode="w+b",  # always binary mode
            buffering=buffering,
            encoding=encoding,
            newline=newline,
        )
        self._info: Optional[StreamInfo] = None
        self._pipes: List[Tuple[str, IO]] = []

    @abc.abstractmethod
    def _read_to_iterable(
        self, uri: str, chunk_size: int, **kwargs
    ) -> Tuple[Union[Iterable[bytes], ContextManager[Iterable[bytes]]], StreamInfo]:
        """
        _read_to_iterable is an abstract method to implement the reading of the source
        resource into the a binary iterator - i.e. you will need to encode your data
        appropriately if it is a string.

        The method should return a tuple of the bytes iterator and StreamInfo object
        which ideally should provide the following info:
            - content_type: (see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type)
            - encoding: (see https://docs.python.org/2.4/lib/standard-encodings.html)
            - etag: (see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag)

        An optional dict "extras" is also provided for any other information.

        Args:
            uri (str): source uri to the resource
            chunk_size (int): size for each chunk

        Raises:
            NotImplementedError: [description]

        Returns:
            Tuple[Iterable[bytes], StreamInfo]: tuple of the iterable and StreamInfo
                object describing the data
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _write_from_fileobj(
        self, uri: str, file_: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        """
        _write_from_fileobj is an abstract method to implement the write to the
        destination resource from the provided file-like object.

        This file-like object only provides binary outputs - i.e. if the resources only
        accepts text input, you will need to decode the output inside this method.

        Args:
            uri (str): destination uri of the resource
            file_ (IO[bytes]): file-like object to read from
            size (int): size of the data inside the file-like object

        Raises:
            NotImplementedError: [description]

        Returns:
            StreamInfo: StreamInfo object describing the data
        """
        raise NotImplementedError

    @classmethod
    def open(
        cls,
        uri: str,
        mode: str = "r",
        buffering: int = -1,
        encoding: str = None,
        newline: str = None,
        inmem_size: int = None,
        delimiter: Union[str, bytes] = None,
        chunk_size: int = io.DEFAULT_BUFFER_SIZE,
        **kwargs,
    ) -> "AbcStream":
        return cls(
            uri,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            newline=newline,
            inmem_size=inmem_size,
            delimiter=delimiter,
            chunk_size=chunk_size,
            **kwargs,
        )

    @property
    def closed(self) -> bool:
        return self._file.closed

    @property
    def encoding(self) -> str:
        if self._info:
            return (
                self._info.encoding
                or self._encoding
                or guess_encoding(self.iter_bytes())[0]
            )
        return self._encoding or guess_encoding(self.iter_bytes())[0]

    @property
    def content_type(self) -> str:
        if self._info:
            return self._info.content_type or self._content_type
        return self._content_type

    @property
    def size(self) -> int:
        curr = self.tell()
        self.seek(0, 2)
        size = self.tell()
        self.seek(curr)
        return size

    @property
    def info(self) -> StreamInfo:
        return self._info or StreamInfo(
            encoding=self.encoding, content_type=self.content_type
        )

    @need_sync
    def read(self, size: Optional[int] = -1) -> Union[str, bytes, bytearray]:
        chunk = self._file.read(size or -1)
        if "b" in self.mode:
            return chunk
        return chunk.decode(self.encoding)

    @need_sync
    def readline(self, size: Optional[int] = -1) -> Union[str, bytes, bytearray]:
        chunk = self._file.readline(size or -1)
        if "b" in self.mode:
            return chunk
        return chunk.decode(self.encoding)

    @need_sync
    def readlines(
        self, hint: Optional[int] = -1
    ) -> Union[List[str], List[bytes], List[bytearray]]:
        chunks = self._file.readlines(hint or -1)
        if "b" in self.mode:
            return chunks
        return [chunk.decode(self.encoding) for chunk in chunks]

    def write(self, data: Union[str, bytes, bytearray]) -> int:
        if "r" in self.mode and "w" not in self.mode and "a" not in self.mode:
            raise IOError(f"mode={self.mode} is readonly")
        if isinstance(data, str):
            return self._write(data.encode(self.encoding))
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("expect data to be of type 'bytes' or 'bytearray'")
        return self._write(data)

    def seek(self, offset, whence: int = 0):
        return self._file.seek(offset, whence)

    def tell(self) -> int:
        return self._file.tell()

    def flush(self):
        self._file.flush()

    def save(self) -> "AbcStream":
        if self.is_empty():
            return self

        if "w" in self.mode or "a" in self.mode:
            self.seek(0)
            info = self._write_from_fileobj(
                self.uri, self._file, self.size, **self._kwargs
            )
            self._update_info(info)

        return self

    def close(self):
        try:
            self.save()
            for _, sink in self._pipes:
                sink.close()
        finally:
            self._cleanup()

    def is_empty(self) -> bool:
        current = self.tell()
        # go to the end of stream
        self.seek(0, 2)
        is_empty = self.tell() == 0
        self.seek(current)
        return is_empty

    def set_encoding(self, encoding: str) -> "AbcStream":
        self._encoding = encoding
        if self._info:
            self._info.encoding = encoding
        for _, sink in self._pipes:
            if hasattr(sink, "set_encoding"):
                sink.set_encoding(encoding)  # type: ignore
        return self

    def sync(self, force: bool = False) -> "AbcStream":
        if not self._info or force:
            iter_bytes, info = self._read_to_iterable(
                self.uri, self._chunk_size, **self._kwargs
            )
            self._update_info(info)
            self._file = self._tempfile(new=True)
            if hasattr(iter_bytes, "__enter__"):
                with iter_bytes as chunks:  # type: ignore
                    for chunk in chunks:
                        self._write(chunk)
            else:
                for chunk in iter_bytes:  # type: ignore
                    self._write(chunk)

            self._file.seek(0)
            self._content_type = self._content_type or guess_content_type_from_buffer(
                self._file.read(1024)
            )
            self._file.seek(0)
        return self

    def iter_bytes(self) -> Iterator[bytes]:
        if not self._info:
            iter_bytes, info = self._read_to_iterable(
                self.uri, self._chunk_size, **self._kwargs
            )
            self._update_info(info)
            self._file = self._tempfile(new=True)
            if hasattr(iter_bytes, "__enter__"):
                with iter_bytes as chunks:  # type: ignore
                    for chunk in chunks:
                        self._write(chunk)
            else:
                for chunk in iter_bytes:  # type: ignore
                    self._write(chunk)
                    yield chunk
            self.seek(0)
        else:
            # remember curr pos
            pos = self.tell()
            self.seek(0)
            read = functools.partial(self._file.read, self._chunk_size)
            yield from iter(read, b"")
            # go back to current pos
            self.seek(pos)

    @need_sync
    def pipe(self, sink: IO, text_mode: bool = False) -> IO:
        if hasattr(sink, "write") and callable(sink.write):  # type: ignore
            # if empty, don't decode
            encoding = self.encoding or "utf-8"
            # update the encoding of the AbcStream to be same as source
            if hasattr(sink, "set_encoding") and encoding:
                sink.set_encoding(encoding)  # type: ignore
            self._pipes.append((encoding if text_mode else "", sink))
            # remember current pos
            pos = self.tell()
            # go to end of stream
            self._file.seek(0, 2)
            end = self.tell()
            # have some content
            if end > 0:
                # go to start
                self._file.seek(0)
                # stream existing to sink
                if text_mode:
                    for line in iter(self._file.readline, b""):
                        sink.write(line.decode(encoding))
                else:
                    for chunk in self._file:
                        sink.write(chunk)
            # go back to original position
            self.seek(pos)
            return sink

        raise ValueError(
            "sink for pipe must be a filelike object - i.e. has write method"
        )

    def _emit(self, chunk: bytes, pipes: List[Tuple[str, IO]]):
        for encoding, sink in pipes:
            data = chunk.decode(self.encoding or encoding) if encoding else chunk
            sink.write(data)

    def __iter__(self) -> Iterator[Union[bytes, str]]:
        # each iter will start from beginning
        self.seek(0)
        # for binary mode, can lazily write to temp and yield the chunk at the same time
        if "b" in self.mode:
            return self.iter_bytes()
        # for text mode, must be buffered from the tempfile instead
        self.sync()
        return iter(self._decode(iter(self._file.readline, b"")))

    def __enter__(self) -> "AbcStream":
        if "w" in self.mode:
            self._file = self._tempfile(new=True)
            return self

        if "a" in self.mode:
            self.sync()
            self.seek(0, 2)  # go to end of stream
            return self

        self.seek(0)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _write(self, chunk: bytes) -> int:
        ret = self._file.write(chunk)
        if self._pipes:
            self._emit(chunk, self._pipes)
        return ret

    def _tempfile(self, new: bool = False) -> IO[bytes]:
        if not new and self._file:
            return self._file

        if self._file:
            self._file.close()

        return tempfile.SpooledTemporaryFile(
            max_size=self._inmem_size or self.INMEM_SIZE,
            mode="w+b",  # always binary mode
            buffering=self.buffering,
            encoding=self.encoding,
            newline=self.newline,
        )

    def _decode(
        self, iter_bytes: Iterable[bytes], encoding: str = None
    ) -> Iterable[str]:
        return (data.decode(encoding or self.encoding) for data in iter_bytes)

    def _cleanup(self):
        self._info = None
        if self._file:
            self._file.close()

    def _update_info(self, info: Optional[StreamInfo]):
        if not info:
            self._info = None
        else:
            info.content_type = info.content_type or self.content_type
            info.encoding = info.encoding or self.encoding
            self._info = info

    def __repr__(self):
        type_ = type(self)
        return (
            f"<{type_.__module__}.{type_.__qualname__} "
            f"uri='{self.uri}' mode='{self.mode}' ... >"
        )

    def __str__(self):
        if "b" in self.mode:
            return self.read().decode(self.encoding)
        return self.read()

    def __hash__(self):
        if self._info and self._info.etag:
            return hash(self._info.etag)
        return hash(self.uri.encode())


AbcStream.register(io.IOBase)
