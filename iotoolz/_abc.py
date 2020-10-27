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
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from iotoolz.utils import guess_content_type_from_buffer, guess_encoding, peek_stream

T = TypeVar("T")


def need_sync(method: Callable[..., Any]) -> Callable[..., Any]:
    """
    need_sync is a decorator function to apply on any AbcStream method.

    Any AbcStream methods that are decorated will always attempt to read from
    the resource source into the tempfile, before executing the method.

    Args:
        method ([Callable[..., Any]]): Any AbcStream method
    """

    @functools.wraps(method)
    def _implement_sync_fileobj(self: "AbcStream", *args, **kwargs) -> Any:
        self.sync()
        return method(self, *args, **kwargs)

    return _implement_sync_fileobj


@dataclasses.dataclass
class StreamInfo:
    """
    dataclass to wrap around some basic info about the stream.
    """

    content_type: Optional[str] = ""
    encoding: Optional[str] = ""
    etag: Optional[str] = ""
    extras: dict = dataclasses.field(default_factory=dict)


class AbcStream(
    abc.ABC
):  # pylint: disable=too-many-instance-attributes,too-many-public-methods
    """
    AbcStream is an abstract class which mimics python's native `open` function very
    closely.
    """

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
        """
        Creates a new instance of AbcStream.

        Args:
            uri (str): [description]
            mode (str, optional): [description]. Defaults to "r".
            buffering (int, optional): [description]. Defaults to -1.
            encoding (str, optional): [description]. Defaults to None.
            newline (str, optional): [description]. Defaults to None.
            content_type (str, optional): [description]. Defaults to "".
            inmem_size (int, optional): [description]. Defaults to None.
            delimiter (Union[str, bytes], optional): [description]. Defaults to None.
            chunk_size (int, optional): [description]. Defaults to io.DEFAULT_BUFFER_SIZE.
        """
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
    def read_to_iterable_(
        self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
    ) -> Tuple[Iterable[bytes], StreamInfo]:
        """
        read_to_iterable_ is an abstract method to implement the reading of the source
        resource into the a binary iterator - i.e. you will need to encode your data
        appropriately if it is a string.

        Alternatively, 'fileobj' argument is also provided where you can write to the
        stream file buffer directly. In this case, you should return an empty iterable.
        However, this will be less efficient as the actual read will only start after
        all the data have been read  into the buffer.

        The method should return a tuple of the bytes iterator and StreamInfo object
        which ideally should provide the following info:

        - content_type: (see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type)

        - encoding: (see https://docs.python.org/2.4/lib/standard-encodings.html)

        - etag: (see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag)

        An optional dict "extras" is also provided for any other information.

        Args:
            uri (str): source uri to the resource
            chunk_size (int): size for each chunk
            fileobj: (IO[bytes]): temp fileobj for the stream

        Raises:
            NotImplementedError: [description]

        Returns:
            Tuple[Iterable[bytes], StreamInfo]: tuple of the iterable and StreamInfo
                object describing the data
        """
        raise NotImplementedError

    @abc.abstractmethod
    def write_from_fileobj_(
        self, uri: str, fileobj: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        """
        Abstract method to implement the write to the
        destination resource from the provided file-like object.

        This file-like object only provides binary outputs - i.e. if the resources only
        accepts text input, you will need to decode the output inside this method.

        Args:
            uri (str): destination uri of the resource
            fileobj (IO[bytes]): file-like object to read from
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
        """
        Creates a new instance of AbcStream. This mimics python's `open` method but
        supports additional keyword arguments.

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

        Returns:
            AbcStream: new instance of AbcStream
        """
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
        """True if the stream is closed."""
        return self._file.closed

    @property
    def encoding(self) -> str:
        """
        Text encoding to use to decode internal binary data into text outputs.

        Returns:
            str: text encoding.
        """
        if self._info:
            return (
                self._info.encoding
                or self._encoding
                or guess_encoding(self.iter_bytes())[0]
            )
        return self._encoding or guess_encoding(self.iter_bytes())[0]

    @property
    def content_type(self) -> str:
        """
        Resource media type (e.g. application/json, text/plain).

        See also https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type

        Returns:
            str: string describing the media type of the resource.
        """
        if self._info:
            return self._info.content_type or self._content_type
        if not self._content_type and self.size > 0:
            with peek_stream(self._file, peek=0) as stream:
                return guess_content_type_from_buffer(stream.read(1024))  # type: ignore
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
        """
        Stream info like content type, encoding, and etag.

        Returns:
            StreamInfo: StreamInfo object for the current stream.
        """
        return self._info or StreamInfo(
            encoding=self.encoding, content_type=self.content_type
        )

    @need_sync
    def peek(self, size: Optional[int] = None) -> bytes:
        """
        Return bytes from the stream without advancing the position.

        Args:
            size (Optional[int], optional): number of bytes to return. Return default
            chunk size if not provided. Return everything if is -1. Defaults to None.

        Returns:
            bytes: data starting from current stream pos.
        """
        with peek_stream(self) as stream:
            return stream.read(size or self._chunk_size)  # type: ignore

    @need_sync
    def read(self, size: Optional[int] = -1) -> Union[str, bytes, bytearray]:
        """
        Read and return up to size bytes. If the argument is omitted, None, or negative,
        data is read and returned until EOF is reached. An empty bytes object is
        returned if the stream is already at EOF.

        Args:
            size (Optional[int], optional): number of bytes to read. Defaults to -1.

        Returns:
            Union[str, bytes, bytearray]: data starting from current stream pos.
        """
        chunk = self._file.read(size or -1)
        if "b" in self.mode:
            return chunk
        return chunk.decode(self.encoding)

    @need_sync
    def readline(self, size: Optional[int] = -1) -> Union[str, bytes, bytearray]:
        """
        Read and return one line from the stream. If size is specified, at most size
        bytes will be read.

        Args:
            size (Optional[int], optional): [description]. Defaults to -1.

        Returns:
            Union[str, bytes, bytearray]: [description]
        """
        chunk = self._file.readline(size or -1)
        if "b" in self.mode:
            return chunk
        return chunk.decode(self.encoding)

    @need_sync
    def readlines(
        self, hint: Optional[int] = -1
    ) -> Union[List[str], List[bytes], List[bytearray]]:
        """
        Read and return a list of lines from the stream. hint can be specified to
        control the number of lines read: no more lines will be read if the total size
        (in bytes/characters) of all lines so far exceeds hint.

        Args:
            hint (Optional[int], optional): [description]. Defaults to -1.

        Returns:
            Union[List[str], List[bytes], List[bytearray]]: [description]
        """
        chunks = self._file.readlines(hint or -1)
        if "b" in self.mode:
            return chunks
        return [chunk.decode(self.encoding) for chunk in chunks]

    def write(self, data: Union[str, bytes, bytearray]) -> int:
        """
        Write the given bytes-like object into buffer and return the number of bytes
        written.

        The data will not be written to the actual resource until "save" or "close"
        method is called.

        Args:
            data (Union[str, bytes, bytearray]): bytes-like object to write.

        Raises:
            IOError: stream is readonly.
            TypeError: expect data to be of type str, bytes or bytearray.

        Returns:
            int: number of bytes written.
        """
        if "r" in self.mode and "w" not in self.mode and "a" not in self.mode:
            raise IOError(f"{self} is readonly: {self.mode}")
        if isinstance(data, str):
            return self._write(data.encode(self.encoding))
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("expect data to be of type 'bytes' or 'bytearray'")
        return self._write(data)

    def seek(self, offset, whence: int = 0) -> int:
        """
        Change the stream position to the given byte offset. offset is interpreted
        relative to the position indicated by whence. The default value for whence is
        SEEK_SET. Values for whence are:

        SEEK_SET or 0 – start of the stream (the default); offset should be zero or positive

        SEEK_CUR or 1 – current stream position; offset may be negative

        SEEK_END or 2 – end of the stream; offset is usually negative

        Return the new absolute position.

        Args:
            offset ([type]): offset relative to position indicated by "whence".
            whence (int, optional): reference position - 0, 1, or 2. Defaults to 0.

        Returns:
            [int]: new absolute position.
        """
        return self._file.seek(offset, whence)

    def tell(self) -> int:
        """
        Return the current stream position.

        Returns:
            int: current stream position.
        """
        return self._file.tell()

    def flush(self):
        """
        Flush the write buffers of the stream if applicable.
        This does nothing for read-only and non-blocking streams.
        """
        self._file.flush()

    def save(self) -> "AbcStream":
        """
        Flush and stream everything in the buffer to the actual resource location.

        Does nothing if mode is read-only. Will not close the stream.

        Returns:
            [AbcStream]: current stream object.
        """
        if self.closed or self.is_empty():
            return self

        if "w" in self.mode or "a" in self.mode:
            # remember current pos
            pos = self.tell()
            self.flush()
            # go to start of stream
            self.seek(0)
            info = self.write_from_fileobj_(
                self.uri, self._file, self.size, **self._kwargs
            )
            if not self._file.closed:
                self._update_info(info)
                # restore org pos
                self.seek(pos)

        return self

    def close(self):
        """
        Flush and close this stream. This method has no effect if the file is already
        closed. Once the file is closed, any operation on the file (e.g. reading or
        writing) will raise a ValueError.

        As a convenience, it is allowed to call this method more than once; only the
        first call, however, will have an effect.
        """
        try:
            self.save()
            for _, sink in self._pipes:
                sink.close()
        finally:
            self._cleanup()

    def is_empty(self) -> bool:
        """
        True if current buffer is empty.

        Returns:
            bool: whether current buffer is empty.
        """
        # remember pos
        pos = self.tell()
        # go to the end of stream
        self.seek(0, 2)
        is_empty = self.tell() == 0
        # restore org pos
        self.seek(pos)
        return is_empty

    def set_encoding(self, encoding: str) -> "AbcStream":
        """
        Set and update the encoding to use to decode the binary data into text.

        Args:
            encoding ([str]): text encoding for the stream.

        Returns:
            AbcStream: current stream object.
        """
        self._encoding = encoding
        if self._info:
            self._info.encoding = encoding
        for _, sink in self._pipes:
            if hasattr(sink, "set_encoding"):
                sink.set_encoding(encoding)  # type: ignore
        return self

    def sync(self, force: bool = False) -> "AbcStream":
        """
        Loads the resource into buffer if not loaded.

        Args:
            force ([bool]): If true, load the resource into buffer again even if it is already loaded.

        Returns:
            AbcStream: current stream object.
        """
        if not self._info or force:
            self._file = self._tempfile(new=True)
            iter_bytes, info = self.read_to_iterable_(
                self.uri, self._chunk_size, self._file, **self._kwargs
            )
            self._update_info(info)
            iter_bytes = iter_bytes or []
            for chunk in iter_bytes:
                self._write(chunk)
            self._file.seek(0)
            if not self._content_type:
                with peek_stream(self._file, peek=0) as stream:
                    self._content_type = guess_content_type_from_buffer(
                        stream.read(1024)
                    )
        return self

    def iter_bytes(self) -> Iterator[bytes]:
        """
        Returns an iterator which yields a bytes stream. Will load the resource into
        buffer if needed.

        Yields:
            Iterator[bytes]: bytes stream from the start position
        """
        if not self._info:
            iter_bytes, info = self.read_to_iterable_(
                self.uri, self._chunk_size, self._file, **self._kwargs
            )
            self._update_info(info)
            self._file = self._tempfile(new=True)
            if iter_bytes:
                for chunk in iter_bytes:
                    self._write(chunk)
                    yield chunk
            else:
                with peek_stream(self._file, peek=0) as file_:
                    read = functools.partial(file_.read, self._chunk_size)
                    yield from iter(read, b"")
            self.seek(0)
        else:
            with peek_stream(self._file, peek=0) as file_:
                read = functools.partial(file_.read, self._chunk_size)
                yield from iter(read, b"")

    @need_sync
    def pipe(self, sink: IO, text_mode: bool = False) -> IO:
        """
        Pipes the data from the current stream object into any file-like object.

        Args:
            sink (IO): Any file-like object or AbcStream object.
            text_mode (bool, optional): If True, writes string to sink rather than bytes. Defaults to False.

        Raises:
            ValueError:  sink for pipe must be a filelike object - i.e. has write method

        Returns:
            IO: file-like object (i.e. sink) that is piped into.
        """
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
        """
        Yields chunks of bytes or lines depending on the mode.

        Yields:
            Iterator[Union[bytes, str]]: bytes stream if in binary mode or line stream for text mode.
        """
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


# AbcStream.register(io.IOBase)
