"""
This module provides the abstract class for a generic IOStream.
"""
import abc
import dataclasses
import datetime
import fnmatch
import functools
import io
import os.path
import tempfile
import warnings
import weakref
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

from iotoolz.utils import (
    guess_content_type_from_buffer,
    guess_encoding,
    guess_filename,
    peek_stream,
)

T = TypeVar("T")


def _set_read_flag(method: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(method)
    def _wrapped_set_read_flag(self: "AbcStream", *args, **kwargs) -> Any:
        ret = method(self, *args, **kwargs)
        self._has_read = True  # pylint: disable=protected-access
        return ret

    return _wrapped_set_read_flag


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

    uri: str = ""
    name: str = ""
    content_type: Optional[str] = ""
    encoding: Optional[str] = ""
    etag: Optional[str] = ""
    last_modified: Optional[datetime.datetime] = None
    extras: dict = dataclasses.field(default_factory=dict)


def merge_streaminfo(info1: StreamInfo, info2: StreamInfo) -> StreamInfo:
    return StreamInfo(
        uri=info2.uri or info1.uri,
        name=info2.name or info1.name,
        content_type=info2.content_type or info1.content_type,
        encoding=info2.encoding or info1.encoding,
        etag=info2.etag or info1.etag,
        last_modified=info2.last_modified or info1.last_modified,
        extras={**info1.extras, **info2.extras},
    )


def transform_streaminfo(info: StreamInfo, **kwargs) -> StreamInfo:
    kwargs = {**dataclasses.asdict(info), **kwargs}
    return StreamInfo(**kwargs)


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
        etag: str = "",
        **kwargs,
    ):
        """
        Creates a new instance of AbcStream.

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
            etag (str, optional): etag for the stream content. Defaults to "".
            **kwargs: Additional keyword arguments to the stream (depends on implementation)
        """
        self.uri = uri
        self.filename = os.path.basename(uri)
        self.mode = mode
        self.buffering = buffering
        self.newline = newline

        self._delimiter = delimiter
        self._chunk_size = chunk_size
        self._inmem_size = inmem_size
        self._kwargs = kwargs

        self._has_read = False
        self._has_stats = False
        self._buffer: Optional[IO[bytes]] = None
        self._info = StreamInfo(
            uri=uri,
            name=guess_filename(uri),
            content_type=content_type,
            encoding=encoding or "utf-8",
            etag=etag,
        )
        self._pipes: List[Tuple[str, IO]] = []

        self._stream_params = {
            "mode": mode,
            "buffering": buffering,
            "encoding": encoding,
            "newline": newline,
            "content_type": content_type,
            "inmem_size": inmem_size,
            "delimiter": delimiter,
            "chunk_size": chunk_size,
            **kwargs,
        }

        # alias
        self.delete = self.unlink
        self.remove = self.unlink

        # finalizer
        self._finalizer = weakref.finalize(self, weakref.WeakMethod(self.close))  # type: ignore

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

    @abc.abstractmethod
    def stats_(self) -> StreamInfo:
        """Retrieve the StreamInfo."""
        raise NotImplementedError

    @abc.abstractmethod
    def exists(self) -> bool:
        """Whether the path points to an existing resource."""
        raise NotImplementedError

    @abc.abstractmethod
    def unlink(self, missing_ok: bool = True, **kwargs):
        """Delete and remove the resource."""
        raise NotImplementedError

    @abc.abstractmethod
    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False,
    ):
        """
        This abstract method should mimics pathlib.mkdir method for different streams.

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
            mode (int, optional): mask mode. Defaults to 0o777.
            parents (bool, optional): If true, creates any parents if required. Defaults to False.
            exist_ok (bool, optional): If true, will not raise exception if dir already exists. Defaults to False.

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError

    @abc.abstractmethod
    def iter_dir_(self) -> Iterable[StreamInfo]:
        """
        If the current stream is a directory, this method should yield StreamInfo in
        the directory. Otherwise, it should yield other StreamInfo in the same
        directory (or level) as the current stream.
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
        etag: str = "",
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
            etag (str, optional): etag for the stream content. Defaults to "".
            **kwargs: Additional keyword arguments to the stream (depends on implementation)

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
            etag=etag,
            **kwargs,
        )

    @property
    def _file(self) -> IO[bytes]:
        """Tempfile object for the stream."""
        return self._buffer or self._clear_buffer()

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
        if not self._info.encoding:
            encoding = guess_encoding(self.iter_bytes())[0]
            self.set_info(encoding=encoding)
        return self._info.encoding or ""

    @property
    def content_type(self) -> str:
        """
        Resource media type (e.g. application/json, text/plain).

        See also https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type

        Returns:
            str: string describing the media type of the resource.
        """
        if not self._info.content_type and self.size > 0:
            with peek_stream(self._file, peek=0) as stream:
                content_type = guess_content_type_from_buffer(stream.read(1024))  # type: ignore
            self.set_info(content_type=content_type)
        return self._info.content_type or ""

    @property
    def etag(self) -> str:
        """Identifier for a specific version of a resource."""
        return self._info.etag or ""

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
        if not self._has_stats and "r" in self.mode:
            return self.stats()
        return self._info

    def stats(self) -> StreamInfo:
        """Retrieve the StreamInfo of the current stream."""
        try:
            self.set_info(self.stats_())
            self._has_stats = True
        except Exception as error:  # pylint: disable=broad-except
            warnings.warn(f"{self.uri}: {error}", RuntimeWarning)
        return self._info

    def is_dir(self) -> bool:
        """Whether stream points to a existing dir."""
        return self.exists() and self.uri.endswith("/")

    def is_file(self) -> bool:
        """Whether stream points to a existing file."""
        return self.exists() and not self.uri.endswith("/")

    def rmdir(self, ignore_errors: bool = False, **kwargs) -> "AbcStream":
        """Remove the entire directory."""
        for stream in self.iter_dir():
            stream.unlink(missing_ok=ignore_errors, **kwargs)
        return self

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

    def seekable(self) -> bool:
        """
        Whether the stream is seekable.

        Returns:
            bool: whether stream is seekable.
        """
        return not self.closed

    def flush(self):
        """
        Flush the write buffers of the stream if applicable.
        This does nothing for read-only and non-blocking streams.
        """
        self._file.flush()

    def save(
        self, data: Union[bytes, bytearray, str] = None, close: bool = False
    ) -> "AbcStream":
        """
        Flush and stream everything in the buffer to the actual resource location.

        Does nothing if mode is read-only. Will not close the stream.

        Args:
            data: (Union[bytes, bytearray, str], optional): Write additional data to stream before saving. Defaults to None.
            close (bool, optional): Close stream after saving. Defaults to False.

        Returns:
            [AbcStream]: current stream object.
        """
        if self.closed:
            return self

        if self.is_empty() and not data:
            if close:
                self.close()
            return self

        if "w" in self.mode or "a" in self.mode:
            if data:
                self.write(data)
            self.flush()
            with peek_stream(self._file, peek=0, ignore_closed=True) as stream:
                info = self.write_from_fileobj_(
                    self.uri, stream, self.size, **self._kwargs
                )
            self.set_info(info)

        if close:
            self.close()

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
            self._file.close()
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

    def set_info(self, info: StreamInfo = None, **kwargs) -> "AbcStream":
        if info:
            self._info = merge_streaminfo(self._info, info)
        if kwargs:
            self._info = transform_streaminfo(self._info, **kwargs)
        return self

    def set_encoding(self, encoding: str) -> "AbcStream":
        """
        Set and update the encoding to use to decode the binary data into text.

        Args:
            encoding ([str]): text encoding for the stream.

        Returns:
            AbcStream: current stream object.
        """
        self.set_info(encoding=encoding)
        for _, sink in self._pipes:
            if hasattr(sink, "set_encoding"):
                sink.set_encoding(encoding)  # type: ignore
        return self

    @_set_read_flag
    def _read_to_buffer(self) -> "AbcStream":
        iter_bytes, info = self.read_to_iterable_(
            self.uri, self._chunk_size, self._clear_buffer(), **self._kwargs
        )
        self.set_info(info, encoding=self.encoding, content_type=self.content_type)
        iter_bytes = iter_bytes or []
        for chunk in iter_bytes:
            self._write(chunk)
        self._file.seek(0)
        return self

    def sync(self, force: bool = False) -> "AbcStream":
        """
        Loads the resource into buffer if not loaded.

        Args:
            force ([bool]): If true, load the resource into buffer again even if it is already loaded.

        Returns:
            AbcStream: current stream object.
        """
        if not self._has_read or force:
            self._read_to_buffer()
        return self

    def clone(self, uri: str = None, info: StreamInfo = None, **kwargs) -> "AbcStream":
        """
        Creates a new stream with the same init args as the current stream.

        Returns:
            AbcStream: new stream with same init args.
        """
        kwargs = {**self._stream_params, **kwargs}
        obj = type(self)(uri or self.uri, **kwargs)
        if info:
            obj.set_info(info)  # pylint: disable=protected-access
        return obj

    def iter_dir(self) -> Iterator["AbcStream"]:
        """
        If the current stream is a directory, this method will yield all Stream
        in the directory. Otherwise, it should yield all Stream in the same
        directory (or level) as the current stream.
        """
        mode = self.mode.replace("w", "")
        if "r" not in mode:
            mode = f"r{mode}"
        # do not copy the content_type and encoding
        return (self.clone(info.uri, mode=mode, info=info) for info in self.iter_dir_())

    @_set_read_flag
    def _iter_bytes_from_reader(self) -> Iterator[bytes]:
        iter_bytes, info = self.read_to_iterable_(
            self.uri, self._chunk_size, self._file, **self._kwargs
        )
        self.set_info(info)
        self._clear_buffer()
        if iter_bytes:
            for chunk in iter_bytes:
                self._write(chunk)
                yield chunk
            self.seek(0)
        else:
            yield from self._iter_bytes_from_buffer()

    def _iter_bytes_from_buffer(self) -> Iterator[bytes]:
        with peek_stream(self._file, peek=0) as file_:
            read = functools.partial(file_.read, self._chunk_size)
            yield from iter(read, b"")
        self.seek(0)

    def iter_bytes(self) -> Iterator[bytes]:
        """
        Returns an iterator which yields a bytes stream. Will load the resource into
        buffer if needed.

        Yields:
            Iterator[bytes]: bytes stream from the start position
        """
        if not self._has_read:
            return self._iter_bytes_from_reader()  # type: ignore
        return self._iter_bytes_from_buffer()

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

    def glob(self, pattern: str = "*") -> Iterator["AbcStream"]:
        """
        Glob the given relative pattern in the directory represented by this path,
        yielding all matching files (of any kind).

        Args:
            pattern (str, optional): unix shell style pattern. Defaults to "*".

        Returns:
            Iterator["AbcStream"]: iterator of streams relative to current stream

        Yields:
            AbcStream: stream object
        """
        if self.is_dir():
            pattern = os.path.join(self.uri, pattern)
        else:
            pattern = os.path.join(os.path.dirname(self.uri), pattern)
        return (
            stream for stream in self.iter_dir() if fnmatch.fnmatch(stream.uri, pattern)
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
            self._clear_buffer()
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

    def _clear_buffer(self) -> IO[bytes]:
        if self._buffer and not self._buffer.closed:
            self._buffer.flush()
            self._buffer.close()

        self._buffer = tempfile.SpooledTemporaryFile(
            max_size=self._inmem_size or self.INMEM_SIZE,
            mode="w+b",  # always binary mode
            buffering=self.buffering,
            newline=self.newline,
        )
        return self._buffer

    def _decode(
        self, iter_bytes: Iterable[bytes], encoding: str = None
    ) -> Iterable[str]:
        return (data.decode(encoding or self.encoding) for data in iter_bytes)

    def _cleanup(self):
        self._has_read = False
        self._has_stats = False
        if self._file and not self._file.closed:
            self._file.flush()
            self._file.close()

    def __repr__(self):
        type_ = type(self)
        return (
            f"<{type_.__module__}.{type_.__qualname__} "
            f"uri='{self.uri}' etag='{self.info.etag}' >"
        )

    def __str__(self):
        with peek_stream(self._file, peek=0) as stream:
            return str(stream.read().decode(self.encoding))

    def __eq__(self, obj):
        if isinstance(obj, AbcStream):
            return obj.__repr__() == self.__repr__()
        return False


AbcStream.register(io.IOBase)
