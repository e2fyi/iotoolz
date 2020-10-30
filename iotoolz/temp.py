"""This module implements the TempStream using the original tempfile in AbcStream."""
import io
import os.path
import weakref
from typing import IO, Iterable, Iterator, Tuple, Union

from iotoolz._abc import AbcStream, StreamInfo
from iotoolz.utils import guess_content_type_from_buffer, guess_encoding

_TEMPSTREAMS: weakref.WeakValueDictionary = weakref.WeakValueDictionary()


class TempStream(AbcStream):
    """TempStream is the stream interface to an in-memory buffer with can rollover to local file system if the "inmem_size" arg is set."""

    supported_schemas = {"tmp", "temp"}

    def __init__(  # pylint: disable=too-many-arguments
        self,
        uri: str,
        data: Union[str, bytes, bytearray] = None,
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
        Creates a new instance of TempStream.

        Args:
            uri (str): uri string to the resource.
            data Union[str, bytes, bytearray]): initial data to the stream.
            mode (str, optional): same as "open" - supports depends on the actual implementation. Defaults to "r".
            buffering (int, optional): same as "open". Defaults to -1.
            encoding (str, optional): encoding used to decode bytes to str. Defaults to None.
            newline (str, optional): same as "open". Defaults to None.
            content_type (str, optional): mime type for the resource. Defaults to "".
            inmem_size (int, optional): max size before buffer rollover from mem to disk. Defaults to None (i.e. never - may raise MemoryError).
            delimiter (Union[str, bytes], optional): delimiter used for determining line boundaries. Defaults to None.
            chunk_size (int, optional): chunk size when iterating bytes stream. Defaults to io.DEFAULT_BUFFER_SIZE.
            etag (str, optional): etag for the stream content. Defaults to "".

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
            etag,
            **kwargs,
        )
        self.set_info(content_type=content_type, encoding=encoding or "utf-8")
        self._initial_data = None
        self._has_read = True  # nothing to read
        self._has_stats = True

        if data:
            if "r" not in self.mode:
                self.mode = "r" + self.mode
                self.mode = "".join(
                    [char for char in self.mode if char not in {"w", "a"}]
                )
            if isinstance(data, (bytes, bytearray)):
                content_type = content_type or guess_content_type_from_buffer(data)
                encoding = encoding or guess_encoding([data])[0]
                if "b" not in self.mode:
                    self.mode += "b"
                self.set_info(content_type=content_type, encoding=encoding)
                self._file.write(data)
            elif isinstance(data, str):
                self.mode = "".join([char for char in self.mode if char != "b"])
                encoding = encoding or "utf-8"
                content_type = content_type or guess_content_type_from_buffer(
                    data.encode(encoding or "utf-8")
                )
                self.set_info(content_type=content_type, encoding=encoding)
                self._file.write(data.encode(encoding or "utf-8"))
            else:
                raise TypeError("data must be of type bytes or str")
            self._file.seek(0)

        # register to weakdict
        _TEMPSTREAMS[self.uri] = self

    def read_to_iterable_(
        self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
    ) -> Tuple[Iterable[bytes], StreamInfo]:
        return [], StreamInfo()

    def write_from_fileobj_(
        self, uri: str, fileobj: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        return StreamInfo()

    def stats_(self) -> StreamInfo:
        return StreamInfo()

    def unlink(self, missing_ok: bool = True, **kwargs):
        self._clear_buffer()

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False,
    ):
        """This method does nothing as you do not need to create a folder for an in-memory buffer."""
        ...

    def iter_dir_(self) -> Iterable[StreamInfo]:
        dirpath = self.uri if self.uri.endswith("/") else os.path.dirname(self.uri)

        return (
            stream.info
            for uri, stream in _TEMPSTREAMS.items()
            if uri.startswith(dirpath)
            if _TEMPSTREAMS.get(uri)
        )

    def iter_dir(self) -> Iterator["TempStream"]:
        """
        If the current stream is a directory, this method will yield all Stream
        in the directory. Otherwise, it should yield all Stream in the same
        directory (or level) as the current stream.
        """
        dirpath = self.uri if self.uri.endswith("/") else os.path.dirname(self.uri)

        return (
            stream
            for uri, stream in _TEMPSTREAMS.items()
            if uri.startswith(dirpath)
            if _TEMPSTREAMS.get(uri)
        )

    def exists(self) -> bool:
        """TempStream will always exist."""
        return True

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
    ) -> "TempStream":
        """
        Creates a new instance of TempStream.

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
            TempStream: new instance of TempStream
        """
        if uri in _TEMPSTREAMS:
            return _TEMPSTREAMS[uri]  # type: ignore
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
