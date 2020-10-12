import io
from typing import IO, Iterable, Tuple, Union

from iotoolz._abc import AbcStream, StreamInfo
from iotoolz.utils import guess_content_type_from_buffer, guess_encoding


class TempStream(AbcStream):
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
        **kwargs,
    ):
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
        self._info = StreamInfo(content_type=content_type, encoding=encoding or "utf-8")
        self._initial_data = None

        if data:
            if "r" not in self.mode:
                self.mode = "r" + self.mode
                self.mode = "".join(
                    [char for char in self.mode if char not in {"w", "a"}]
                )
            if isinstance(data, (bytes, bytearray)):
                self._content_type = content_type or guess_content_type_from_buffer(
                    data
                )
                self._encoding = encoding or guess_encoding([data])[0]
                if "b" not in self.mode:
                    self.mode += "b"
                self._file.write(data)
            elif isinstance(data, str):
                self.mode = "".join([char for char in self.mode if char != "b"])
                self._encoding = encoding or "utf-8"
                self._content_type = content_type or guess_content_type_from_buffer(
                    data.encode(encoding or "utf-8")
                )
                self._file.write(data.encode(encoding or "utf-8"))
            else:
                raise TypeError("data must be of type bytes or str")
            self.seek(0)

    def _read_to_iterable(
        self, uri: str, chunk_size: int, **kwargs
    ) -> Tuple[Iterable[bytes], StreamInfo]:
        return [], StreamInfo()

    def _write_from_fileobj(
        self, uri: str, file_: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        return StreamInfo()
