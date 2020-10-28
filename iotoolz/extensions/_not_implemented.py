"""This module implements the TempStream using the original tempfile in AbcStream."""
from typing import IO, Iterable, Set, Tuple, Type

from iotoolz._abc import AbcStream, StreamInfo


def mock_stream(supported_schemas: Set[str], msg: str) -> Type[AbcStream]:
    class NotImplementedStream(AbcStream):
        """NotImplementedStream is a mock stream interface for extension streams that does not have the required dependencies installed."""

        def __init__(self, *args, **kwargs):  # pylint: disable=super-init-not-called
            raise NotImplementedError(msg)

        def read_to_iterable_(
            self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
        ) -> Tuple[Iterable[bytes], StreamInfo]:
            raise NotImplementedError(msg)

        def write_from_fileobj_(
            self, uri: str, fileobj: IO[bytes], size: int, **kwargs
        ) -> StreamInfo:
            raise NotImplementedError(msg)

    NotImplementedStream.supported_schemas = supported_schemas

    return NotImplementedStream
