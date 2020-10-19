"""
Common io utils  based on existing libs.
"""
import contextlib
import functools
from typing import Iterable, Iterator, Optional, Tuple, TypeVar

import cytoolz
import magic
from chardet.universaldetector import UniversalDetector

T = TypeVar("T")


@contextlib.contextmanager
def peek_stream(stream: T, peek: Optional[int] = None) -> Iterator[T]:
    """
    Context manager to restore the stream position when exiting the context.

    If the arg "peek" is provided, stream will start at the provided position when
    entering the context.

    Args:
        stream (T): Any stream object with the seek and tell method.
        peek (Optional[int], optional): stream position to start at when entering the context. Defaults to None.

    Raises:
        TypeError: stream is not seekable.

    Returns:
        Iterator[T]: stream object with the modified position.
    """
    if not hasattr(stream, "seek") or not hasattr(stream, "tell"):
        raise TypeError(f"{stream} is not seekable")
    pos = stream.tell()  # type: ignore
    try:
        if isinstance(peek, int):
            stream.seek(peek)  # type: ignore
        yield stream
    finally:
        stream.seek(pos)  # type: ignore


def guess_encoding(
    data: Iterable[bytes], default_encoding: str = "utf-8"
) -> Tuple[str, float]:
    """
    Guess the encoding to decode bytes into corresponding string object.

    Uses chardet to attempt to progressively guess the encoding which can be used to
    decode the bytes into corresponding strings. Returns a tuple[encoding, confidence].

    Args:
        data (Iterable[bytes]): [description]
        default_encoding (str, optional): [description]. Defaults to "utf-8".

    Returns:
        Tuple[str, float]: [description]
    """

    detector = UniversalDetector()
    for line in data:
        detector.feed(line)
        if detector.done:
            break
    detector.close()
    return (
        detector.result.get("encoding", default_encoding),  # type: ignore
        detector.result.get("confidence", 0.0),
    )


guess_content_type_from_file = cytoolz.excepts(
    IOError, functools.partial(magic.from_file, mime=True), lambda _: ""
)


guess_content_type_from_buffer = cytoolz.excepts(
    Exception, functools.partial(magic.from_buffer, mime=True), lambda _: ""
)
