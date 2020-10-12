import contextlib
import functools
from typing import Any, Callable, Iterable, Iterator, Tuple, TypeVar

import cytoolz
import magic
from chardet.universaldetector import UniversalDetector

T = TypeVar("T")


@contextlib.contextmanager
def contextualize_iter(
    iter_: Iterator[T], post_hook: Callable[[], Any]
) -> Iterator[Iterator[T]]:
    try:
        yield iter_
    finally:
        post_hook()


def guess_encoding(
    data: Iterable[bytes], default_encoding: str = "utf-8"
) -> Tuple[str, float]:

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
