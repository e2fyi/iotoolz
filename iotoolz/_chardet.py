import warnings
from typing import Iterable, Tuple

from charset_normalizer import from_bytes

try:
    from chardet import UniversalDetector

    CHARDET_INSTALLED = True
except ImportError:
    CHARDET_INSTALLED = False
    warnings.warn(
        "'chardet' not installed. defaulting to 'charset_normalizer'.",
        ImportWarning,
    )


def _charset_guess_encoding(
    data: Iterable[bytes], default_encoding: str = "utf-8"
) -> Tuple[str, float]:
    for line in data:
        result = from_bytes(line).best()
        if result:
            return (result.encoding, result.coherence)
    return (default_encoding, 0.0)


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
    if not CHARDET_INSTALLED:
        return _charset_guess_encoding(data=data, default_encoding=default_encoding)

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
