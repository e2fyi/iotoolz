import inspect

import rx
import rx.core.observable

from iotoolz import AbcStream
from iotoolz.streams import open_stream
from iotoolz.utils import copy_signature


@copy_signature(open_stream)
def open_rxstream(*args, **kwargs) -> rx.core.observable.observable.Observable:
    """Open a rxstream."""
    inspect.signature(open_rxstream).bind(*args, **kwargs)
    return rx.from_iterable(open_stream(*args, **kwargs))


def rxstream_from(stream: AbcStream) -> rx.core.observable.observable.Observable:
    """
    Creates a RxStream from a regular stream.

    Args:
        stream (AbcStream): a stream object.

    Returns:
        rx.core.observable.observable.Observable: a rx observable object.
    """
    return rx.from_iterable(stream)


RxStream = open_rxstream
