import pathlib
from typing import Callable, Iterable, Union

import pandas as pd

from iotoolz import AbcStream
from iotoolz.streams import Stream


def open_csv(
    uri: Union[pathlib.Path, str],
    stream_type: Callable[..., AbcStream] = Stream,
    **kwargs,
) -> Iterable[dict]:
    """
    Open a csv resource using 'pandas.read_csv' method.

    Args:
        uri (Union[pathlib.Path, str]): uri of the csv resource.
        **kwargs: additional params passed to `pandas.read_csv`.
    Returns:
        Iterable[dict]: an iterable of dict.
    """
    with stream_type(uri) as stream:
        return pd.read_csv(stream, **kwargs).to_dict(orient="records")  # type: ignore
