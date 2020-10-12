from typing import IO, Iterable, Tuple

import cytoolz
import requests
import requests_toolbelt

from iotoolz._abc import AbcStream, StreamInfo


class HttpStream(AbcStream):
    supported_schemes = {"http", "https"}

    def _read_to_iterable(
        self, uri: str, chunk_size: int, **kwargs
    ) -> Tuple[Iterable[bytes], StreamInfo]:
        resp = requests.get(uri, stream=True, **cytoolz.dissoc(kwargs, "stream"))
        resp.raise_for_status()
        info = StreamInfo(
            content_type=resp.headers.get("Content-Type"),
            encoding=resp.encoding,
            etag=resp.headers.get("etag"),
        )
        return resp.iter_content(chunk_size=chunk_size), info

    def _write_from_fileobj(
        self, uri: str, file_: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        use_post = kwargs.get("use_post")
        requests_method = requests.post if use_post else requests.put
        resp = requests_method(
            uri,
            data=requests_toolbelt.StreamingIterator(size, file_),
            **cytoolz.dissoc(kwargs, "use_post", "data")
        )
        resp.raise_for_status()
        return StreamInfo()
