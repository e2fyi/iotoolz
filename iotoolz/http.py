"""This module implements a HttpStream using the requests lib."""
from typing import IO, Iterable, Tuple

import cachetools
import cytoolz
import dateutil.parser
import requests
import requests_toolbelt

from iotoolz._abc import AbcStream, StreamInfo

_to_datetime = cytoolz.excepts(Exception, dateutil.parser.parse)


class HttpStream(AbcStream):
    """HttpStream is the stream interface to http/https resources using "requests"."""

    supported_schemas = {"http", "https"}

    def read_to_iterable_(
        self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
    ) -> Tuple[Iterable[bytes], StreamInfo]:
        """
        Implements a http GET request using "requests" with stream flag always set.

        Args:
            uri (str): http or https endpoint to retrieve the data.
            chunk_size (int): size for each bytes chunk (provided in the Stream constructor).
            fileobj (IO[bytes]): file-like object of the stream buffer.
            **kwargs: keyword arguments that will be passed to the "requests.get" method.

        Returns:
            Tuple[Iterable[bytes], StreamInfo]: tuple of the bytes iterable and StreamInfo.
        """
        resp = requests.get(
            uri, stream=True, **cytoolz.dissoc(kwargs, "stream", "use_post")
        )
        resp.raise_for_status()
        info = StreamInfo(
            content_type=resp.headers.get("Content-Type"),
            encoding=resp.encoding,
            etag=resp.headers.get("etag"),
        )
        return resp.iter_content(chunk_size=chunk_size), info

    def write_from_fileobj_(
        self, uri: str, fileobj: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        """
        Implements a http PUT or POST request using the "requests" lib.

        This implements takes in an additional "use_post" flag to indicate whether the
        http method should be PUT (default) or POST.

        Args:
            uri (str): http/https endpoint to PUT/POST the data.
            fileobj (IO[bytes]): file-like object of the current data in buffer.
            size (int): size of the data in the file-like object.
            **kwargs: additional keyword arguments to pass to the "requests.put" or "requests.post" method.

        Returns:
            StreamInfo: [description]
        """
        use_post = kwargs.get("use_post")
        headers = kwargs.get("headers", {})
        if "content-type" not in headers:
            headers["content-type"] = self.content_type
        if "charset=" not in headers["content-type"] and self.encoding:
            if not headers["content-type"].endswith(";"):
                headers["content-type"] += "; "
            headers["content-type"] += f"charset={self.encoding}"
        requests_method = requests.post if use_post else requests.put
        resp = requests_method(
            uri,
            data=requests_toolbelt.StreamingIterator(size, fileobj),
            headers=headers,
            **cytoolz.dissoc(kwargs, "use_post", "data", "headers"),
        )
        resp.raise_for_status()
        return StreamInfo(content_type=self.content_type, encoding=self.encoding)

    @cachetools.cached(cache=cachetools.TTLCache(maxsize=1, ttl=60))
    def stats_(self) -> StreamInfo:
        resp = requests.head(
            self.uri, **cytoolz.dissoc(self._kwargs, "stream", "use_post")
        )
        resp.raise_for_status()
        last_modified_str = resp.headers.get("Last-Modified")
        last_modified = _to_datetime(last_modified_str) if last_modified_str else None
        return StreamInfo(
            content_type=resp.headers.get("Content-Type"),
            encoding=resp.encoding,
            etag=resp.headers.get("ETag"),
            last_modified=last_modified,
        )

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False,
    ):
        """This method does nothing as the actual HTTP call will handle any folder creation as part of the request."""
        ...

    def iter_dir_(self) -> Iterable[StreamInfo]:
        """This method does nothing."""
        return ()

    def exists(self) -> bool:
        """Whether the http resource exists."""
        resp = requests.head(
            self.uri, **cytoolz.dissoc(self._kwargs, "stream", "use_post")
        )
        return resp.ok
