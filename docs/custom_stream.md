# Creating a custom AbcStream class

You can create your own custom `Stream` class by inheriting from the abstract class
`iotoolz.AbcStream`.

The abstract class `iotoolz.AbcStream` requires the following methods to be implemented:

```py
# This is the abstract method to get the data from the actual IO resource and return
# a Tuple with an Iterable to the data and the corresponding StreamInfo.
def read_to_iterable_(
    self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
) -> Tuple[Iterable[bytes], StreamInfo]:
    ...

# This is the abstract method to write the data to the actual IO resource.
# This method is only triggered when "close" or "save" is called.
# You should use the "fileobj" parameter (a file-like obj) to write the current data to
# the actual IO resource.
def write_from_fileobj_(
    self, uri: str, fileobj: IO[bytes], size: int, **kwargs
) -> StreamInfo:
    ...


# This is the abstract method to list the streams in the dir-like path.
# If the current stream is a directory, this method should yield uri in
# the directory. Otherwise, it should yield other uri in the same
# directory (or level) as the current stream.
def iter_dir_(self) -> Iterable[str]:
    ...
```

> `StreamInfo` is a dataclass to hold the various info about the data stream (e.g.
> content_type, encoding and etag).

Ideally, the implementation of any `AbcStream` class should also provide
`supported_schemas` (Set[str]) as a class variable. This class variable will be used
in the future to infer what sort of schemas that will be supported by the class. For
example, since `https` and `http` are supported by `iotoolz.HttpStream`, all uri that
starts with `https://` and `http://` can be handled by `iotoolz.HttpStream`.

## Example: HttpStream

```py
class HttpStream(AbcStream):
    supported_schemes = {"http", "https"}

    def read_to_iterable_(
        self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
    ) -> Tuple[Iterable[bytes], StreamInfo]:
        resp = requests.get(uri, stream=True, **cytoolz.dissoc(kwargs, "stream"))
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
        use_post = kwargs.get("use_post")
        requests_method = requests.post if use_post else requests.put
        resp = requests_method(
            uri,
            data=requests_toolbelt.StreamingIterator(size, fileobj),
            **cytoolz.dissoc(kwargs, "use_post", "data")
        )
        resp.raise_for_status()
        return StreamInfo()

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False,
    ):
        """This method does nothing as the actual HTTP call will handle any folder
        creation as part of the request."""
        ...

    def iter_dir_(self) -> Iterable[str]:
        """This method does nothing."""
        return ()
```

## Example: S3Stream

For some libraries, you may not be able to return an iterator (e.g. `boto3`). In this
case, you can write the data directly to the `fileobj`. However, this will be a little
slower as all the data needs to be written to the buffer before you can actually get
the data (i.e. `stream.read()`).

> If you return an iterable (e.g. generator), the chunks are yield as they are written
> to the buffer.

```py
# NOTE this not the actual implementation
class S3Stream(AbcStream):
    supported_schemas = {"s3", "s3a", "s3n"}

    def read_to_iterable_(
        self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
    ) -> Tuple[Iterable[bytes], StreamInfo]:
        """Downloads the S3 object to buffer with 'boto3.s3.transfer.S3Transfer'."""
        self._client.download_fileobj(
            self.bucket,
            self.key,
            fileobj,
            ExtraArgs=self._extra_download_args,
            Config=self._transfer_config,
        )
        fileobj.seek(0)  # reset to initial counter
        return (
            [],
            StreamInfo(content_type=self.content_type, encoding=self.encoding),
        )

    def write_from_fileobj_(
        self, uri: str, fileobj: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        """Uploads the data in the buffer with 'boto3.s3.transfer.S3Transfer'."""
        self._update_info(StreamInfo())
        self._client.upload_fileobj(
            fileobj,
            self.bucket,
            self.key,
            ExtraArgs={
                "ContentType": self.content_type,
                "ContentEncoding": self.encoding,
                **self._extra_upload_args,
            },
            Config=self._transfer_config,
        )
        return StreamInfo()

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False,
    ):
        """This method does nothing as you do not need to create a 'folder' for an
        object store."""
        ...

    def iter_dir_(self) -> Iterable[str]:
        ...
        # bunch of codes that to list objs based on prefix

```

## Example: Register your own custom Stream

```py
from iotoolz import AbcStream, StreamInfo
from iotoolz.streams import register_stream, open_stream

# create your custom stream
class SomeStream(AbcStream):
    supported_schemas = {"some"}

    def read_to_iterable_(
        self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
    ) -> Tuple[Iterable[bytes], StreamInfo]:
        # does nothing
        return [], StreamInfo()

    def write_from_fileobj_(
        self, uri: str, fileobj: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        # does nothing
        return StreamInfo()

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False,
    ):
        # does nothing
        ...

    def iter_dir_(self) -> Iterable[str]:
        # does nothing
        return ()

# register it to the default stream factory
register_stream(SomeStream, StreamInfo.supported_schemas)

# use the remote resource like a normal file object
with open_stream("some://foo/bar.txt", "r") as stream:
    print(stream.read())
```
