# iotoolz.streams

The module `iotoolz.streams` provides a helper class
[iotoolz.streams.Streams](./classes/Streams.md) to manage the different concrete
AbcStream classes.

It also provides a default `iotoolz.streams.Streams` singleton which support most of the
implemented streams. The singleton object's methods are exposed as module callables which
you can import from.

## Usage

### Basic Setup

```py
from iotoolz.streams import (
    set_schema_kwargs,
    set_buffer_rollover_size,
)

# do not verify the ssl cert for HttpStream (via requests)
set_schema_kwargs("https", verify=False)

# use a custom client for S3Stream (via boto3)
set_schema_kwargs(
    "s3",
    {
        "client": boto3.client(
            "s3",
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            aws_session_token=SESSION_TOKEN,
        )
    },
)

# buffer will rollover to disk if the data is more than 100 MB
# (default is everything is in-memory - may result in memory error)
set_buffer_rollover_size(10**8)
```

### Opening streams

You can open any stream just like python's built-in `open` method.

```py
import pandas

from iotoolz import open_stream

# print line by line some data in from a https endpoint
# and do not verify the ssl cert of the https endpoint
with open_stream(
    "https://foo/bar/data.txt",
    mode="r",
    schema_kwargs={"https": {"verify": False}}
) as stream:
    for line in stream:
        print(line)

# POST some binary content to a http endpoint (default is PUT)
with open_stream("https://foo.bar/api/data", "wb", use_post=True) as stream:
    stream.write(b"hello world")

# Copying a local file to s3
with open_stream("path/to/data.csv", "r") as csv_source,
     open_stream("s3://bucket/foobar.txt?StorageClass=STANDARD", "w") as s3_sink:
    # pipe content in csv_source to tmpsink
    csv_source.pipe(s3_sink)

# load to pandas dataframe from s3 fileobj
with open_stream("s3://bucket/foobar.csv", "r") as csv:
    df = pd.read_csv(csv)

```

## Stream-like operations

`Stream` is an alias of `open_stream`, both methods return a concrete `AbcStream` object.
You can treat the object as both a "file-like" and "stream-like" object - i.e. you can
read, write, seek, flush, close the object.

> NOTE
>
> By default, the underlying buffer is in-memory. You can enable rollover to disk by
> passing the `inmem_size` arg to the method, or update the default `inmem_size` value
> with the `iotoolz.streams.set_buffer_rollover_size` method.

```py
from iotoolz import open_stream, Stream, set_buffer_rollover_size

# `Stream` is an alias of `open_stream`
assert open_stream == Stream

# rollover to disk if data is over 100 MB
set_buffer_rollover_size(10**8)

# you can overwrite the default kwargs here also
stream = Stream(
    "path/to/data.txt",
    mode="rw",  # you can both read and write to a stream
)
# stream is lazily evaluated, nothing will be buffered until you call some methods
# that requires the data
data = stream.read()
# will attempt to provide encoding and content_type if not provided when opening the stream
print(stream.encoding)
print(stream.content_type)
# stream has the same interface as an IO object - i.e. u can seek, flush, close, etc
stream.seek(5)  # go to offset 5 from start of buffer
stream.write("replace with this text")
stream.seek(0, whence=2)  # go to end of buffer
stream.write("additional text after original eof")  # continue writing to the end of the buffer
stream.save()  # flush save the entire buffer to the same dst location
stream.close() # close the stream
```

## Path-like operations

`mkdir`, `iter_dir` and `glob` are path-like methods that are available to the
stream object. These methods mimics their equivalent in `pathlib.Path` when appropriate.

| method     | supported streams                          | desc                                                            |
| ---------- | ------------------------------------------ | --------------------------------------------------------------- |
| `mkdir`    | `FileStream`                               | create a directory.                                             |
| `iter_dir` | `FileStream`, `TempStream`, and `S3Stream` | iterate thru the streams in the directory.                      |
| `glob`     | `FileStream`, `TempStream`, and `S3Stream` | iterate thru the streams in the directory that match a pattern. |

```py
import itertools

from iotoolz import Stream, mkdir, iter_dir, glob

# similar to 'mkdir -p'
mkdir("path/to/folder", parents=True, exist_ok=True)
Stream("path/to/folder").mkdir(parents=True, exist_ok=True)

# list object in an s3 bucket
iter_dir("s3://bucket/prefix/")
for stream in Stream("s3://bucket/prefix/").iter_dir():
    print(stream.uri)

# find s3 objects with a specific pattern
glob("s3://bucket/prefix/", "*.txt")
for stream in Stream("s3://bucket/prefix/").glob("*.txt"):
    print(stream.uri)

```
