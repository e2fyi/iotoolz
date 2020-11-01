# iotoolz

[![PyPI version](https://badge.fury.io/py/iotoolz.svg)](https://badge.fury.io/py/iotoolz)
[![Build Status](https://travis-ci.com/e2fyi/iotoolz.svg?branch=master)](https://travis-ci.com/github/e2fyi/iotoolz)
[![Coverage Status](https://coveralls.io/repos/github/e2fyi/iotoolz/badge.svg?branch=master)](https://coveralls.io/github/e2fyi/iotoolz?branch=master)
[![Documentation Status](https://readthedocs.org/projects/iotoolz/badge/?version=latest)](https://iotoolz.readthedocs.io/en/latest/?badge=latest)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Downloads](https://pepy.tech/badge/iotoolz/month)](https://pepy.tech/project/iotoolz)

`iotoolz` is an improvement over `e2fyi-utils` and is inspired partly by `toolz`.
`iotoolz` is a lib to help provide a consistent dev-x for interacting with any IO resources.
It provides an abstract class `iotoolz.AbcStream` which mimics python's native `open`
very closely (with some additional parameters and methods such as `save`).

API documentation can be found at [https://iotoolz.readthedocs.io/en/latest/](https://iotoolz.readthedocs.io/en/latest/).

Change logs are available in [CHANGELOG.md](https://github.com/e2fyi/iotoolz/blob/master/CHANGELOG.md).

> - Python 3.6 and above
> - Licensed under [Apache-2.0](./LICENSE).

## Supported streams

Current the following streams are supported:

- `iotoolz.FileStream`: wrapper over built-in `open` function (`file://`)
- `iotoolz.TempStream`: in-memory stream that will rollover to disk (`tmp://`, `temp://`)
- `iotoolz.HttpStream`: http or https stream implemented with `requests` (`http://`, `https://`)
- `iotoolz.extensions.S3Stream`: s3 stream implemented with `boto3` (`s3://`, `s3a://`, `s3n://`)

## Installation

```bash
# install the default packages only (most lite-weight)
pip install iotoolz

# install dependencies for specific extension
pip install iotoolz[boto3]

# install all the extras
pip install iotoolz[all]
```

Available extras:

- `all`: All the optional dependencies
- `boto3`: `boto3` for `iotoolz.extensions.S3Stream`
- `minio`: TODO

## Quickstart

The helper object `iotoolz.streams.stream_factory` is a default singleton of
`iotoolz.streams.Streams` provided to support most of the common use cases.

`iotoolz.streams.open_stream` (alias `iotoolz.streams.Stream`) and is a util method
provided by the singleton helper to create a stream object. This method accepts the same
arguments as python's `open` method with the following additional parameters:

- `data`: optional str or bytes that will be passed into the stream
- `fileobj`: optional file-like object which will be copied into the stream
- `content_type`: optional mime type information to describe the stream (e.g. application/json)
- `inmem_size`: determines how much memory to allocate to the stream before rolling over to local file system. Defaults to no limits (may result in MemoryError).
- `schema_kwargs`: optional mapping of schemas to their default kwargs.

### Basic Setup

```py
from iotoolz.streams import (
    set_schema_kwargs,
    set_buffer_rollover_size,
)

# set params to pass to the Stream obj handling https
# i.e. HttpStream (implemented with requests)
set_schema_kwargs(
    "https",
    verify=False,  # do not verify ssl cert
    use_post=True  # use POST instead of PUT when writing to https
)

# use a custom client for S3Stream (via boto3)
set_schema_kwargs(
    "s3",
    client=boto3.client(
        "s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN,
    )
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

## TempStream

`TempStream` is a stream can functions like a virtual file system in memory.

```py
import gc

from iotoolz import Stream, exists, glob, iter_dir

# this stream can be garbage collected
Stream("tmp://foo/bar/data.txt", data="foobar")

# True if not gc yet, False if already gc
exists("tmp://foo/bar/data.txt")

# force gc
gc.collect()
# will not exist
exists("tmp://foo/bar/data.txt")

# create temp stream with strong ref (hence will not be gc)
s1 = Stream("tmp://foo/bar/data.txt", data="foobar")
s2 = Stream("tmp://foo/example.txt", data="...")

# returns s1 and s2
iter_dir("tmp://foo/")

# returns s1 only
glob("tmp://foo/bar/*.txt")
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

`exists`, `mkdir`, `iter_dir` and `glob` are path-like methods that are available to the
stream object. These methods mimics their equivalent in `pathlib.Path` when appropriate.

| method                       | supported streams                           | desc                                                                                       |
| ---------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `stats`                      | All Streams                                 | return the StreamInfo for an existing resource                                             |
| `unlink`, `delete`, `remove` | All Streams                                 | Delete and remove the stream (except for `TempStream` where the buffer is cleared instead) |
| `exists`                     | All Streams                                 | check if a stream points to an existing resource.                                          |
| `mkdir`                      | `FileStream`                                | create a directory.                                                                        |
| `rmdir`                      | `FileStream`, `TempStream`, and `S3Stream`, | remove recursively everything in the directory.                                            |
| `iter_dir`                   | `FileStream`, `TempStream`, and `S3Stream`  | iterate thru the streams in the directory.                                                 |
| `glob`                       | `FileStream`, `TempStream`, and `S3Stream`  | iterate thru the streams in the directory that match a pattern.                            |

```py
import itertools

from iotoolz import Stream, mkdir, iter_dir, glob, exists

# similar to 'mkdir -p'
mkdir("path/to/folder", parents=True, exist_ok=True)
Stream("path/to/folder").mkdir(parents=True, exist_ok=True)

# list object in an s3 bucket
iter_dir("s3://bucket/prefix/")
for stream in Stream("s3://bucket/prefix/").iter_dir():
    print(stream.uri)

# find s3 objects with a specific pattern
glob("s3://bucket/prefix/*txt")
for stream in Stream("s3://bucket/prefix/").glob("*.txt"):
    print(stream.uri)

# exists
exists("s3://bucket/prefix/foo.txt")

# stats
info = stats("s3://bucket/prefix/foo.txt")
print(info.name)
print(info.content_type)
print(info.encoding)
print(info.last_modified)
print(info.etag)
print(info.extras)

# delete resource
unlink("s3://bucket/prefix/foo.txt")

# rm all key with prefix
rmdir("s3://bucket/prefix/")

```

## Piping streams

`pipe` is method to push data to a sink (similar to NodeJS stream except it has no
watermark or buffering).

```py
from  iotoolz.streams import open_stream

local_file = open_stream(
    "path/to/google.html", content_type="text/html", mode="w"
)
temp_file = open_stream(
    "tmp://google.html", content_type="text/html", mode="wb"
)

# when source is closed, all sinks will be closed also
with open_stream("https://google.com") as source:
    # writes to a temp file then to a local file in sequence
    source.pipe(temp_file).pipe(local_file)


local_file2 = open_stream(
    "path/to/google1.html", content_type="text/html", mode="w"
)
local_file3 = open_stream(
    "path/to/google2.html", content_type="text/html", mode="w"
)

# when source is closed, all sinks will be closed also
with open_stream("tmp://foo_src", mode="w") as source:
    # writes in a fan shape manner
    source.pipe(local_file2)
    source.pipe(local_file3)

    source.write("hello world")
```

> TODO support transform streams so that pipe can be more useful
