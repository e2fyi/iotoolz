# iotoolz

[![PyPI version](https://badge.fury.io/py/iotoolz.svg)](https://badge.fury.io/py/iotoolz)
[![Build Status](https://travis-ci.com/e2fyi/iotoolz.svg?branch=master)](https://travis-ci.com/github/e2fyi/iotoolz)
[![Coverage Status](https://coveralls.io/repos/github/e2fyi/iotoolz/badge.svg?branch=master)](https://coveralls.io/github/e2fyi/iotoolz?branch=master)
[![Documentation Status](https://readthedocs.org/projects/iotoolz/badge/?version=latest)](https://iotoolz.readthedocs.io/en/latest/?badge=latest)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Downloads](https://pepy.tech/badge/iotoolz/month)](https://pepy.tech/project/iotoolz/month)

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

### iotoolz.streams

The helper object `iotoolz.streams.stream_factory` is a default singleton of
`iotoolz.streams.Streams` provided to support most of the common use cases.

`iotoolz.streams.open_stream` is a util method provided by the singleton helper to create
a stream object. This method accepts the same arguments as python's `open` method with
the following additional parameters:

- `data`: optional str or bytes that will be passed into the stream
- `fileobj`: optional file-like object which will be copied into the stream
- `content_type`: optional mime type information to describe the stream (e.g. application/json)
- `inmem_size`: determines how much memory to allocate to the stream before rolling over to local file system. Defaults to no limits (may result in MemoryError).
- `schema_kwargs`: optional mapping of schemas to their default kwargs.

```py
from iotoolz.streams import open_stream

default_schema_kwargs = {
    "https": {"verify": False}  # pass to requests - i.e. don't verify ssl
}

# this will return a stream that reads from the site
http_google = open_stream(
    "https://google.com",
    mode="r",
    schema_kwargs=default_schema_kwargs
)

html = http_google.read()
content_type = http_google.content_type
encoding = http_google.encoding

# this will write to the https endpoint using the POST method (default is PUT)
with open_stream("https://foo/bar", mode="wb", use_post=True) as stream:
    stream.write(b"hello world")


# this will write to a local path
# save will write the current content to the local file
foo_txt = open_stream(
    "path/to/foo.txt",
    mode="w",
    content_type="text/plain",
    encoding="utf-8",
    data="foo bar",
).save()

# go to the end of the buffer
foo_txt.seek(0, whence=2)
# append more data
foo_txt.write("\nnext line")
# save and close the data
foo_txt.close()


# save a local file to S3
with open_stream("key.txt", "rb") as csv_source,
     open_stream("s3://bucket/folder/key.txt", "wb") as s3_sink:
    csv_source.pipe(s3_sink)
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
