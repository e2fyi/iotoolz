# iotoolz.streams

The module `iotoolz.streams` provides a helper class
[iotoolz.streams.Streams](./classes/Streams.md) to manage the different concrete
AbcStream classes.

It also provides a default `iotoolz.streams.Streams` singleton which support most of the
implemented streams. The singleton object's methods are exposed as module callables:

- `open_stream`: corresponds to `iotoolz.streams.Streams.open`
- `register_stream` : corresponds to `iotoolz.streams.Streams.register_stream`
- `set_schema_kwargs`: corresponds to `iotoolz.streams.Streams.set_schema_kwargs`

## Usage

```py
import pandas

from iotoolz.streams import open_stream, register_stream, set_schema_kwargs

# do not verify the ssl cert for all https requests
set_schema_kwargs("https", verify=False)

# print line by line some data in from a https endpoint
with open_stream("https://foo/bar/data.txt", "r") as stream:
    for line in stream:
        print(line)

# Post some binary content to a http endpoint
with open_stream("https://foo.bar/api/data", "wb") as stream:
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
