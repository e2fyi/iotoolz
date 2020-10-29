# `iotoolz`

## v0.1.0-rc-10

- Added `etag` as arg to stream constructor
- `S3Stream.iter_dir()` will now yield stream with etag

## v0.1.0-rc-9

- Added `exists` to check if a stream points to an existing resource
- `glob` now takes in 1 argument only, which is the pattern
- `TempStream.open`, `iotoolz.streams.open_stream`, and `iotoolz.streams.Stream` now will always return an existing `TempStream` (i.e. same uri)

## v0.1.0-rc-8

- Fix rollover to disk error
- Fix doc

## v0.1.0-rc-7

- Added path-like methods to AbcStream (i.e. similar to `pathlib.Path`)

  - `mkdir`: creates a directory if appropriate for the stream
  - `iter_dir`: list streams in the directory
  - `glob`: list streams in the directory with uri that matches the pattern

## v0.1.0-rc-6

- Fix typo for HttpStream: schemes -> schemas
- key for S3Stream should not have leading slash

## v0.1.0-rc-5

- Fix bug where dynamic mock stream class is improperly created when the required dependencies are not available.

## v0.1.0-rc-4

- Add extension `iotoolz.extensions.s3.S3Stream` which is implemented with `boto3`.

## v0.1.0-rc-3

- Converted sphinx docs to mkdocs for better look and feel.
- Removed the need for a context manager for the iterable.
- Renamed the 2 abstract methods.

## v0.1.0-rc-2

- Initial Release

  - Features:
    - `iotoolz.AbcStream` is an abstract class to represent any IO. It follows the `open` very closely.
