# `iotoolz`

## v0.1.0-rc-16

- Added `MinioStream` (`minio://`)
- Added `seekable` method for AbcStream

## v0.1.0-rc-15

- Fix  system markers for windows so that python-magic is installed correctly on windows machine.

## v0.1.0-rc-14

- Loosen the dependencies requirements so that it is more forgiving with older packages (e.g. using an older version of requests).

## v0.1.0-rc-13

- `cytoolz` is now an optional package.

## v0.1.0-rc-12

- Change how the buffer is flushed and closed when the stream is GC or when the application exits.

## v0.1.0-rc-11

- Fix error when running inside a thread (will not register signal handler if inside a thread). Dev need to ensure stream is flushed and closed properly.

## v0.1.0-rc-10

- Added `stats` and `unlink` abstract methods to `AbcStream`
- Added `rmdir`, `is_file`, `is_dir` methods to `AbcStream`
- Fix `glob` to correctly match both dir and filename
- `S3Stream.iter_dir()` will now yield streams with `StreamInfo`
- `AbcStream.iter_dir_()` now return `Iterable[StreamInfo]` instead of `Iterable[str]`
- Refractored the internals of AbcStream to be cleaner

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
