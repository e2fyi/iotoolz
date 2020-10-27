# `iotoolz`

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
