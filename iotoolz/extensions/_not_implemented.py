"""This module implements the TempStream using the original tempfile in AbcStream."""
from typing import IO, Iterable, Set, Tuple, Type

from iotoolz._abc import AbcStream, StreamInfo


def mock_stream(supported_schemas: Set[str], msg: str) -> Type[AbcStream]:
    class NotImplementedStream(AbcStream):
        """NotImplementedStream is a mock stream interface for extension streams that does not have the required dependencies installed."""

        def __init__(self, *args, **kwargs):  # pylint: disable=super-init-not-called
            raise NotImplementedError(msg)

        def read_to_iterable_(
            self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
        ) -> Tuple[Iterable[bytes], StreamInfo]:
            """
            read_to_iterable_ is an abstract method to implement the reading of the source
            resource into the a binary iterator - i.e. you will need to encode your data
            appropriately if it is a string.

            Alternatively, 'fileobj' argument is also provided where you can write to the
            stream file buffer directly. In this case, you should return an empty iterable.
            However, this will be less efficient as the actual read will only start after
            all the data have been read  into the buffer.

            The method should return a tuple of the bytes iterator and StreamInfo object
            which ideally should provide the following info:

            - content_type: (see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type)

            - encoding: (see https://docs.python.org/2.4/lib/standard-encodings.html)

            - etag: (see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag)

            An optional dict "extras" is also provided for any other information.

            Args:
                uri (str): source uri to the resource
                chunk_size (int): size for each chunk
                fileobj: (IO[bytes]): temp fileobj for the stream

            Raises:
                NotImplementedError: [description]

            Returns:
                Tuple[Iterable[bytes], StreamInfo]: tuple of the iterable and StreamInfo
                    object describing the data
            """
            raise NotImplementedError

        def write_from_fileobj_(
            self, uri: str, fileobj: IO[bytes], size: int, **kwargs
        ) -> StreamInfo:
            """
            Abstract method to implement the write to the
            destination resource from the provided file-like object.

            This file-like object only provides binary outputs - i.e. if the resources only
            accepts text input, you will need to decode the output inside this method.

            Args:
                uri (str): destination uri of the resource
                fileobj (IO[bytes]): file-like object to read from
                size (int): size of the data inside the file-like object

            Raises:
                NotImplementedError: [description]

            Returns:
                StreamInfo: StreamInfo object describing the data
            """
            raise NotImplementedError

        def stats_(self) -> StreamInfo:
            """Retrieve the StreamInfo."""
            raise NotImplementedError

        def exists(self) -> bool:
            """Whether the path points to an existing resource."""
            raise NotImplementedError

        def unlink(self, missing_ok: bool = True, **kwargs):
            """Delete and remove the resource."""
            raise NotImplementedError

        def mkdir(
            self,
            mode: int = 0o777,
            parents: bool = False,
            exist_ok: bool = False,
        ):
            """
            This abstract method should mimics pathlib.mkdir method for different streams.

            Create a new directory at this given path. If mode is given, it is combined with
            the process’ umask value to determine the file mode and access flags. If the path
            already exists, FileExistsError is raised.

            If parents is true, any missing parents of this path are created as needed; they
            are created with the default permissions without taking mode into account
            (mimicking the POSIX mkdir -p command).

            If parents is false (the default), a missing parent raises FileNotFoundError.

            If exist_ok is false (the default), FileExistsError is raised if the target
            directory already exists.

            If exist_ok is true, FileExistsError exceptions will be ignored (same behavior
            as the POSIX mkdir -p command), but only if the last path component is not an
            existing non-directory file.

            Args:
                mode (int, optional): mask mode. Defaults to 0o777.
                parents (bool, optional): If true, creates any parents if required. Defaults to False.
                exist_ok (bool, optional): If true, will not raise exception if dir already exists. Defaults to False.

            Raises:
                NotImplementedError: [description]
            """
            raise NotImplementedError

        def iter_dir_(self) -> Iterable[StreamInfo]:
            """
            If the current stream is a directory, this method should yield StreamInfo in
            the directory. Otherwise, it should yield other StreamInfo in the same
            directory (or level) as the current stream.
            """
            raise NotImplementedError

    NotImplementedStream.supported_schemas = supported_schemas

    return NotImplementedStream
