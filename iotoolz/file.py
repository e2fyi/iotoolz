"""This module implements the FileStream with python native "open" method."""
import datetime
import io
import os
import os.path
import pathlib
import shutil
from typing import IO, Iterable, Tuple, Union

from iotoolz._abc import AbcStream, StreamInfo
from iotoolz.utils import guess_content_type_from_file


class FileStream(AbcStream):
    """FileStream is the stream interface to the local file system with python's "open" method."""

    supported_schemas = {"", "file"}

    def __init__(
        self,
        uri: Union[str, pathlib.Path],
        mode: str = "r",
        buffering: int = -1,
        encoding: str = None,
        newline: str = None,
        content_type: str = "",
        inmem_size: int = None,
        delimiter: Union[str, bytes] = None,
        chunk_size: int = io.DEFAULT_BUFFER_SIZE,
        etag: str = "",
        **kwargs,
    ):
        super().__init__(
            str(uri),
            mode,
            buffering,
            encoding,
            newline,
            content_type,
            inmem_size,
            delimiter,
            chunk_size,
            etag,
            **kwargs,
        )

    def read_to_iterable_(
        self, uri: str, chunk_size: int, fileobj: IO[bytes], **kwargs
    ) -> Tuple[Iterable[bytes], StreamInfo]:
        last_modified = datetime.datetime.fromtimestamp(
            pathlib.Path(self.uri).stat().st_mtime
        )

        def iter_bytes() -> Iterable[bytes]:
            with open(
                self.uri, mode="rb", buffering=self.buffering, newline=self.newline,
            ) as stream:
                for chunk in stream:
                    yield chunk

        return (
            iter_bytes(),
            StreamInfo(
                content_type=guess_content_type_from_file(self.uri),
                last_modified=last_modified,
            ),
        )

    def write_from_fileobj_(
        self, uri: str, fileobj: IO[bytes], size: int, **kwargs
    ) -> StreamInfo:
        os.makedirs(os.path.dirname(uri), exist_ok=True)
        mode = self.mode.replace("r", "")
        if "b" not in mode:
            mode += "b"
        with open(
            self.uri,
            mode=mode,
            buffering=self.buffering,
            encoding=None,
            newline=self.newline,
        ) as stream:
            shutil.copyfileobj(fileobj, stream)
        last_modified = datetime.datetime.fromtimestamp(
            pathlib.Path(self.uri).stat().st_mtime
        )
        return StreamInfo(last_modified=last_modified)

    def stats_(self) -> StreamInfo:
        last_modified = datetime.datetime.fromtimestamp(
            pathlib.Path(self.uri).stat().st_mtime
        )
        return StreamInfo(
            content_type=guess_content_type_from_file(self.uri),
            last_modified=last_modified,
        )

    def unlink(self, missing_ok: bool = True, **kwargs):
        try:
            pathlib.Path(self.uri).unlink()
        except FileNotFoundError:
            if not missing_ok:
                raise

    def is_dir(self) -> bool:
        """Whether stream points to a existing dir."""
        return pathlib.Path(self.uri).is_dir()

    def is_file(self) -> bool:
        """Whether stream points to a existing file."""
        return pathlib.Path(self.uri).is_file()

    def rmdir(self, ignore_errors: bool = False, **kwargs) -> "FileStream":
        """Remove the entire directory."""
        print(self.uri)
        shutil.rmtree(self.uri, ignore_errors=ignore_errors, **kwargs)
        return self

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False,
    ):
        """
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
        """
        pathlib.Path(self.uri).mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    def iter_dir_(self) -> Iterable[StreamInfo]:
        dirpath = self.uri if os.path.isdir(self.uri) else os.path.dirname(self.uri)
        for entry in os.scandir(dirpath):
            yield StreamInfo(
                uri=entry.path,
                name=entry.name,
                last_modified=datetime.datetime.fromtimestamp(entry.stat().st_mtime),
            )

    def exists(self) -> bool:
        """Whether the stream points to an existing resource."""
        return pathlib.Path(self.uri).exists()
