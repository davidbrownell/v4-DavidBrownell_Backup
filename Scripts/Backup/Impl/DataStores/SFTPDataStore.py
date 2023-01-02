# ----------------------------------------------------------------------
# |
# |  SFTPDataStore.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-25 11:15:55
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022-23
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Contains the SFTPDataStore object"""

import stat
import textwrap
import traceback

from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Iterator, List, Optional, Tuple, Union

import paramiko

from paramiko.config import SSH_PORT

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation.Streams.DoneManager import DoneManager, DoneManagerException
from Common_Foundation import TextwrapEx
from Common_Foundation.Types import overridemethod
from Common_Foundation import Types

from .FileBasedDataStore import FileBasedDataStore, ItemType


# ----------------------------------------------------------------------
class SFTPDataStore(FileBasedDataStore):
    """DataStore assessable via a SFTP server"""

    # ----------------------------------------------------------------------
    @classmethod
    @contextmanager
    def Create(
        cls,
        dm: DoneManager,
        host: str,
        username: str,
        private_key_or_password: Union[Path, str],
        working_dir: Optional[Path]=None,
        *,
        port: int=SSH_PORT,
    ) -> Iterator["SFTPDataStore"]:
        log_filename = Path("paramiko.log").resolve()

        paramiko.util.log_to_file(str(log_filename))  # type: ignore

        ssh = paramiko.SSHClient()

        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if isinstance(private_key_or_password, Path):
            password = None
            private_key_file = paramiko.RSAKey.from_private_key_file(str(private_key_or_password))
        else:
            password = private_key_or_password
            private_key_file = None

        # Connect
        error: Optional[str] = None

        try:
            ssh.connect(
                host,
                port=port,
                username=username,
                password=password,
                pkey=private_key_file,
            )
        except Exception as ex:
            if dm.is_debug:
                error = traceback.format_exc()
            else:
                error = str(ex)

        if error:
            raise DoneManagerException(
                textwrap.dedent(
                    """\
                    Unable to connect.

                        Host:           {}
                        Port:           {}
                        Username:       {}

                        Error:
                            {}

                        Additional information is available at {}.
                    """,
                ).format(
                    host,
                    port,
                    username,
                    TextwrapEx.Indent(
                        error,
                        8,
                        skip_first_line=True,
                    ).rstrip(),
                    log_filename if dm.capabilities.is_headless else TextwrapEx.CreateAnsiHyperLink(
                        "file:///{}".format(log_filename.as_posix()),
                        str(log_filename),
                    ),
                ),
            )

        with ExitStack(ssh.close):
            error: Optional[str] = None

            try:
                sftp = ssh.open_sftp()

                sftp.chdir(str(working_dir))

                yield cls(sftp)

            except Exception as ex:
                if dm.is_debug:
                    error = traceback.format_exc()
                else:
                    error = str(ex)

            if error:
                raise DoneManagerException(
                    textwrap.dedent(
                        """\
                        Unable to open the SFTP client.

                            Host:           {}
                            Port:           {}
                            Username:       {}
                            Working Dir:    {}

                            Error:
                                {}

                            Additional information is available at {}.
                        """,
                    ).format(
                        host,
                        port,
                        username,
                        working_dir.as_posix() if working_dir else "<None>",
                        TextwrapEx.Indent(
                            error,
                            8,
                            skip_first_line=True,
                        ).rstrip(),
                        log_filename if dm.capabilities.is_headless else TextwrapEx.CreateAnsiHyperLink(
                            "file:///{}".format(log_filename.as_posix()),
                            str(log_filename),
                        ),
                    ),
                )

    # ----------------------------------------------------------------------
    def __init__(
        self,
        sftp_client: paramiko.SFTPClient,
    ):
        super(SFTPDataStore, self).__init__()

        self._client                        = sftp_client

    # ----------------------------------------------------------------------
    @overridemethod
    def ExecuteInParallel(self) -> bool:
        return False

    # ----------------------------------------------------------------------
    @overridemethod
    def ValidateBackupInputs(
        self,
        input_filename_or_dirs: List[Path],  # pylint: disable=unused-argument
    ) -> None:
        # Nothing to do here
        pass

    # ----------------------------------------------------------------------
    @overridemethod
    def SnapshotFilenameToDestinationName(
        self,
        path: Path,
    ) -> Path:
        if path.parts[0]:
            # Probably on Windows
            path = Path(path.parts[0].replace(":", "_").rstrip("\\")) / Path(*path.parts[1:])
        else:
            if not path.parts[0]:
                path = Path(*path.parts[1:])

        return path

    # ----------------------------------------------------------------------
    @overridemethod
    def GetBytesAvailable(self) -> Optional[int]:
        # We don't have APIs to implement this functionality
        return None

    # ----------------------------------------------------------------------
    @overridemethod
    def GetWorkingDir(self) -> Path:
        return Path(self._client.getcwd() or "")

    # ----------------------------------------------------------------------
    @overridemethod
    def SetWorkingDir(
        self,
        path: Path,
    ) -> None:
        self._client.chdir(path.as_posix())

    # ----------------------------------------------------------------------
    @overridemethod
    def GetItemType(
        self,
        path: Path,
    ) -> Optional[ItemType]:
        try:
            result = self._client.stat(path.as_posix())
            assert result.st_mode is not None

            if stat.S_IFMT(result.st_mode) == stat.S_IFDIR:
                return ItemType.Dir

            return ItemType.File

        except FileNotFoundError:
            return None

    # ----------------------------------------------------------------------
    @overridemethod
    def GetFileSize(
        self,
        path: Path,
    ) -> int:
        return Types.EnsureValid(self._client.stat(path.as_posix()).st_size)

    # ----------------------------------------------------------------------
    @overridemethod
    def RemoveDir(
        self,
        path: Path,
    ) -> None:
        try:
            # The client can only remove empty directories, so make it empty
            dirs_to_remove: List[Path] = []

            for root, directories, filenames in self.Walk(path):
                for filename in filenames:
                    self.RemoveFile(root / filename)

                dirs_to_remove.append(root)

            for dir_to_remove in reversed(dirs_to_remove):
                self._client.rmdir(dir_to_remove.as_posix())

        except FileNotFoundError:
            # There is no harm in attempting to remove the dir if it does not exist
            pass

    # ----------------------------------------------------------------------
    @overridemethod
    def RemoveFile(
        self,
        path: Path,
    ) -> None:
        try:
            self._client.unlink(path.as_posix())
        except FileNotFoundError:
            # There is no harm in attempting to remove the file if it does not exist
            pass

    # ----------------------------------------------------------------------
    @overridemethod
    def MakeDirs(
        self,
        path: Path,
    ) -> None:
        try:
            self._client.mkdir(path.as_posix())
        except OSError as ex:
            if "exists" not in str(ex):
                raise

    # ----------------------------------------------------------------------
    @overridemethod
    @contextmanager
    def Open(
        self,
        filename: Path,
        *args,
        **kwargs,
    ):
        with self._client.open(filename.as_posix(), *args, **kwargs) as f:
            yield f

    # ----------------------------------------------------------------------
    @overridemethod
    def Rename(
        self,
        old_path: Path,
        new_path: Path,
    ) -> None:
        self.RemoveItem(new_path)
        self._client.rename(old_path.as_posix(), new_path.as_posix())

    # ----------------------------------------------------------------------
    @overridemethod
    def Walk(
        self,
        path: Path=Path(),
    ) -> Generator[
        Tuple[
            Path,                           # root
            List[str],                      # directories
            List[str],                      # filenames
        ],
        None,
        None,
    ]:
        to_search: List[Path] = [Path(path), ]

        while to_search:
            search_dir = to_search.pop(0)

            if self.GetItemType(search_dir) != ItemType.Dir:
                continue

            directories: List[str] = []
            filenames: List[str] = []

            for item in self._client.listdir_attr(search_dir.as_posix()):
                assert item.st_mode is not None

                is_dir = stat.S_IFMT(item.st_mode) == stat.S_IFDIR

                if is_dir:
                    directories.append(item.filename)
                else:
                    filenames.append(item.filename)

            yield search_dir, directories, filenames

            to_search += [search_dir / directory for directory in directories]
