# ----------------------------------------------------------------------
# |
# |  Common.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-28 14:21:45
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Implements functionality used by Mirror and Offsite"""

import hashlib
import re
import textwrap

from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import auto, Enum
from pathlib import Path
from typing import Any, Callable, cast, Dict, Iterable, Iterator, List, Optional, Pattern, Tuple, Union, TYPE_CHECKING
from urllib import parse as urlparse

from Common_Foundation.Shell.All import CurrentShell
from Common_Foundation.Streams.DoneManager import DoneManager
from Common_Foundation import TextwrapEx

from Common_FoundationEx import ExecuteTasks
from Common_FoundationEx.InflectEx import inflect

from .DataStores.DataStore import DataStore, ItemType
from .DataStores.FileSystemDataStore import FileSystemDataStore
from .DataStores.SFTPDataStore import SFTPDataStore, SSH_PORT

if TYPE_CHECKING:
    from .Snapshot import Snapshot  # pragma: no cover


# ----------------------------------------------------------------------
# |
# |  Public Types
# |
# ----------------------------------------------------------------------
SFTP_TEMPLATE_REGEX                         = re.compile(
    textwrap.dedent(
        r"""(?#
        Start                               )^(?#
        Prefix                              )ftp:\/\/(?#
        Username                            )(?P<username>[^\s:]+)(?#
        [sep]                               ):(?#
        Posix Private Key Path              )(?P<password_or_private_key_path>[^@]+)(?#
        [sep]                               )@(?#
        Host                                )(?P<host>[^:\/]+)(?#
        Port Group Begin                    )(?:(?#
            [sep]                           ):(?#
            Port                            )(?P<port>\d+)(?#
        Port Group End                      ))?(?#
        Working Group Begin                 )(?:(?#
            [sep]                           )/(?#
            Posix Working Dir               )(?P<working_dir>.+)(?#
        Working Group End                   ))?(?#
        End                                 )$(?#
        )""",
    ),
)


# ----------------------------------------------------------------------
class DiffOperation(Enum):
    """Defines the cause of a difference in files"""

    add                                     = auto()
    remove                                  = auto()
    modify                                  = auto()


# ----------------------------------------------------------------------
@dataclass(frozen=True)
class DirHashPlaceholder(object):
    """Object that signals that absence of a hash value because the associated item is a directory"""

    # ----------------------------------------------------------------------
    explicitly_added: bool                  = field(kw_only=True)

    # ----------------------------------------------------------------------
    def __eq__(self, other) -> bool:
        return isinstance(other, self.__class__)

    # ----------------------------------------------------------------------
    def __ne__(self, other) -> bool:
        return not self == other


# ----------------------------------------------------------------------
@dataclass(frozen=True)
class DiffResult(object):
    """Represents a difference between a file at a source and destination"""

    # ----------------------------------------------------------------------
    operation: DiffOperation
    path: Path

    # Used when operation is [add, update]
    this_hash: Union[None, str, DirHashPlaceholder]
    this_file_size: Optional[int]

    # Used when operation is [remove, update]
    other_hash: Union[None, str, DirHashPlaceholder]
    other_file_size: Optional[int]

    # ----------------------------------------------------------------------
    def __post_init__(self):
        assert (
            (self.operation == DiffOperation.add and self.this_hash is not None and self.other_hash is None)
            or (self.operation == DiffOperation.modify and self.this_hash is not None and self.other_hash is not None)
            or (self.operation == DiffOperation.remove and self.this_hash is None and self.other_hash is not None)
        ), "Instance is in an inconsistent state"

        assert (
            (self.this_hash is None and self.this_file_size is None)
            or (
                self.this_hash is not None
                and (
                    (isinstance(self.this_hash, DirHashPlaceholder) and self.this_file_size is None)
                    or (isinstance(self.this_hash, str) and self.this_file_size is not None)
                )
            )
        ), "'this' values are in an inconsistent state"

        assert (
            (self.other_hash is None and self.other_file_size is None)
            or (
                self.other_hash is not None
                and (
                    (isinstance(self.other_hash, DirHashPlaceholder) and self.other_file_size is None)
                    or (isinstance(self.other_hash, str) and self.other_file_size is not None)
                )
            )
        ), "'other' values are in an inconsistent state"

        assert (
            self.operation != DiffOperation.modify
            or (
                isinstance(self.this_hash, str)
                and isinstance(self.other_hash, str)
                and self.this_hash != self.other_hash
            )
        ), "modify values are in an inconsistent state"

    # ----------------------------------------------------------------------
    def ToJson(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "operation": str(self.operation).split(".")[1],
            "path": self.path.as_posix(),
        }

        if isinstance(self.this_hash, str):
            assert self.this_file_size is not None

            result["this_hash"] = self.this_hash
            result["this_file_size"] = self.this_file_size

        if isinstance(self.other_hash, str):
            assert self.other_file_size is not None

            result["other_hash"] = self.other_hash
            result["other_file_size"] = self.other_file_size

        return result

    # ----------------------------------------------------------------------
    @classmethod
    def FromJson(
        cls,
        values: Dict[str, Any],
    ) -> "DiffResult":
        if values["operation"] == "add":
            operation = DiffOperation.add

            this_hash = values.get("this_hash", DirHashPlaceholder(explicitly_added=False))
            this_file_size = values.get("this_file_size", None)

            other_hash = None
            other_file_size = None

        elif values["operation"] == "modify":
            operation = DiffOperation.modify

            this_hash = values["this_hash"]
            this_file_size = values["this_file_size"]

            other_hash = values["other_hash"]
            other_file_size = values["other_file_size"]

        elif values["operation"] == "remove":
            operation = DiffOperation.remove

            this_hash = None
            this_file_size = None

            other_hash = values.get("other_hash", DirHashPlaceholder(explicitly_added=False))
            other_file_size = values.get("other_file_size", None)

        else:
            assert False, values["optional"]  # pragma: no cover

        return cls(
            operation,
            Path(values["path"]),
            this_hash,
            this_file_size,
            other_hash,
            other_file_size,
        )


# ----------------------------------------------------------------------
EXECUTE_TASKS_REFRESH_PER_SECOND            = 2


# ----------------------------------------------------------------------
PENDING_COMMIT_EXTENSION                    = ".__pending_commit__"
PENDING_DELETE_EXTENSION                    = ".__pending_delete__"


# ----------------------------------------------------------------------
# |
# |  Public Functions
# |
# ----------------------------------------------------------------------
def GetDestinationHelp() -> str:
    return textwrap.dedent(
        """\
        Data Store Destinations
        =======================
        The value provided on the command line for 'destination' can be any of these formats...

        File System
        -----------
        Writes content to the local file system.

            Data Store Destination Examples:
                - /home/mirrored_content
                - C:\\MirroredContent

        SFTP
        ----
        Writes content to a SFTP server.

            Format:
                ftp://<username>:<password or posix path to private key>@<host>[:<port>][/<working_dir>]

            Data Store Destination Examples:
                ftp://my_username:my_password@my_server.com
                ftp://my_username:my_password@my_server.com/A/Working/Dir
                ftp://my_username:/path/to/private/key@my_server.com
                ftp://my_username:/path/to/private/key@my_server.com/A/Working/Dir
        """,
    ).replace("\n", "\n\n")


# ----------------------------------------------------------------------
def GetTaskDisplayName(
    filename: Path,
) -> str:
    return TextwrapEx.BoundedLJust(str(filename), 100)


# ----------------------------------------------------------------------
@contextmanager
def YieldDataStore(
    dm: DoneManager,
    destination: Union[str, Path],
    *,
    ssd: bool,
) -> Iterator[DataStore]:
    if isinstance(destination, str):
        sftp_match = SFTP_TEMPLATE_REGEX.match(destination)
        if sftp_match:
            private_key_or_password = sftp_match.group("password_or_private_key_path")

            private_key_filename = Path(private_key_or_password)
            if private_key_filename.is_file():
                private_key_or_password = private_key_filename

            working_dir = sftp_match.group("working_dir")
            if working_dir:
                working_dir = Path(urlparse.unquote(working_dir))
            else:
                working_dir = None

            with SFTPDataStore.Create(
                dm,
                sftp_match.group("host"),
                sftp_match.group("username"),
                private_key_or_password,
                working_dir,
                port=int(sftp_match.group("port") or SSH_PORT),
            ) as data_store:
                yield data_store
                return

    # Create a FileSystemDataStore instance
    is_local_filesystem_override_value_for_testing: Optional[bool] = None

    # '[nonlocal]' should only be used while testing
    if isinstance(destination, str) and destination.startswith("[nonlocal]"):
        original_destination = destination
        destination = destination[len("[nonlocal]"):]

        dm.WriteInfo(
            textwrap.dedent(
                """\
                The destination string used to create a 'FileSystemDataStore' instance has been explicitly declared as nonlocal;
                this should only be used in testing scenarios.

                    Connection:  {}
                    Filename:    {}

                """,
            ).format(original_destination, destination),
        )

        is_local_filesystem_override_value_for_testing = False

    yield FileSystemDataStore(
        Path(destination),
        ssd=ssd,
        is_local_filesystem_override_value_for_testing=is_local_filesystem_override_value_for_testing,
    )


# ----------------------------------------------------------------------
def CreateFilterFunc(
    file_includes: Optional[List[Pattern]],
    file_excludes: Optional[List[Pattern]],
) -> Optional[Callable[[Path], bool]]:
    if not file_includes and not file_excludes:
        return None

    # ----------------------------------------------------------------------
    def SnapshotFilter(
        filename: Path,
    ) -> bool:
        filename_str = filename.as_posix()

        if file_excludes is not None and any(exclude.search(filename_str) for exclude in file_excludes):
            return False

        if file_includes is not None and not any(include.search(filename_str) for include in file_includes):
            return False

        return True

    # ----------------------------------------------------------------------

    return SnapshotFilter


# ----------------------------------------------------------------------
def CalculateDiffs(
    dm: DoneManager,
    source_snapshot: "Snapshot",
    dest_snapshot: "Snapshot",
) -> Dict[DiffOperation, List[DiffResult]]:
    diffs: Dict[DiffOperation, List[DiffResult]] = {
        # This order should remain consistent, as removes must happen before adds
        DiffOperation.remove: [],
        DiffOperation.add: [],
        DiffOperation.modify: [],
    }

    with dm.Nested(
        "\nCalculating diffs...",
        lambda: "{} found".format(inflect.no("diff", sum(len(diff_items) for diff_items in diffs.values()))),
        suffix="\n",
    ) as diff_dm:
        for diff in source_snapshot.Diff(dest_snapshot):
            assert diff.operation in diffs, diff.operation
            diffs[diff.operation].append(diff)

        if dm.is_verbose:
            with diff_dm.YieldVerboseStream() as stream:
                wrote_content = False

                for desc, operation in [
                    ("Adding", DiffOperation.add),
                    ("Modifying", DiffOperation.modify),
                    ("Removing", DiffOperation.remove),
                ]:
                    these_diffs = diffs[operation]
                    if not these_diffs:
                        continue

                    stream.write(
                        textwrap.dedent(
                            """\
                            {}{}:
                            """,
                        ).format(
                            "\n" if wrote_content else "",
                            desc,
                        ),
                    )

                    for diff_index, diff in enumerate(these_diffs):
                        stream.write(
                            "  {}) [{}] {}\n".format(
                                diff_index + 1,
                                "FILE" if diff.path.is_file()
                                    else "DIR " if diff.path.is_dir()
                                        else "????"
                                ,
                                diff.path if dm.capabilities.is_headless else TextwrapEx.CreateAnsiHyperLink(
                                    "file:///{}".format(diff.path.as_posix()),
                                    str(diff.path),
                                ),
                            ),
                        )

                    wrote_content = True

    return diffs


# ----------------------------------------------------------------------
def ValidateSizeRequirements(
    dm: DoneManager,
    local_data_store: DataStore,
    destination_data_store: DataStore,
    add_and_modify_diffs: Iterable[DiffResult],
    *,
    header: str="Validating size requirements...",
) -> None:
    bytes_available = destination_data_store.GetBytesAvailable()

    if bytes_available is None:
        return

    bytes_required = 0

    with dm.Nested(
        header,
        [
            lambda: "{} required".format(TextwrapEx.GetSizeDisplay(bytes_required)),
            lambda: "{} available".format(TextwrapEx.GetSizeDisplay(cast(int, bytes_available))),
        ],
    ) as validate_dm:
        for diff in add_and_modify_diffs:
            item_type = local_data_store.GetItemType(diff.path)

            if item_type == ItemType.Dir:
                continue

            if item_type is None:
                validate_dm.WriteInfo("The local file '{}' is no longer available.\n".format(diff.path))
                continue

            assert item_type == ItemType.File, item_type

            bytes_required += local_data_store.GetFileSize(diff.path)

        if (bytes_available * 0.85) <= bytes_required:
            validate_dm.WriteError("There is not enough disk space to process this request.\n")
            return


# ----------------------------------------------------------------------
def WriteFile(
    data_store: DataStore,
    source_filename: Path,
    dest_filename: Path,
    status: Callable[
        [
            int,                            # bytes written
        ],
        None,
    ],
) -> None:
    temp_dest_filename = dest_filename.parent / "{}.__temp__{}".format(
        dest_filename.stem,
        dest_filename.suffix,
    )

    with source_filename.open("rb") as source:
        data_store.MakeDirs(temp_dest_filename.parent)

        with data_store.Open(temp_dest_filename, "wb") as dest:
            bytes_written = 0

            while True:
                chunk = source.read(16384)
                if not chunk:
                    break

                dest.write(chunk)

                bytes_written += len(chunk)
                status(bytes_written)

    data_store.Rename(temp_dest_filename, dest_filename)


# ----------------------------------------------------------------------
def CreateDestinationPathFuncFactory() -> Callable[[Path, str], Path]:  # pragma: no cover
    if CurrentShell.family_name == "Windows":
        # ----------------------------------------------------------------------
        def CreateDestinationPathWindows(
            path: Path,
            extension: str,
        ) -> Path:
            assert ":" in path.parts[0], path.parts

            return (
                Path(path.parts[0].replace(":", "_").rstrip("\\"))
                / Path(*path.parts[1:-1])
                / (path.name + extension)
            )

        # ----------------------------------------------------------------------

        return CreateDestinationPathWindows

    # ----------------------------------------------------------------------
    def CreateDestinationPathNotWindows(
        path: Path,
        extension: str,
    ) -> Path:
        assert path.parts[0] == "/", path.parts

        return (
            Path(*path.parts[1:-1])
            / (path.name + extension)
        )

    # ----------------------------------------------------------------------

    return CreateDestinationPathNotWindows


# ----------------------------------------------------------------------
def CopyLocalContent(
    dm: DoneManager,
    destination_data_store: DataStore,
    diffs: Iterable[DiffResult],
    create_destination_path_func: Callable[[Path, str], Path],
    *,
    ssd: bool,
    quiet: bool,
) -> List[Optional[Path]]:
    # ----------------------------------------------------------------------
    def Add(
        context: DiffResult,
        on_simple_status_func: Callable[[str], None],  # pylint: disable=unused-argument
    ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[Optional[Path]]]:
        diff = context

        dest_filename = create_destination_path_func(diff.path, PENDING_COMMIT_EXTENSION)

        content_size = None

        if diff.path.is_file():
            assert diff.this_file_size is not None
            content_size = diff.this_file_size
        elif diff.path.is_dir():
            content_size = 1
        else:
            assert False, diff.path  # pragma: no cover

        # ----------------------------------------------------------------------
        def Execute(
            status: ExecuteTasks.Status,
        ) -> Tuple[Optional[Path], Optional[str]]:
            if not diff.path.exists():
                return None, None

            if diff.path.is_dir():
                destination_data_store.MakeDirs(dest_filename)
            elif diff.path.is_file():
                WriteFile(
                    destination_data_store,
                    diff.path,
                    dest_filename,
                    lambda bytes_transferred: cast(None, status.OnProgress(bytes_transferred, None)),
                )
            else:
                assert False, diff.path  # pragma: no cover

            return dest_filename, None

        # ----------------------------------------------------------------------

        return content_size, Execute

    # ----------------------------------------------------------------------

    return ExecuteTasks.Transform(
        dm,
        "Processing",
        [
            ExecuteTasks.TaskData(GetTaskDisplayName(diff.path), diff)
            for diff in diffs
        ],
        Add,
        quiet=quiet,
        max_num_threads=None if ssd and destination_data_store.ExecuteInParallel() else 1,
        refresh_per_second=EXECUTE_TASKS_REFRESH_PER_SECOND,
    )


# ----------------------------------------------------------------------
def CalculateHash(
    data_store: DataStore,
    input_item: Path,
    status: Callable[[int], None],
) -> str:
    hasher = hashlib.sha512()

    bytes_hashed = 0

    with data_store.Open(input_item, "rb") as f:
        while True:
            chunk = f.read(16384)
            if not chunk:
                break

            hasher.update(chunk)

            bytes_hashed += len(chunk)
            status(bytes_hashed)

    return hasher.hexdigest()