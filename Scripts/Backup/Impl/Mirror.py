# ----------------------------------------------------------------------
# |
# |  Mirror.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-13 08:09:07
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Mirror functionality"""

import itertools
import re
import textwrap

from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Callable, cast, Dict, List, Iterator, Optional, Pattern, Tuple, Union
from urllib import parse as urlparse

from rich.progress import Progress, TimeElapsedColumn

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx
from Common_Foundation.Shell.All import CurrentShell
from Common_Foundation.Streams.Capabilities import Capabilities as StreamCapabilities
from Common_Foundation.Streams.DoneManager import DoneManager
from Common_Foundation import TextwrapEx

from Common_FoundationEx import ExecuteTasks
from Common_FoundationEx.InflectEx import inflect

from .Capabilities.Capabilities import Capabilities, ItemType
from .Capabilities.FileSystemCapabilities import FileSystemCapabilities
from .Capabilities.SFTPCapabilities import SFTPCapabilities, SSH_PORT

from .Snapshot import Snapshot


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
CONTENT_DIR_NAME                            = "Content"

PENDING_COMMIT_EXTENSION                    = ".__pending_commit__"
PENDING_DELETE_EXTENSION                    = ".__pending_delete__"


# ----------------------------------------------------------------------
class ValidateType(str, Enum):
    """Controls how validation is performed"""

    standard                                = "standard"                    # File names and sizes are validated
    complete                                = "complete"                    # File names, sizes, and hash values are validated


# ----------------------------------------------------------------------
def GetDestinationHelp() -> str:
    return textwrap.dedent(
        """\
        Destinations
        ============
        The value provided on the command line for 'destination' can be any of the following values...

        File System
        -----------
        Mirrors content to the local file system.

            Examples:
                - /home/mirrored_content
                - C:\\MirroredContent

        SFTP
        ----
        Mirrors content to a SFTP server.

            Format:
                ftp://<username>:<password or posix path to private key>@<host>[:<port>][/<working_dir>]

            Examples:
                ftp://my_username:my_password@my_server.com
                ftp://my_username:my_password@my_server.com/This/Working/Dir
                ftp://my_username:/path/to/private/key@my_server.com
        """,
    ).replace("\n", "\n\n")


# ----------------------------------------------------------------------
def Backup(
    dm: DoneManager,
    destination: Union[str, Path],
    input_filenames_or_dirs: List[Path],
    *,
    ssd: bool,
    force: bool,
    quiet: bool,
    file_includes: Optional[List[Pattern]],
    file_excludes: Optional[List[Pattern]],
) -> None:
    # Process the inputs
    for input_file_or_dir in input_filenames_or_dirs:
        if not input_file_or_dir.exists():
            raise Exception("'{}' is not a valid filename or directory.".format(input_file_or_dir))

    with _YieldCapabilities(
        dm,
        destination,
        ssd=ssd,
    ) as capabilities:
        capabilities.ValidateMirrorInputs(input_filenames_or_dirs)

        if file_includes or file_excludes:
            # ----------------------------------------------------------------------
            def SnapshotFilter(
                filename: Path,
            ) -> bool:
                filename_str = filename.as_posix()

                if file_excludes is not None and any(exclude.match(filename_str) for exclude in file_excludes):
                    return False

                if file_includes is not None and not any(include.match(filename_str) for include in file_includes):
                    return False

                return True

            # ----------------------------------------------------------------------

            filter_filename_func = SnapshotFilter
        else:
            filter_filename_func = None

        # Load the local snapshot
        with dm.Nested("Creating the local snapshot...") as local_dm:
            local_snapshot = Snapshot.Calculate(
                local_dm,
                input_filenames_or_dirs,
                FileSystemCapabilities(),
                run_in_parallel=ssd,
                filter_filename_func=filter_filename_func,
                quiet=quiet,
            )

            if local_dm.result != 0:
                return

        # Load the remote snapshot
        if force or not Snapshot.IsPersisted(capabilities):
            mirrored_snapshot = Snapshot(
                Snapshot.Node(
                    None,
                    None,
                    Snapshot.DirHashPlaceholder(explicitly_added=False),
                    None,
                ),
            )
        else:
            with dm.Nested("\nReading the destination snapshot..") as destination_dm:
                mirrored_snapshot = Snapshot.LoadPersisted(destination_dm, capabilities)

                if destination_dm.result != 0:
                    return

        # Calculate the differences
        diffs: Dict[Snapshot.DiffOperation, List[Snapshot.DiffResult]] = {
            Snapshot.DiffOperation.add: [],
            Snapshot.DiffOperation.modify: [],
            Snapshot.DiffOperation.remove: [],
        }

        with dm.Nested(
            "\nCalculating diffs...",
            lambda: "{} found".format(inflect.no("diff", sum(len(diff_items) for diff_items in diffs.values()))),
            suffix="\n",
        ):
            for diff in local_snapshot.Diff(mirrored_snapshot):
                assert diff.operation in diffs, diff.operation
                diffs[diff.operation].append(diff)

        if not any(diff_items for diff_items in diffs.values()):
            return

        # Calculate the size requirements
        bytes_available = capabilities.GetBytesAvailable()

        if bytes_available is not None:
            bytes_required = 0

            with dm.Nested(
                "Validating size requirements...",
                [
                    lambda: "{} required".format(TextwrapEx.GetSizeDisplay(bytes_required)),
                    lambda: "{} available".format(TextwrapEx.GetSizeDisplay(cast(int, bytes_available))),
                ],
            ) as validate_dm:
                for diff in itertools.chain(diffs[Snapshot.DiffOperation.add], diffs[Snapshot.DiffOperation.modify]):
                    item_type = capabilities.GetItemType(diff.path)

                    if item_type == ItemType.Dir:
                        continue

                    if item_type is None:
                        validate_dm.WriteInfo("The local file '{}' is no longer available.\n".format(diff.path))
                        continue

                    assert item_type == ItemType.File, item_type

                    bytes_required += capabilities.GetFileSize(diff.path)

                if (bytes_available * 0.85) <= bytes_required:
                    validate_dm.WriteError("There is not enough disk space to process this request.\n")
                    return

        # Cleanup previous content
        _CleanupImpl(dm, capabilities)
        if dm.result != 0:
            return

        # Persist all content
        with dm.Nested("\nPersisting content...") as persist_dm:
            # ----------------------------------------------------------------------
            def WriteFile(
                source_filename: Path,
                dest_filename: Path,
                status: Callable[
                    [
                        int,                # bytes written
                    ],
                    None,
                ],
            ) -> None:
                capabilities.MakeDirs(dest_filename.parent)

                temp_dest_filename = dest_filename.parent / "{}.__temp__{}".format(
                    dest_filename.stem,
                    dest_filename.suffix,
                )

                was_successful = False

                with source_filename.open("rb") as source:
                    capabilities.MakeDirs(temp_dest_filename.parent)

                    with capabilities.Open(temp_dest_filename, "wb") as dest:
                        bytes_written = 0

                        while True:
                            chunk = source.read(16384)
                            if not chunk:
                                was_successful = True
                                break

                            dest.write(chunk)
                            bytes_written += len(chunk)

                            status(bytes_written)

                if was_successful:
                    capabilities.Rename(temp_dest_filename, dest_filename)

            # ----------------------------------------------------------------------

            # Transfer the snapshot
            pending_snapshot_filename = Path(Snapshot.PERSISTED_FILE_NAME + PENDING_COMMIT_EXTENSION)

            temp_directory = CurrentShell.CreateTempDirectory()

            with ExitStack(lambda: PathEx.RemoveTree(temp_directory)):
                with persist_dm.Nested("Creating snapshot data...") as snapshot_dm:
                    local_snapshot.Persist(
                        snapshot_dm,
                        FileSystemCapabilities(temp_directory),
                    )

                    if snapshot_dm.result != 0:
                        return

                with persist_dm.Nested("Transferring snapshot data...") as snapshot_dm:
                    source_filename = temp_directory / Snapshot.PERSISTED_FILE_NAME

                    with snapshot_dm.YieldStdout() as stdout_context:
                        stdout_context.persist_content = False

                        with Progress(
                            *Progress.get_default_columns(),
                            TimeElapsedColumn(),
                            "{task.fields[status]}",
                            console=StreamCapabilities.Get(stdout_context.stream).CreateRichConsole(stdout_context.stream),  # type: ignore
                            transient=True,
                        ) as progress_bar:
                            total_progress_id = progress_bar.add_task(
                                "{}Total Progress".format(stdout_context.line_prefix),
                                total=source_filename.stat().st_size,
                                status="",
                                visibile=True,
                            )

                            WriteFile(
                                source_filename,
                                pending_snapshot_filename,
                                lambda bytes_transferred: progress_bar.update(total_progress_id, completed=bytes_transferred),
                            )

                    if snapshot_dm.result != 0:
                        return

            # Transfer the content
            prev_working_dir = capabilities.GetWorkingDir()

            capabilities.MakeDirs(Path(CONTENT_DIR_NAME))
            capabilities.SetWorkingDir(Path(CONTENT_DIR_NAME))

            with ExitStack(lambda: capabilities.SetWorkingDir(prev_working_dir)):
                if CurrentShell.family_name == "Windows":
                    # ----------------------------------------------------------------------
                    def CreateDestinationPathWindows(
                        path: Path,
                        extension: str,
                    ) -> Path:  # pragma: no cover
                        assert ":" in path.parts[0], path.parts

                        return (
                            Path(path.parts[0].replace(":", "_").rstrip("\\"))
                            / Path(*path.parts[1:-1])
                            / (path.name + extension)
                        )

                    # ----------------------------------------------------------------------

                    create_destination_path_func = CreateDestinationPathWindows  # pragma: no cover
                else:
                    # ----------------------------------------------------------------------
                    def CreateDestinationPathNotWindows(
                        path: Path,
                        extension: str,
                    ) -> Path:  # pragma: no cover
                        assert path.parts[0] == "/", path.parts

                        return (
                            Path(*path.parts[1:-1])
                            / (path.name + extension)
                        )

                    # ----------------------------------------------------------------------

                    create_destination_path_func = CreateDestinationPathNotWindows  # pragma: no cover

                pending_delete_items: List[Optional[Path]] = []
                pending_commit_items: List[Optional[Path]] = []

                # If force, mark the original content items for deletion
                if force:
                    for root, directories, filenames in capabilities.Walk():
                        for item in itertools.chain(directories, filenames):
                            fullpath = root / item

                            delete_filename = fullpath.parent / (fullpath.name + PENDING_DELETE_EXTENSION)

                            capabilities.Rename(fullpath, delete_filename)
                            pending_delete_items.append(delete_filename)

                executed_work = False

                persist_dm.WriteLine("")

                # Rename removed & modified files to to-be-deleted
                if diffs[Snapshot.DiffOperation.modify] or diffs[Snapshot.DiffOperation.remove]:
                    with persist_dm.Nested(
                        "Marking content to be removed...",
                        suffix="\n",
                    ) as this_dm:
                        with this_dm.YieldVerboseStream() as stream:
                            if diffs[Snapshot.DiffOperation.modify]:
                                stream.write("Modifying\n")
                                stream.write("".join("  - {}\n".format(diff.path) for diff in diffs[Snapshot.DiffOperation.modify]))
                                stream.write("\n")

                            if diffs[Snapshot.DiffOperation.remove]:
                                stream.write("Removing\n")
                                stream.write("".join("  - {}\n".format(diff.path) for diff in diffs[Snapshot.DiffOperation.remove]))
                                stream.write("\n")

                        # ----------------------------------------------------------------------
                        def Remove(
                            context: Path,
                            on_simple_status_func: Callable[[str], None], # pylint: disable=unused-argument
                        ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[Optional[Path]]]:
                            source_filename = context

                            dest_filename = create_destination_path_func(
                                source_filename,
                                PENDING_DELETE_EXTENSION,
                            )

                            # ----------------------------------------------------------------------
                            def Execute(
                                status: ExecuteTasks.Status,
                            ) -> Tuple[Optional[Path], Optional[str]]:
                                original_dest_filename = dest_filename.with_suffix("")

                                if not capabilities.GetItemType(original_dest_filename):
                                    status.OnInfo("'{}' no longer exists.\n".format(source_filename))
                                    return None, None

                                capabilities.Rename(original_dest_filename, dest_filename)
                                return dest_filename, None

                            # ----------------------------------------------------------------------

                            return None, Execute

                        # ----------------------------------------------------------------------

                        pending_delete_items += ExecuteTasks.Transform(
                            this_dm,
                            "Processing",
                            [
                                ExecuteTasks.TaskData(Snapshot.GetTaskDisplayName(diff.path), diff.path)
                                for diff in itertools.chain(
                                    diffs[Snapshot.DiffOperation.modify],
                                    diffs[Snapshot.DiffOperation.remove],
                                )
                            ],
                            Remove,
                            quiet=quiet,
                            max_num_threads=None if capabilities.ExecuteInParallel() else 1,
                        )

                        if this_dm.result != 0:
                            return

                        executed_work = True

                # Move added & modified files to temp files in dest dir
                if diffs[Snapshot.DiffOperation.add] or diffs[Snapshot.DiffOperation.modify]:
                    with persist_dm.Nested(
                        "Transferring added and modified content...",
                        suffix="\n",
                    ) as this_dm:
                        with this_dm.YieldVerboseStream() as stream:
                            if diffs[Snapshot.DiffOperation.add]:
                                stream.write("Adding\n")
                                stream.write("".join("  - {}\n".format(diff.path) for diff in diffs[Snapshot.DiffOperation.add]))
                                stream.write("\n")

                            if diffs[Snapshot.DiffOperation.modify]:
                                stream.write("Modifying\n")
                                stream.write("".join("  - {}\n".format(diff.path) for diff in diffs[Snapshot.DiffOperation.modify]))
                                stream.write("\n")

                        # ----------------------------------------------------------------------
                        def Add(
                            context: Path,
                            on_simple_status_func: Callable[[str], None],  # pylint: disable=unused-argument
                        ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[Optional[Path]]]:

                            source_filename = context

                            dest_filename = create_destination_path_func(source_filename, PENDING_COMMIT_EXTENSION)

                            content_size = None

                            if source_filename.is_file():
                                content_size = source_filename.stat().st_size
                            elif source_filename.is_dir():
                                content_size = 1

                            # ----------------------------------------------------------------------
                            def Execute(
                                status: ExecuteTasks.Status,
                            ) -> Tuple[Optional[Path], Optional[str]]:
                                if not source_filename.exists():
                                    return None, None

                                if source_filename.is_dir():
                                    capabilities.MakeDirs(dest_filename)
                                elif source_filename.is_file():
                                    assert source_filename.is_file(), source_filename
                                    capabilities.MakeDirs(dest_filename.parent)

                                    WriteFile(
                                        source_filename,
                                        dest_filename,
                                        lambda transferred: cast(None, status.OnProgress(transferred, None)),
                                    )
                                else:
                                    assert False, source_filename  # pragma: no cover

                                return dest_filename, None

                            # ----------------------------------------------------------------------

                            return content_size, Execute

                        # ----------------------------------------------------------------------

                        pending_commit_items += ExecuteTasks.Transform(
                            this_dm,
                            "Processing",
                            [
                                ExecuteTasks.TaskData(Snapshot.GetTaskDisplayName(diff.path), diff.path)
                                for diff in itertools.chain(
                                    diffs[Snapshot.DiffOperation.add],
                                    diffs[Snapshot.DiffOperation.modify],
                                )
                            ],
                            Add,
                            quiet=quiet,
                            max_num_threads=None if capabilities.ExecuteInParallel() else 1,
                        )

                        if this_dm.result != 0:
                            return

                        executed_work = True

                if executed_work:
                    for desc, items, func in [
                        (
                            "Committing added content...",
                            pending_commit_items,
                            lambda fullpath: capabilities.Rename(fullpath, fullpath.with_suffix("")),
                        ),
                        (
                            "Committing removed content...",
                            pending_delete_items,
                            capabilities.RemoveItem,
                        ),
                    ]:
                        if any(item for item in items):
                            with persist_dm.Nested(
                                desc,
                                suffix="\n",
                            ) as this_dm:
                                # ----------------------------------------------------------------------
                                def CommitImpl(
                                    context: Path,
                                    on_simple_status_func: Callable[[str], None],  # pylint: disable=unused-argument
                                ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[None]]:
                                    fullpath = context

                                    # ----------------------------------------------------------------------
                                    def Execute(
                                        status: ExecuteTasks.Status,  # pylint: disable=unused-argument
                                    ) -> Tuple[None, Optional[str]]:
                                        if capabilities.GetItemType(fullpath):
                                            func(fullpath)

                                        return None, None

                                    # ----------------------------------------------------------------------

                                    return None, Execute

                                # ----------------------------------------------------------------------

                                ExecuteTasks.Transform(
                                    this_dm,
                                    "Processing",
                                    [
                                        ExecuteTasks.TaskData(Snapshot.GetTaskDisplayName(fullpath), fullpath)
                                        for fullpath in items
                                        if fullpath
                                    ],
                                    CommitImpl,
                                    quiet=quiet,
                                    max_num_threads=None if capabilities.ExecuteInParallel() else 1,
                                )

                                if this_dm.result != 0:
                                    return

            # Commit the snapshot data
            with persist_dm.Nested("Committing snapshot data...") as commit_dm:
                capabilities.Rename(
                    pending_snapshot_filename,
                    pending_snapshot_filename.with_suffix(""),
                )

                if commit_dm.result != 0:
                    return


# ----------------------------------------------------------------------
def Cleanup(
    dm: DoneManager,
    destination: Union[str, Path],
) -> None:
    with _YieldCapabilities(
        dm,
        destination,
        ssd=False,
    ) as capabilities:
        return _CleanupImpl(dm, capabilities)


# ----------------------------------------------------------------------
def Validate(
    dm: DoneManager,
    destination: Union[str, Path],
    validate_type: ValidateType,
    *,
    ssd: bool,
    quiet: bool,
) -> None:
    with _YieldCapabilities(
        dm,
        destination,
        ssd=ssd,
    ) as capabilities:
        if not Snapshot.IsPersisted(capabilities):
            dm.WriteError("No snapshot was found.\n")
            return

        mirrored_snapshot = Snapshot.LoadPersisted(dm, capabilities)

        _CleanupImpl(dm, capabilities)

        current_working_dir = capabilities.GetWorkingDir()

        with ExitStack(lambda: capabilities.SetWorkingDir(current_working_dir)):
            content_dir = capabilities.GetWorkingDir() / CONTENT_DIR_NAME
            capabilities.SetWorkingDir(content_dir)

            with dm.Nested(
                "\nExtracting files...",
                suffix="\n",
            ) as extract_dm:
                current_snapshot = Snapshot.Calculate(
                    extract_dm,
                    [Path()],
                    capabilities,
                    run_in_parallel=capabilities.ExecuteInParallel(),
                    quiet=quiet,
                    calculate_hashes=validate_type == ValidateType.complete,
                )

            # The values in the mirrored snapshot are based on the original values provided during the backup
            # while the values of the current snapshot are based on what is on the filesystem. Convert
            # the data in the mirror snapshot so it matches the values in the current snapshot before
            # we do the comparison.
            new_root = Snapshot.Node(None, None, Snapshot.DirHashPlaceholder(explicitly_added=False), None)

            for node in mirrored_snapshot.node.Enum():
                destination_path = capabilities.SnapshotFilenameToDestinationName(node.fullpath)

                if node.is_dir:
                    if not node.children:
                        new_root.AddDir(destination_path, force=True)
                elif node.is_file:
                    new_root.AddFile(destination_path, cast(str, node.hash_value), cast(int, node.file_size))
                else:
                    assert False, node  # pragma: no cover

        with dm.Nested(
            "Validating content...",
            suffix="\n" if dm.is_verbose else "",
        ) as validate_dm:
            # Windows and Linux have different sorting orders, so capture and sort the list before
            # displaying the contents.
            diffs = list(
                current_snapshot.Diff(
                    Snapshot(new_root),
                    compare_hashes=validate_type == ValidateType.complete,
                ),
            )

            if not diffs:
                validate_dm.WriteInfo("No differences were found.\n")
                return

            diffs.sort(key=lambda diff: diff.path)

            for diff in diffs:
                if diff.operation == Snapshot.DiffOperation.add:
                    validate_dm.WriteError("'{}' has been added.\n".format(diff.path))
                elif diff.operation == Snapshot.DiffOperation.remove:
                    validate_dm.WriteError("'{}' has been removed.\n".format(diff.path))
                elif diff.operation == Snapshot.DiffOperation.modify:
                    assert diff.this_file_size is not None
                    assert diff.other_file_size is not None

                    validate_dm.WriteWarning(
                        textwrap.dedent(
                            """\
                            '{}' has been modified.

                                Expected file size:     {}
                                Actual file size:       {}
                            {}
                            """,
                        ).format(
                            diff.path,
                            diff.other_file_size,
                            diff.this_file_size,
                            "" if diff.this_hash == "ignored" else TextwrapEx.Indent(
                                textwrap.dedent(
                                    """\
                                    Expected hash value:    {}
                                    Actual hash value:      {}
                                    """,
                                ).format(
                                    diff.other_hash,
                                    diff.this_hash,
                                ),
                                4,
                            ),
                        ),
                    )
                else:
                    assert False, diff.operation  # pragma: no cover


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
@contextmanager
def _YieldCapabilities(
    dm: DoneManager,
    destination: Union[str, Path],
    *,
    ssd: bool,
) -> Iterator[Capabilities]:
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

            with SFTPCapabilities.Create(
                dm,
                sftp_match.group("host"),
                sftp_match.group("username"),
                private_key_or_password,
                working_dir,
                port=int(sftp_match.group("port") or SSH_PORT),
            ) as capabilities:
                yield capabilities
                return

    yield FileSystemCapabilities(
        Path(destination),
        ssd=ssd,
    )


# ----------------------------------------------------------------------
def _CleanupImpl(
    dm: DoneManager,
    capabilities: Capabilities,
) -> None:
    items_reverted = 0

    with dm.Nested(
        "Reverting partially committed content at the destination...",
        lambda: "{} reverted".format(inflect.no("item", items_reverted)),
    ) as clean_dm:
        item_type = capabilities.GetItemType(Path(CONTENT_DIR_NAME))

        if item_type is None:
            clean_dm.WriteInfo("Content does not exist.\n")
            return

        if item_type == ItemType.File:
            with clean_dm.Nested("Removing the file '{}'...".format(CONTENT_DIR_NAME)):
                capabilities.RemoveFile(Path(CONTENT_DIR_NAME))
                return

        if item_type != ItemType.Dir:
            raise Exception("'{}' is not a valid directory.".format(CONTENT_DIR_NAME))

        for root, directories, filenames in capabilities.Walk():
            if clean_dm.capabilities.is_interactive:
                clean_dm.WriteStatus("Processing '{}'...".format(root))

            for item in itertools.chain(directories, filenames):
                fullpath = root / item

                if fullpath.suffix == PENDING_COMMIT_EXTENSION:
                    with clean_dm.Nested("Removing '{}'...".format(fullpath)):
                        capabilities.RemoveItem(fullpath)
                        items_reverted += 1

                elif fullpath.suffix == PENDING_DELETE_EXTENSION:
                    original_filename = fullpath.with_suffix("")

                    with clean_dm.Nested("Restoring '{}'...".format(original_filename)):
                        capabilities.Rename(fullpath, original_filename)

                        items_reverted += 1
