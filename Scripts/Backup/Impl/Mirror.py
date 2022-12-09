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
import textwrap

from enum import Enum
from pathlib import Path
from typing import Callable, cast, Dict, List, Optional, Pattern, Tuple, Union

from rich.progress import Progress, TimeElapsedColumn

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx
from Common_Foundation.Shell.All import CurrentShell
from Common_Foundation.Streams.Capabilities import Capabilities
from Common_Foundation.Streams.DoneManager import DoneManager
from Common_Foundation import TextwrapEx

from Common_FoundationEx import ExecuteTasks
from Common_FoundationEx.InflectEx import inflect

from .DataStores.DataStore import DataStore, ItemType
from .DataStores.FileBasedDataStore import FileBasedDataStore
from .DataStores.FileSystemDataStore import FileSystemDataStore

from . import Common
from .Snapshot import Snapshot


# ----------------------------------------------------------------------
# |
# |  Public Types
# |
# ----------------------------------------------------------------------
CONTENT_DIR_NAME                            = "Content"



# ----------------------------------------------------------------------
class ValidateType(str, Enum):
    """Controls how validation is performed"""

    standard                                = "standard"                    # File names and sizes are validated
    complete                                = "complete"                    # File names, sizes, and hash values are validated


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

    local_data_store = FileSystemDataStore()

    with Common.YieldDataStore(
        dm,
        destination,
        ssd=ssd,
    ) as destination_data_store:
        if not isinstance(destination_data_store, FileBasedDataStore):
            dm.WriteError("'{}' does not resolve to a file-based data store, which is required when mirroring content.\n".format(destination))
            return

        destination_data_store.ValidateBackupInputs(input_filenames_or_dirs)

        # Load the local snapshot
        with dm.Nested("Creating the local snapshot...") as local_dm:
            local_snapshot = Snapshot.Calculate(
                local_dm,
                input_filenames_or_dirs,
                local_data_store,
                run_in_parallel=ssd,
                filter_filename_func=Common.CreateFilterFunc(file_includes, file_excludes),
                quiet=quiet,
            )

            if local_dm.result != 0:
                return

        # Load the remote snapshot
        if force or not Snapshot.IsPersisted(destination_data_store):
            mirrored_snapshot = Snapshot(
                Snapshot.Node(
                    None,
                    None,
                    Common.DirHashPlaceholder(explicitly_added=False),
                    None,
                ),
            )
        else:
            with dm.Nested("\nReading the destination snapshot..") as destination_dm:
                mirrored_snapshot = Snapshot.LoadPersisted(destination_dm, destination_data_store)

                if destination_dm.result != 0:
                    return

        # Calculate the differences
        diffs: Dict[Common.DiffOperation, List[Common.DiffResult]] = Common.CalculateDiffs(
            dm,
            local_snapshot,
            mirrored_snapshot,
        )

        if not any(diff_items for diff_items in diffs.values()):
            return

        # Calculate the size requirements
        Common.ValidateSizeRequirements(
            dm,
            local_data_store,
            destination_data_store,
            itertools.chain(diffs[Common.DiffOperation.add], diffs[Common.DiffOperation.modify]),
        )

        if dm.result != 0:
            return

        # Cleanup previous content
        _CleanupImpl(dm, destination_data_store)
        if dm.result != 0:
            return

        # Persist all content
        with dm.Nested("\nPersisting content...") as persist_dm:
            # Transfer the snapshot
            pending_snapshot_filename = Path(Snapshot.PERSISTED_FILE_NAME + Common.PENDING_COMMIT_EXTENSION)

            temp_directory = CurrentShell.CreateTempDirectory()

            with ExitStack(lambda: PathEx.RemoveTree(temp_directory)):
                with persist_dm.Nested("Creating snapshot data...") as snapshot_dm:
                    local_snapshot.Persist(
                        snapshot_dm,
                        FileSystemDataStore(temp_directory),
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
                            console=Capabilities.Get(stdout_context.stream).CreateRichConsole(stdout_context.stream),  # type: ignore
                            transient=True,
                        ) as progress_bar:
                            total_progress_id = progress_bar.add_task(
                                "{}Total Progress".format(stdout_context.line_prefix),
                                total=source_filename.stat().st_size,
                                status="",
                                visibile=True,
                            )

                            Common.WriteFile(
                                destination_data_store,
                                source_filename,
                                pending_snapshot_filename,
                                lambda bytes_transferred: progress_bar.update(total_progress_id, completed=bytes_transferred),
                            )

                    if snapshot_dm.result != 0:
                        return

            # Transfer the content
            prev_working_dir = destination_data_store.GetWorkingDir()

            destination_data_store.MakeDirs(Path(CONTENT_DIR_NAME))
            destination_data_store.SetWorkingDir(Path(CONTENT_DIR_NAME))

            with ExitStack(lambda: destination_data_store.SetWorkingDir(prev_working_dir)):
                create_destination_path_func = Common.CreateDestinationPathFuncFactory()

                pending_delete_items: List[Optional[Path]] = []
                pending_commit_items: List[Optional[Path]] = []

                # If force, mark the original content items for deletion
                if force:
                    for root, directories, filenames in destination_data_store.Walk():
                        for item in itertools.chain(directories, filenames):
                            fullpath = root / item

                            delete_filename = fullpath.parent / (fullpath.name + Common.PENDING_DELETE_EXTENSION)

                            destination_data_store.Rename(fullpath, delete_filename)
                            pending_delete_items.append(delete_filename)

                executed_work = False

                persist_dm.WriteLine("")

                # Rename removed & modified files to to-be-deleted
                if diffs[Common.DiffOperation.modify] or diffs[Common.DiffOperation.remove]:
                    with persist_dm.Nested(
                        "Marking content to be removed...",
                        suffix="\n",
                    ) as this_dm:
                        # ----------------------------------------------------------------------
                        def Remove(
                            context: Path,
                            on_simple_status_func: Callable[[str], None], # pylint: disable=unused-argument
                        ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[Optional[Path]]]:
                            source_filename = context

                            dest_filename = create_destination_path_func(
                                source_filename,
                                Common.PENDING_DELETE_EXTENSION,
                            )

                            # ----------------------------------------------------------------------
                            def Execute(
                                status: ExecuteTasks.Status,
                            ) -> Tuple[Optional[Path], Optional[str]]:
                                original_dest_filename = dest_filename.with_suffix("")

                                if not destination_data_store.GetItemType(original_dest_filename):
                                    status.OnInfo("'{}' no longer exists.\n".format(source_filename))
                                    return None, None

                                destination_data_store.Rename(original_dest_filename, dest_filename)
                                return dest_filename, None

                            # ----------------------------------------------------------------------

                            return None, Execute

                        # ----------------------------------------------------------------------

                        pending_delete_items += ExecuteTasks.Transform(
                            this_dm,
                            "Processing",
                            [
                                ExecuteTasks.TaskData(Common.GetTaskDisplayName(diff.path), diff.path)
                                for diff in itertools.chain(
                                    diffs[Common.DiffOperation.modify],
                                    diffs[Common.DiffOperation.remove],
                                )
                            ],
                            Remove,
                            quiet=quiet,
                            max_num_threads=None if destination_data_store.ExecuteInParallel() else 1,
                            refresh_per_second=Common.EXECUTE_TASKS_REFRESH_PER_SECOND,
                        )

                        if this_dm.result != 0:
                            return

                        executed_work = True

                # Move added & modified files to temp files in dest dir
                if diffs[Common.DiffOperation.add] or diffs[Common.DiffOperation.modify]:
                    with persist_dm.Nested(
                        "Transferring added and modified content...",
                        suffix="\n",
                    ) as this_dm:
                        pending_commit_items += Common.CopyLocalContent(
                            this_dm,
                            destination_data_store,
                            itertools.chain(
                                diffs[Common.DiffOperation.add],
                                diffs[Common.DiffOperation.modify],
                            ),
                            create_destination_path_func,
                            quiet=quiet,
                            ssd=ssd,
                        )

                        if this_dm.result != 0:
                            return

                        executed_work = True

                if executed_work:
                    for desc, items, func in [
                        (
                            "Committing added content...",
                            pending_commit_items,
                            lambda fullpath: destination_data_store.Rename(fullpath, fullpath.with_suffix("")),
                        ),
                        (
                            "Committing removed content...",
                            pending_delete_items,
                            destination_data_store.RemoveItem,
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
                                        if destination_data_store.GetItemType(fullpath):
                                            func(fullpath)

                                        return None, None

                                    # ----------------------------------------------------------------------

                                    return None, Execute

                                # ----------------------------------------------------------------------

                                ExecuteTasks.Transform(
                                    this_dm,
                                    "Processing",
                                    [
                                        ExecuteTasks.TaskData(Common.GetTaskDisplayName(fullpath), fullpath)
                                        for fullpath in items if fullpath
                                    ],
                                    CommitImpl,
                                    quiet=quiet,
                                    max_num_threads=None if destination_data_store.ExecuteInParallel() else 1,
                                    refresh_per_second=Common.EXECUTE_TASKS_REFRESH_PER_SECOND,
                                )

                                if this_dm.result != 0:
                                    return

            # Commit the snapshot data
            with persist_dm.Nested("Committing snapshot data...") as commit_dm:
                destination_data_store.Rename(
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
    with Common.YieldDataStore(
        dm,
        destination,
        ssd=False,
    ) as destination_data_store:
        if not isinstance(destination_data_store, FileBasedDataStore):
            dm.WriteError("'{}' does not resolve to a file-based data store, which is required when mirroring content.\n".format(destination))
            return

        return _CleanupImpl(dm, destination_data_store)


# ----------------------------------------------------------------------
def Validate(
    dm: DoneManager,
    destination: Union[str, Path],
    validate_type: ValidateType,
    *,
    ssd: bool,
    quiet: bool,
) -> None:
    with Common.YieldDataStore(
        dm,
        destination,
        ssd=ssd,
    ) as destination_data_store:
        if not isinstance(destination_data_store, FileBasedDataStore):
            dm.WriteError("'{}' does not resolve to a file-based data store, which is required when mirroring content.\n".format(destination))
            return

        if not Snapshot.IsPersisted(destination_data_store):
            dm.WriteError("No snapshot was found.\n")
            return

        mirrored_snapshot = Snapshot.LoadPersisted(dm, destination_data_store)

        _CleanupImpl(dm, destination_data_store)

        current_working_dir = destination_data_store.GetWorkingDir()

        with ExitStack(lambda: destination_data_store.SetWorkingDir(current_working_dir)):
            content_dir = destination_data_store.GetWorkingDir() / CONTENT_DIR_NAME
            destination_data_store.SetWorkingDir(content_dir)

            with dm.Nested(
                "\nExtracting files...",
                suffix="\n",
            ) as extract_dm:
                current_snapshot = Snapshot.Calculate(
                    extract_dm,
                    [Path()],
                    destination_data_store,
                    run_in_parallel=destination_data_store.ExecuteInParallel(),
                    quiet=quiet,
                    calculate_hashes=validate_type == ValidateType.complete,
                )

            # The values in the mirrored snapshot are based on the original values provided during the backup
            # while the values of the current snapshot are based on what is on the filesystem. Convert
            # the data in the mirror snapshot so it matches the values in the current snapshot before
            # we do the comparison.
            new_root = Snapshot.Node(None, None, Common.DirHashPlaceholder(explicitly_added=False), None)

            for node in mirrored_snapshot.node.Enum():
                destination_path = destination_data_store.SnapshotFilenameToDestinationName(node.fullpath)

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
                if diff.operation == Common.DiffOperation.add:
                    validate_dm.WriteError("'{}' has been added.\n".format(diff.path))
                elif diff.operation == Common.DiffOperation.remove:
                    validate_dm.WriteError("'{}' has been removed.\n".format(diff.path))
                elif diff.operation == Common.DiffOperation.modify:
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
def _CleanupImpl(
    dm: DoneManager,
    data_store: FileBasedDataStore,
) -> None:
    items_reverted = 0

    with dm.Nested(
        "Reverting partially committed content at the destination...",
        lambda: "{} reverted".format(inflect.no("item", items_reverted)),
    ) as clean_dm:
        item_type = data_store.GetItemType(Path(CONTENT_DIR_NAME))

        if item_type is None:
            clean_dm.WriteInfo("Content does not exist.\n")
            return

        if item_type == ItemType.File:
            with clean_dm.Nested("Removing the file '{}'...".format(CONTENT_DIR_NAME)):
                data_store.RemoveFile(Path(CONTENT_DIR_NAME))
                return

        if item_type != ItemType.Dir:
            raise Exception("'{}' is not a valid directory.".format(CONTENT_DIR_NAME))

        for root, directories, filenames in data_store.Walk():
            if clean_dm.capabilities.is_interactive:
                clean_dm.WriteStatus("Processing '{}'...".format(root))  # pragma: no cover

            for item in itertools.chain(directories, filenames):
                fullpath = root / item

                if fullpath.suffix == Common.PENDING_COMMIT_EXTENSION:
                    with clean_dm.Nested("Removing '{}'...".format(fullpath)):
                        data_store.RemoveItem(fullpath)
                        items_reverted += 1

                elif fullpath.suffix == Common.PENDING_DELETE_EXTENSION:
                    original_filename = fullpath.with_suffix("")

                    with clean_dm.Nested("Restoring '{}'...".format(original_filename)):
                        data_store.Rename(fullpath, original_filename)

                        items_reverted += 1
