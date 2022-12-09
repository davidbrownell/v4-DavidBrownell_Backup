# ----------------------------------------------------------------------
# |
# |  Offsite.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-28 14:12:48
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Offsite functionality"""

import datetime
import itertools
import json
import os
import re
import shutil
import sys
import textwrap
import threading
import uuid

from contextlib import contextmanager
from dataclasses import dataclass
from enum import auto, Enum
from pathlib import Path
from typing import Any, Callable, cast, Dict, Iterator, List, Optional, Pattern, Set, Tuple, Union

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx
from Common_Foundation.Shell.All import CurrentShell
from Common_Foundation.Streams.DoneManager import DoneManager
from Common_Foundation import SubprocessEx
from Common_Foundation import TextwrapEx

from Common_FoundationEx import ExecuteTasks
from Common_FoundationEx.InflectEx import inflect

from .DataStores.BulkStorageDataStore import BulkStorageDataStore
from .DataStores.FileBasedDataStore import FileBasedDataStore, ItemType
from .DataStores.FileSystemDataStore import FileSystemDataStore

from . import Common
from .Snapshot import Snapshot


# ----------------------------------------------------------------------
# |
# |  Public Types
# |
# ----------------------------------------------------------------------
DEFAULT_ARCHIVE_VOLUME_SIZE                 = 250 * 1024 * 1024  # 250MB

INDEX_FILENAME                              = "index.json"
INDEX_HASH_FILENAME                         = "{}.hash".format(INDEX_FILENAME)

ARCHIVE_FILENAME                            = "data.7z"
DELTA_SUFFIX                                = ".delta"


# ----------------------------------------------------------------------
@dataclass(frozen=True)
class SnapshotFilenames(object):
    """Filenames used to store snapshot information"""

    backup_name: str
    standard: Path
    pending: Path

    # ----------------------------------------------------------------------
    @classmethod
    def Create(
        cls,
        backup_name: str,
    ) -> "SnapshotFilenames":
        snapshot_filename = CurrentShell.user_directory / "OffsiteBackup.{}.json".format(backup_name)

        return cls(
            backup_name,
            snapshot_filename,
            snapshot_filename.parent / "{}.__pending__{}".format(
                snapshot_filename.stem,
                snapshot_filename.suffix,
            ),
        )


# ----------------------------------------------------------------------
# |
# |  Public Functions
# |
# ----------------------------------------------------------------------
def Backup(
    dm: DoneManager,
    input_filenames_or_dirs: List[Path],
    backup_name: str,
    destination: Optional[str],
    encryption_password: Optional[str],
    working_dir: Path,
    *,
    compress: bool,
    ssd: bool,
    force: bool,
    quiet: bool,
    file_includes: Optional[List[Pattern]],
    file_excludes: Optional[List[Pattern]],
    archive_volume_size: int=DEFAULT_ARCHIVE_VOLUME_SIZE,
    ignore_pending_snapshot: bool=False,
    commit_pending_snapshot: bool=True,
) -> None:
    # Process the inputs
    for input_file_or_dir in input_filenames_or_dirs:
        if not input_file_or_dir.exists():
            raise Exception("'{}' is not a valid filename or directory.".format(input_file_or_dir))

    if compress or encryption_password:
        zip_binary = _GetZipBinary()
    else:
        zip_binary = None

    snapshot_filenames = SnapshotFilenames.Create(backup_name)

    if snapshot_filenames.pending.is_file():
        if not ignore_pending_snapshot:
            dm.WriteError(
                textwrap.dedent(
                    """\

                    A pending snapshot exists for the backup '{}'; this snapshot should be committed before creating updates
                    to the backup.

                    To commit the pending snapshot, run this script with the 'commit' command.

                    To ignore this error and delete the pending snapshot, run this script with the '--ignore-pending-snapshot'
                    argument.


                    """,
                ).format(snapshot_filenames.backup_name),
            )

            return

        PathEx.RemoveFile(snapshot_filenames.pending)

    elif ignore_pending_snapshot:
        dm.WriteError(
            "A pending snapshot for '{}' was not found.\n".format(snapshot_filenames.backup_name),
        )
        return

    # Load the local snapshot
    with dm.Nested("Creating the local snapshot...") as local_dm:
        local_snapshot = Snapshot.Calculate(
            local_dm,
            input_filenames_or_dirs,
            FileSystemDataStore(),
            run_in_parallel=ssd,
            filter_filename_func=Common.CreateFilterFunc(file_includes, file_excludes),
            quiet=quiet,
        )

        if local_dm.result != 0:
            return

    if force or not snapshot_filenames.standard.is_file():
        force = True

        offsite_snapshot = Snapshot(
            Snapshot.Node(
                None,
                None,
                Common.DirHashPlaceholder(explicitly_added=False),
                None,
            ),
        )
    else:
        with dm.Nested("\nReading the most recent offsite snapshot...") as destination_dm:
            offsite_snapshot = Snapshot.LoadPersisted(
                destination_dm,
                FileSystemDataStore(),
                snapshot_filename=snapshot_filenames.standard,
            )

            if destination_dm.result != 0:
                return

    # Calculate the differences
    diffs: Dict[Common.DiffOperation, List[Common.DiffResult]] = Common.CalculateDiffs(
        dm,
        local_snapshot,
        offsite_snapshot,
    )

    if not any(diff_items for diff_items in diffs.values()):
        return

    # Capture all of the changes in a temp directory
    now = datetime.datetime.now()

    file_content_root = working_dir / "{year:04d}.{month:02d}.{day:02d}.{hour:02d}.{minute:02d}.{second:02d}-{microsecond:06d}{suffix}".format(
        year=now.year,
        month=now.month,
        day=now.day,
        hour=now.hour,
        minute=now.minute,
        second=now.second,
        microsecond=now.microsecond,
        suffix=DELTA_SUFFIX if not force else "",
    )

    file_content_root.mkdir(parents=True)
    file_content_data_store = FileSystemDataStore(file_content_root)

    # ----------------------------------------------------------------------
    def OnExit():
        if destination is None:
            template = textwrap.dedent(
                """\


                Content has been written to '{{}}',
                however the changes have not been committed yet.

                After the generated content is transferred to an offsite location, run this script
                again with the 'commit' command using the backup name '{}' to ensure that
                these changes are not processed when this offsite backup is run again.


                """,
            ).format(backup_name)

        elif dm.result == 0:
            PathEx.RemoveTree(file_content_root)
            return

        else:
            if dm.result < 0:
                type_desc = "errors"
            elif dm.result > 0:
                type_desc = "warnings"
            else:
                assert False, dm.result  # pragma: no cover

            template = "The temporary directory '{{}}' was preserved due to {}.".format(type_desc)

        dm.WriteInfo(
            "\n" + template.format(
                file_content_root if dm.capabilities.is_headless else TextwrapEx.CreateAnsiHyperLink(
                    "file:///{}".format(working_dir.as_posix()),
                    str(working_dir),
                ),
            ),
        )

    # ----------------------------------------------------------------------

    with ExitStack(OnExit):
        with dm.Nested(
            "Preparing file content...",
            suffix="\n",
        ) as prepare_dm:
            if diffs[Common.DiffOperation.add] or diffs[Common.DiffOperation.modify]:
                # Create a lookup of all existing files at the offsite
                offsite_file_lookup: Set[str] = set()

                for node in offsite_snapshot.node.Enum():
                    if not node.is_file:
                        continue

                    assert isinstance(node.hash_value, str), node.hash_value
                    offsite_file_lookup.add(node.hash_value)

                # Gather all the diffs associated with files that need to be transferred
                diffs_to_process: List[Common.DiffResult] = []

                for diff in itertools.chain(
                    diffs[Common.DiffOperation.add],
                    diffs[Common.DiffOperation.modify],
                ):
                    if not diff.path.is_file():
                        continue

                    assert isinstance(diff.this_hash, str), diff.this_hash
                    if diff.this_hash in offsite_file_lookup:
                        continue

                    diffs_to_process.append(diff)
                    offsite_file_lookup.add(diff.this_hash)

                if diffs_to_process:
                    # Calculate the size requirements
                    Common.ValidateSizeRequirements(
                        prepare_dm,
                        file_content_data_store,
                        file_content_data_store,
                        diffs_to_process,
                    )

                    if prepare_dm.result != 0:
                        return

                    # Preserve the files
                    with prepare_dm.Nested("\nPreserving files...") as preserve_dm:
                        # ----------------------------------------------------------------------
                        def Move(
                            context: Common.DiffResult,
                            on_simple_status_func: Callable[[str], None],  # pylint: disable=unused-argument
                        ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[Optional[Path]]]:

                            diff = context

                            content_size = None

                            if diff.path.exists():
                                content_size = diff.path.stat().st_size

                            # ----------------------------------------------------------------------
                            def Execute(
                                status: ExecuteTasks.Status,
                            ) -> Tuple[Optional[Path], Optional[str]]:
                                if not diff.path.exists():
                                    raise Exception("The file '{}' was not found.".format(diff.path))

                                assert diff.path.is_file(), diff.path
                                assert isinstance(diff.this_hash, str), diff.this_hash
                                dest_filename = Path(diff.this_hash[:2]) / diff.this_hash[2:4] / diff.this_hash

                                Common.WriteFile(
                                    file_content_data_store,
                                    diff.path,
                                    dest_filename,
                                    lambda bytes_written: cast(None, status.OnProgress(bytes_written, None)),
                                )

                                return dest_filename, None

                            # ----------------------------------------------------------------------

                            return content_size, Execute

                        # ----------------------------------------------------------------------

                        ExecuteTasks.Transform(
                            preserve_dm,
                            "Processing",
                            [
                                ExecuteTasks.TaskData(Common.GetTaskDisplayName(diff.path), diff)
                                for diff in diffs_to_process
                            ],
                            Move,
                            quiet=quiet,
                            max_num_threads=None if file_content_data_store.ExecuteInParallel() else 1,
                            refresh_per_second=Common.EXECUTE_TASKS_REFRESH_PER_SECOND,
                        )

                        if preserve_dm.result != 0:
                            return

            with prepare_dm.Nested(
                "\nPreserving index...",
                suffix="\n",
            ):
                index_filename_path = Path(INDEX_FILENAME)

                with file_content_data_store.Open(index_filename_path, "w") as f:
                    json_diffs: List[Dict[str, Any]] = []

                    for these_diffs in diffs.values():
                        these_diffs.sort(key=lambda value: str(value.path))

                        for diff in these_diffs:
                            json_diffs.append(diff.ToJson())

                    json.dump(json_diffs, f)

                with file_content_data_store.Open(Path(INDEX_HASH_FILENAME), "w") as f:
                    f.write(
                        Common.CalculateHash(
                            file_content_data_store,
                            index_filename_path,
                            lambda _: None,
                        ),
                    )

            if encryption_password and compress:
                heading = "Compressing and encrypting..."
                encryption_arg = ' "-p{}"'.format(encryption_password)
                compression_level = 9
            elif encryption_password:
                heading = "Encrypting..."
                encryption_arg = ' "-p{}"'.format(encryption_password)
                compression_level = 0
            elif compress:
                heading = "Compressing..."
                encryption_arg = ""
                compression_level = 9
            else:
                heading = None
                encryption_arg = None
                compression_level = None

            if heading:
                with prepare_dm.Nested(
                    heading,
                    suffix="\n",
                ) as zip_dm:
                    assert zip_binary is not None

                    command_line = '{binary} a -t7z -mx{compression} -ms=on -mhe=on -sccUTF-8 -scsUTF-8 -ssw -v{archive_volume_size} "{archive_filename}" *{encryption_arg}'.format(
                        binary=zip_binary,
                        archive_filename=ARCHIVE_FILENAME,
                        compression=compression_level,
                        encryption_arg=encryption_arg,
                        archive_volume_size=archive_volume_size,
                    )

                    zip_dm.WriteVerbose(
                        "Command Line: {}\n\n".format(_ScrubZipCommandLine(command_line)),
                    )

                    with zip_dm.YieldStream() as stream:
                        zip_dm.result = SubprocessEx.Stream(
                            command_line,
                            stream,
                            cwd=file_content_root,
                        )

                    if zip_dm.result != 0:
                        return

                with prepare_dm.Nested(
                    "Validating archive...",
                    suffix="\n",
                ) as validate_dm:
                    assert zip_binary is not None

                    command_line = '{binary} t "{archive_filename}.001"{encryption_arg}'.format(
                        binary=zip_binary,
                        archive_filename=file_content_root / ARCHIVE_FILENAME,
                        encryption_arg=encryption_arg,
                    )

                    validate_dm.WriteVerbose(
                        "Command Line: {}\n\n".format(_ScrubZipCommandLine(command_line)),
                    )

                    with validate_dm.YieldStream() as stream:
                        validate_dm.result = SubprocessEx.Stream(
                            command_line,
                            stream,
                        )

                    if validate_dm.result != 0:
                        return

                with prepare_dm.Nested("Cleaning content...") as clean_dm:
                    for item in file_content_root.iterdir():
                        if item.name.startswith(ARCHIVE_FILENAME):
                            continue

                        with clean_dm.VerboseNested("Removing '{}'...".format(item)):
                            PathEx.RemoveItem(item)

        if not destination:
            with dm.Nested("Preserving the pending snapshot...") as pending_dm:
                local_snapshot.Persist(
                    pending_dm,
                    FileSystemDataStore(snapshot_filenames.pending),
                    snapshot_filename=snapshot_filenames.pending,
                )

                if pending_dm.result != 0:
                    return

            return

        with Common.YieldDataStore(
            dm,
            destination,
            ssd=ssd,
        ) as destination_data_store:
            if isinstance(destination_data_store, BulkStorageDataStore):
                # We want to include the date-based directory in the upload, so upload the
                # file content root parent rather than the file content root itself.
                destination_data_store.Upload(
                    dm,
                    snapshot_filenames.backup_name,
                    file_content_root.parent,
                )

            elif isinstance(destination_data_store, FileBasedDataStore):
                destination_data_store.SetWorkingDir(Path(snapshot_filenames.backup_name))

                # Get the files
                transfer_diffs: List[Common.DiffResult] = []

                for root, _, filenames in os.walk(file_content_root):
                    root = Path(root)

                    transfer_diffs += [
                        Common.DiffResult(
                            Common.DiffOperation.add,
                            filename,
                            "ignore",
                            filename.stat().st_size,
                            None,
                            None,
                        )
                        for filename in [root / filename for filename in filenames]
                    ]

                Common.ValidateSizeRequirements(
                    dm,
                    file_content_data_store,
                    destination_data_store,
                    transfer_diffs,
                    header="Validating destination size requirements...",
                )

                if dm.result != 0:
                    return

                dm.WriteLine("")

                with dm.Nested(
                    "Transferring content to the destination...",
                    suffix="\n",
                ) as transfer_dm:
                    len_file_content_root_parts = len(file_content_root.parts)

                    # ----------------------------------------------------------------------
                    def StripPath(
                        path: Path,
                        extension: str,
                    ) -> Path:
                        assert len(path.parts) > len_file_content_root_parts
                        assert path.parts[:len_file_content_root_parts] == file_content_root.parts, (path.parts, file_content_root.parts)

                        return (
                            Path(file_content_root.name)
                            / Path(*path.parts[len_file_content_root_parts:-1])
                            / (path.name + extension)
                        )

                    # ----------------------------------------------------------------------

                    pending_items = Common.CopyLocalContent(
                        transfer_dm,
                        destination_data_store,
                        transfer_diffs,
                        StripPath,
                        quiet=quiet,
                        ssd=ssd,
                    )

                    if transfer_dm.result != 0:
                        return

                    if not any(pending_item for pending_item in pending_items):
                        transfer_dm.WriteError("No content was transferred.\n")
                        return

                with dm.Nested(
                    "Committing content on the destination...",
                    suffix="\n",
                ) as commit_dm:
                    # ----------------------------------------------------------------------
                    def CommitContent(
                        context: Path,
                        on_simple_status_func: Callable[[str], None],  # pylint: disable=unused-argument
                    ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[None]]:
                        fullpath = context

                        # ----------------------------------------------------------------------
                        def Execute(
                            status: ExecuteTasks.Status,  # pylint: disable=unused-argument
                        ) -> Tuple[None, Optional[str]]:
                            destination_data_store.Rename(fullpath, fullpath.with_suffix(""))
                            return None, None

                        # ----------------------------------------------------------------------

                        return None, Execute

                    # ----------------------------------------------------------------------

                    ExecuteTasks.Transform(
                        commit_dm,
                        "Processing",
                        [
                            ExecuteTasks.TaskData(Common.GetTaskDisplayName(pending_item), pending_item)
                            for pending_item in pending_items if pending_item
                        ],
                        CommitContent,
                        quiet=quiet,
                        max_num_threads=None if destination_data_store.ExecuteInParallel() else 1,
                        refresh_per_second=Common.EXECUTE_TASKS_REFRESH_PER_SECOND,
                    )

                    if commit_dm.result != 0:
                        return

            else:
                assert False, destination_data_store  # pragma: no cover

            if dm.result != 0:
                return

            if commit_pending_snapshot:
                with dm.Nested("Committing snapshot locally...") as commit_dm:
                    local_snapshot.Persist(
                        commit_dm,
                        FileSystemDataStore(snapshot_filenames.standard.parent),
                        snapshot_filename=snapshot_filenames.standard,
                    )


# ----------------------------------------------------------------------
def Commit(
    dm: DoneManager,
    backup_name: str,
) -> None:
    snapshot_filenames = SnapshotFilenames.Create(backup_name)

    if not snapshot_filenames.pending.is_file():
        dm.WriteError("A pending snapshot for the backup '{}' was not found.\n".format(snapshot_filenames.backup_name))
        return

    with dm.Nested("Committing the pending snapshot for the backup '{}'...".format(snapshot_filenames.backup_name)):
        PathEx.RemoveFile(snapshot_filenames.standard)
        shutil.move(snapshot_filenames.pending, snapshot_filenames.standard)


# ----------------------------------------------------------------------
def Restore(
    dm: DoneManager,
    backup_name: str,
    data_store_connection_string: str,
    encryption_password: Optional[str],
    working_dir: Path,
    dir_substitutions: Dict[str, str],
    *,
    ssd: bool,
    quiet: bool,
    dry_run: bool,
    overwrite: bool,
) -> None:
    with Common.YieldDataStore(
        dm,
        data_store_connection_string,
        ssd=ssd,
    ) as data_store:
        if not isinstance(data_store, FileBasedDataStore):
            dm.WriteError(
                textwrap.dedent(
                    """\
                    '{}' does not resolve to a file-based data store, which is required when restoring content.

                    Most often, this error is encountered when attempting to restore an offsite backup that was
                    originally transferred to a cloud-based data store.

                    To restore these types of offsite backups, copy the content from the original data store
                    to your local file system and run this script again while pointing to that
                    location on your file system. This local directory should contain the primary directory
                    created during the initial backup and all directories created as a part of subsequent backups.

                    """,
                ).format(data_store_connection_string),
            )
            return

        with _YieldTempDirectory("staging content") as staging_directory:
            # ----------------------------------------------------------------------
            @dataclass(frozen=True)
            class Instruction(object):
                # ----------------------------------------------------------------------
                operation: Common.DiffOperation
                file_content_path: Optional[Path]
                original_filename: str
                local_filename: Path

                # ----------------------------------------------------------------------
                def __post_init__(self):
                    assert self.file_content_path is None or self.operation in [Common.DiffOperation.add, Common.DiffOperation.modify]

            # ----------------------------------------------------------------------

            instructions: Dict[str, List[Instruction]] = {}

            # ----------------------------------------------------------------------
            def CountInstructions() -> int:
                total = 0

                for these_instructions in instructions.values():
                    total += len(these_instructions)

                return total

            # ----------------------------------------------------------------------

            with dm.Nested(
                "Processing file content...",
                lambda: "{} found".format(inflect.no("instruction", CountInstructions())),
            ) as preprocess_dm:
                backup_name_path = Path(backup_name)

                if data_store.GetItemType(backup_name_path) == ItemType.Dir:
                    data_store.SetWorkingDir(backup_name_path)

                # We should have a bunch of dirs organized by datetime
                offsite_directories: Dict[str, List[Tuple[str, bool]]] = {}

                for _, directories, filenames in data_store.Walk():
                    if filenames:
                        preprocess_dm.WriteError(
                            textwrap.dedent(
                                """\
                                Files were not expected:

                                {}

                                """,
                            ).format(
                                "\n".join("    - '{}'".format(filename) for filename in filenames),
                            ),
                        )
                        return

                    dir_regex = re.compile(
                        textwrap.dedent(
                            r"""(?#
                            Year                )(?P<year>\d{{4}})(?#
                            Month               )\.(?P<month>\d{{2}})(?#
                            Day                 )\.(?P<day>\d{{2}})(?#
                            Hour                )\.(?P<hour>\d{{2}})(?#
                            Minute              )\.(?P<minute>\d{{2}})(?#
                            Second              )\.(?P<second>\d{{2}})(?#
                            Index               )-(?P<index>\d+)(?#
                            Suffix              )(?P<suffix>{})?(?#
                            )""",
                        ).format(re.escape(DELTA_SUFFIX)),
                    )

                    for directory in directories:
                        match = dir_regex.match(directory)
                        if not match:
                            preprocess_dm.WriteError("'{}' is not a recognized directory name.".format(directory))
                            return

                        offsite_directories.setdefault(directory,[]).append(
                            (
                                directory,
                                not match.group("suffix"),
                            ),
                        )

                    # Only process top-level items
                    break

                if not offsite_directories:
                    preprocess_dm.WriteError("No directories were found.")
                    return

                # Sort the directories
                keys = list(offsite_directories.keys())
                keys.sort()

                all_directories: List[Tuple[str, bool]] = []

                for key in keys:
                    all_directories += offsite_directories[key]

                primary_indexes: List[int] = []

                for index, (directory, is_primary) in enumerate(all_directories):
                    if is_primary:
                        primary_indexes.append(index)

                if not primary_indexes:
                    preprocess_dm.WriteError("No primary directories were found.\n")
                    return

                if len(primary_indexes) > 1:
                    preprocess_dm.WriteError(
                        textwrap.dedent(
                            """\
                            Multiple primary directories were found.

                            Primary Directories found:

                            {}

                            """,
                        ).format(
                            "\n".join("    - '{}'".format(all_directories[primary_index][0]) for primary_index in primary_indexes),
                        ),
                    )
                    return

                directories = [data[0] for data in all_directories[primary_indexes[-1]:]]

                # Process each directory

                # ----------------------------------------------------------------------
                class ProcessDirectoryState(Enum):
                    Transferring                = 0
                    Extracting                  = auto()
                    Verifying                   = auto()
                    Moving                      = auto()

                # ----------------------------------------------------------------------
                def ProcessDirectory(
                    context: str,
                    on_simple_status_func: Callable[[str], None],  # pylint: disable=unused-argument
                ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[Path]]:
                    directory = context

                    # ----------------------------------------------------------------------
                    def Execute(
                        status: ExecuteTasks.Status,
                    ) -> Tuple[Path, Optional[str]]:
                        destination_dir = working_dir / directory

                        if destination_dir.is_dir():
                            # The destination already exists, no need to process it further
                            return destination_dir, None

                        with _YieldRestoredArchive(
                            data_store,
                            directory,
                            lambda bytes_transferred: cast(
                                None,
                                status.OnProgress(
                                    ProcessDirectoryState.Transferring.value + 1,
                                    bytes_transferred,
                                ),
                            ),
                        ) as (archive_directory, archive_directory_is_temporary):
                            with _YieldRestoredFiles(
                                directory,
                                archive_directory,
                                encryption_password,
                                lambda message: cast(
                                    None,
                                    status.OnProgress(
                                        ProcessDirectoryState.Extracting.value + 1,
                                        message,
                                    ),
                                ),
                            ) as (contents_dir, contents_dir_is_temporary):
                                # Validate the contents
                                _VerifyRestoredFiles(
                                    directory,
                                    contents_dir,
                                    lambda message: cast(
                                        None,
                                        status.OnProgress(
                                            ProcessDirectoryState.Verifying.value + 1,
                                            message,
                                        ),
                                    ),
                                )

                                # Move/Copy the content. Note that the code assumes a flat directory
                                # structure in that it doesn't do anything to account for nested dirs.
                                # The assumption matches the current archive format.
                                if archive_directory_is_temporary or contents_dir_is_temporary:
                                    func = shutil.move
                                else:
                                    # ----------------------------------------------------------------------
                                    def CreateSymLink(
                                        source: Path,
                                        dest: Path,
                                    ) -> None:
                                        dest /= source.name

                                        os.symlink(source, dest, target_is_directory=source.is_dir())

                                    # ----------------------------------------------------------------------

                                    func = CreateSymLink

                                temp_dest_dir = destination_dir.parent / (destination_dir.name + "__temp__")

                                PathEx.RemoveTree(temp_dest_dir)
                                temp_dest_dir.mkdir(parents=True)

                                items = [item for item in contents_dir.iterdir() if item.name != INDEX_HASH_FILENAME]

                                for item_index, item in enumerate(items):
                                    status.OnProgress(
                                        ProcessDirectoryState.Moving.value + 1,
                                        "Moving {} of {}...".format(item_index + 1, len(items)),
                                    )

                                    func(item, temp_dest_dir)

                                shutil.move(temp_dest_dir, destination_dir)

                        return destination_dir, None

                    # ----------------------------------------------------------------------

                    return len(ProcessDirectoryState), Execute

                # ----------------------------------------------------------------------

                directory_working_dirs: List[Optional[Path]] = ExecuteTasks.Transform(
                    preprocess_dm,
                    "Processing",
                    [
                        ExecuteTasks.TaskData(directory, directory)
                        for directory in directories
                    ],
                    ProcessDirectory,
                    quiet=quiet,
                    max_num_threads=None if ssd and data_store.ExecuteInParallel() else 1,
                    refresh_per_second=Common.EXECUTE_TASKS_REFRESH_PER_SECOND,
                )

                if preprocess_dm.result != 0:
                    return

                with preprocess_dm.Nested("Staging working content...") as stage_dm:
                    # ----------------------------------------------------------------------
                    def HashToFilename(
                        hash_value: str,
                    ) -> Path:
                        return (
                            staging_directory
                            / hash_value[:2]
                            / hash_value[2:4]
                            / hash_value
                        )

                    # ----------------------------------------------------------------------
                    def PathToFilename(
                        path: str,
                    ) -> Path:
                        for source_text, dest_text in dir_substitutions.items():
                            path = path.replace(source_text, dest_text)

                        return Path(path)

                    # ----------------------------------------------------------------------

                    file_hashes: Set[str] = set()

                    for index, (directory, directory_working_dir) in enumerate(zip(directories, directory_working_dirs)):
                        assert directory_working_dir is not None

                        these_instructions: List[Instruction] = []

                        with stage_dm.Nested(
                            "Processing '{}' ({} of {})...".format(
                                directory,
                                index + 1,
                                len(directories),
                            ),
                            lambda: "{} added".format(inflect.no("instruction", len(these_instructions))),
                        ):
                            # Link the content
                            for root, _, filenames in os.walk(
                                directory_working_dir,
                                followlinks=True,
                            ):
                                root = Path(root)

                                if root == directory_working_dir:
                                    continue

                                for filename in filenames:
                                    fullpath = root / filename

                                    dest_filename = staging_directory / Path(*fullpath.parts[len(directory_working_dir.parts):])

                                    dest_filename.parent.mkdir(parents=True, exist_ok=True)

                                    os.symlink(fullpath, dest_filename)

                            # Read the instructions
                            with (directory_working_dir / INDEX_FILENAME).open() as f:
                                json_content = json.load(f)

                            # TODO: Validate JSON against schema

                            for item_index, item in enumerate(json_content):
                                try:
                                    assert "operation" in item, item

                                    if item["operation"] == "add":
                                        hash_value = item.get("this_hash", None)

                                        if hash_value is None:
                                            # We need to create a directory
                                            hash_filename = None
                                        else:
                                            hash_filename = HashToFilename(item["this_hash"])

                                            file_hashes.add(item["this_hash"])

                                        these_instructions.append(
                                            Instruction(
                                                Common.DiffOperation.add,
                                                hash_filename,
                                                item["path"],
                                                PathToFilename(item["path"]),
                                            ),
                                        )

                                    elif item["operation"] == "modify":
                                        if item["other_hash"] not in file_hashes:
                                            raise Exception("The original file does not exist in the staged content.")

                                        new_hash_filename = HashToFilename(item["this_hash"])
                                        file_hashes.add(item["this_hash"])

                                        these_instructions.append(
                                            Instruction(
                                                Common.DiffOperation.modify,
                                                new_hash_filename,
                                                item["path"],
                                                PathToFilename(item["path"]),
                                            ),
                                        )

                                    elif item["operation"] == "remove":
                                        hash_value = item.get("other_hash", None)

                                        if hash_value is not None:
                                            if item["other_hash"] not in file_hashes:
                                                raise Exception("The referenced file does not exist in the staged content.")

                                        these_instructions.append(
                                            Instruction(
                                                Common.DiffOperation.remove,
                                                None,
                                                item["path"],
                                                PathToFilename(item["path"]),
                                            ),
                                        )

                                    else:
                                        assert False, item["operation"]  # pragma: no cover

                                except Exception as ex:
                                    raise Exception(
                                        textwrap.dedent(
                                            """\
                                            An error was encountered while processing '{}' [Index: {}].

                                                Original Filename:      {}
                                                Error:                  {}

                                            """,
                                        ).format(
                                            directory,
                                            item_index,
                                            item["path"],
                                            str(ex),
                                        ),
                                    ) from ex

                        assert these_instructions
                        instructions[directory] = these_instructions

            with dm.Nested("\nProcessing instructions...") as all_instructions_dm:
                all_instructions_dm.WriteLine("")

                temp_directory = CurrentShell.CreateTempDirectory()

                with ExitStack(lambda: PathEx.RemoveTree(temp_directory)):
                    commit_actions: List[Callable[[], None]] = []

                    # ----------------------------------------------------------------------
                    def WriteImpl(
                        local_filename: Path,
                        content_filename: Optional[Path],
                    ) -> None:
                        if content_filename is None:
                            # ----------------------------------------------------------------------
                            def CommitDir() -> None:
                                if local_filename.is_file():
                                    PathEx.RemoveFile(local_filename)

                                local_filename.mkdir(parents=True, exist_ok=True)

                            # ----------------------------------------------------------------------

                            commit_actions.append(CommitDir)
                            return

                        temp_filename = temp_directory / str(uuid.uuid4())

                        with content_filename.resolve().open("rb") as source:
                            with temp_filename.open("wb") as dest:
                                dest.write(source.read())

                        # ----------------------------------------------------------------------
                        def CommitFile() -> None:
                            PathEx.RemoveItem(local_filename)

                            local_filename.parent.mkdir(parents=True, exist_ok=True)
                            shutil.move(temp_filename, local_filename)

                        # ----------------------------------------------------------------------

                        commit_actions.append(CommitFile)

                    # ----------------------------------------------------------------------
                    def WriteItem(
                        dm: DoneManager,
                        instruction: Instruction,
                    ) -> None:
                        if (
                            instruction.local_filename.exists()
                            and not overwrite
                        ):
                            dm.WriteError(
                                "The local item '{}' exists and will not be overwritten.".format(
                                    instruction.local_filename,
                                ),
                            )
                            return

                        WriteImpl(instruction.local_filename, instruction.file_content_path)

                    # ----------------------------------------------------------------------
                    def UpdateFile(
                        dm: DoneManager,  # pylint: disable=unused-argument
                        instruction: Instruction,
                    ) -> None:
                        assert instruction.file_content_path is not None

                        WriteImpl(instruction.local_filename, instruction.file_content_path)

                    # ----------------------------------------------------------------------
                    def RemoveItem(
                        dm: DoneManager,  # pylint: disable=unused-argument
                        instruction: Instruction,
                    ) -> None:
                        commit_actions.append(
                            lambda: cast(None, PathEx.RemoveItem(instruction.local_filename)),
                        )

                    # ----------------------------------------------------------------------

                    operation_map: Dict[
                        Common.DiffOperation,
                        Tuple[
                            str,                # Heading prefix
                            Callable[[DoneManager, Instruction], None],
                        ],
                    ] = {
                        Common.DiffOperation.add: ("Restoring", WriteItem),
                        Common.DiffOperation.modify: ("Updating", UpdateFile),
                        Common.DiffOperation.remove: ("Removing", RemoveItem),
                    }

                    for directory_index, (directory, these_instructions) in enumerate(instructions.items()):
                        with all_instructions_dm.Nested(
                            "Processing '{}' ({} of {})...".format(directory, directory_index + 1, len(instructions)),
                            suffix="\n",
                        ) as instructions_dm:
                            with instructions_dm.YieldStream() as stream:
                                stream.write(
                                    textwrap.dedent(
                                        """\

                                        {}
                                        """,
                                    ).format(
                                        TextwrapEx.CreateTable(
                                            [
                                                "Operation",
                                                "Local Location",
                                                "Original Location",
                                            ],
                                            [
                                                [
                                                    "[{}]".format(str(instruction.operation).split(".")[1].upper()),
                                                    str(instruction.local_filename),
                                                    instruction.original_filename,
                                                ]
                                                for instruction in these_instructions
                                            ],
                                            [
                                                TextwrapEx.Justify.Center,
                                                TextwrapEx.Justify.Left,
                                                TextwrapEx.Justify.Left,
                                            ],
                                        ),
                                    ),
                                )

                            if not dry_run:
                                for instruction_index, instruction in enumerate(these_instructions):
                                    prefix, func = operation_map[instruction.operation]

                                    with instructions_dm.Nested(
                                        "{} the {} '{}' ({} of {})...".format(
                                            prefix,
                                            "file" if instruction.file_content_path is not None else "directory",
                                            instruction.local_filename,
                                            instruction_index + 1,
                                            len(these_instructions),
                                        ),
                                    ) as execute_dm:
                                        func(execute_dm, instruction)

                                        if execute_dm.result != 0:
                                            break

                                instructions_dm.WriteLine("")

                            if instructions_dm.result != 0:
                                break

                    # Commit
                    with all_instructions_dm.Nested("Committing content..."):
                        for commit_action in commit_actions:
                            commit_action()


# ----------------------------------------------------------------------
# |
# |  Private Functions
# |
# ----------------------------------------------------------------------
# Not using functools.cache here, as we want the function to generate exceptions each time it is invoked,
# but only calculate the result once
_get_zip_binary_result: Union[None, str, Exception] = None
_get_zip_binary_result_lock = threading.Lock()

def _GetZipBinary() -> str:
    global _get_zip_binary_result  # pylint: disable=global-statement

    with _get_zip_binary_result_lock:
        if _get_zip_binary_result is None:
            if CurrentShell.family_name == "Linux":
                zip_binary = "7zz"  # pragma: no cover
            else:
                zip_binary = "7z"  # pragma: no cover

            # Ensure that the binary is installed
            result = SubprocessEx.Run(zip_binary)

            if result.returncode == 0:
                _get_zip_binary_result = zip_binary
            else:
                _get_zip_binary_result = Exception(
                    "7zip is not available for compression and/or encryption; please add it to the path before invoking this script ({}).".format(zip_binary),
                )

        if isinstance(_get_zip_binary_result, Exception):
            raise _get_zip_binary_result

        return _get_zip_binary_result


# ----------------------------------------------------------------------
def _ScrubZipCommandLine(
    command_line: str,
) -> str:
    """Produces a string suitable for display within a log file"""

    return re.sub(
        r'"-p(?P<password>\\\"|[^\"])+\"',
        '"-p*****"',
        command_line,
    )


# ----------------------------------------------------------------------
@contextmanager
def _YieldTempDirectory(
    desc: str,
) -> Iterator[Path]:
    temp_directory = CurrentShell.CreateTempDirectory()
    should_delete = True

    try:
        yield temp_directory
    except:
        should_delete = False
        raise
    finally:
        if should_delete:
            PathEx.RemoveTree(temp_directory)
        else:
            # This is such an uncommon scenario that we can afford to write
            # to stdout.
            sys.stderr.write(
                "**** The temporary directory '{}' has been preserved due to exceptions while {}.\n".format(
                    temp_directory,
                    desc,
                ),
            )


# ----------------------------------------------------------------------
@contextmanager
def _YieldRestoredArchive(
    data_store: FileBasedDataStore,
    directory: str,
    status: Callable[[str], None],
) -> Iterator[
    Tuple[
        Path,
        bool,                               # is temporary directory
    ],
]:
    if data_store.is_local_filesystem:
        working_dir = data_store.GetWorkingDir() / directory
        assert working_dir.is_dir(), working_dir

        yield working_dir, False
        return

    status("Calculating files to transfer...")

    with _YieldTempDirectory("transferring archive files") as temp_directory:
        # Map the remote filenames to local filenames
        filename_map: Dict[Path, Path] = {}

        # Don't change the capability's working dir, as multiple threads
        # might be accessing it at the same time. That does make this code
        # bit more more cumbersome.
        capability_dir = data_store.GetWorkingDir() / directory
        len_capability_dir_parts = len(capability_dir.parts)

        for root, _, filenames in data_store.Walk(Path(directory)):
            assert root.parts[:len_capability_dir_parts] == capability_dir.parts, (root.parts, capability_dir.parts)
            relative_root = Path(*root.parts[len_capability_dir_parts:])

            for filename in filenames:
                filename_map[root / filename] = temp_directory / relative_root / filename

        if not filename_map:
            raise Exception("The directory '{}' does not contain any files.".format(directory))

        # Transfer the files
        for filename_index, (source_filename, dest_filename) in enumerate(filename_map.items()):
            file_size = max(1, data_store.GetFileSize(source_filename))

            status_template = "Transferring '{}' ({} of {}) [{}] {{:.02f}}%...".format(
                source_filename.name,
                filename_index,
                len(filename_map),
                TextwrapEx.GetSizeDisplay(file_size),
            )

            with data_store.Open(source_filename, "rb") as source:
                dest_filename.parent.mkdir(parents=True, exist_ok=True)

                with dest_filename.open("wb") as dest:
                    bytes_transferred = 0

                    while True:
                        chunk = source.read(16384)
                        if not chunk:
                            break

                        dest.write(chunk)

                        bytes_transferred += len(chunk)
                        status(status_template.format((bytes_transferred / file_size) * 100))

        yield temp_directory, True


# ----------------------------------------------------------------------
@contextmanager
def _YieldRestoredFiles(
    directory_name: str,
    archive_dir: Path,
    encryption_password: Optional[str],
    status: Callable[[str], None],
) -> Iterator[
    Tuple[
        Path,
        bool,                               # is temporary directory
    ],
]:
    if (archive_dir / INDEX_FILENAME).is_file():
        yield archive_dir, False
        return

    # By default, 7zip will prompt for a password with archives that were created
    # with a password but no password was provided. This is not what we want, as
    # it will block indefinitely. Instead, employ this workaround suggested at
    # https://sourceforge.net/p/sevenzip/discussion/45798/thread/2b98fd92/.
    #
    #   1) Attempt to extract with a bogus password; this will work for archives
    #      created without a password.
    #
    #   2) If extraction fails, issue an error.
    #
    password = encryption_password or str(uuid.uuid4())

    # Validate
    status("Validating archive...")

    archive_filename = archive_dir / (ARCHIVE_FILENAME + ".001")

    if not archive_filename.is_file():
        raise Exception("The archive file '{}' was not found.".format(archive_filename.name))

    result = SubprocessEx.Run(
        '{binary} t "{filename}" "-p{password}"'.format(
            binary=_GetZipBinary(),
            password=password,
            filename=archive_filename,
        ),
    )

    if result.returncode != 0:
        raise Exception(
            textwrap.dedent(
                """\
                Archive validation failed for the directory '{}' ({}).

                    {}

                """,
            ).format(
                directory_name,
                result.returncode,
                TextwrapEx.Indent(
                    result.output.strip(),
                    4,
                    skip_first_line=True,
                ),
            ),
        )

    # Extract
    status("Extracting archive...")

    with _YieldTempDirectory("extracting the archive") as temp_directory:
        result = SubprocessEx.Run(
            '{binary} x "{filename}" "-p{password}"'.format(
                binary=_GetZipBinary(),
                password=password,
                filename=archive_filename,
            ),
            cwd=temp_directory,
        )

        if result.returncode != 0:
            raise Exception(
                textwrap.dedent(
                    """\
                    Archive extraction failed for the directory '{}' ({}).

                        {}

                    """,
                ).format(
                    directory_name,
                    result.returncode,
                    TextwrapEx.Indent(
                        result.output.strip(),
                        4,
                        skip_first_line=True,
                    ),
                ),
            )

        yield temp_directory, True


# ----------------------------------------------------------------------
def _VerifyRestoredFiles(
    directory_name: str,
    contents_dir: Path,
    status: Callable[[str], None],
) -> None:
    # Ensure that the index is present
    for filename in [INDEX_FILENAME, INDEX_HASH_FILENAME]:
        if not (contents_dir / filename).is_file():
            raise Exception("The index file '{}' does not exist.".format(filename))

    # Ensure that the content is valid
    all_filenames: List[Path] = []

    for root, _, filenames in os.walk(contents_dir):
        root = Path(root)

        all_filenames += [
            root / filename
            for filename in filenames
            if filename != INDEX_HASH_FILENAME
        ]

    data_store = FileSystemDataStore()

    errors: List[str] = []

    for filename_index, filename in enumerate(all_filenames):
        if filename.name == INDEX_FILENAME:
            with (contents_dir / INDEX_HASH_FILENAME).open() as f:
                expected_hash_value = f.read().strip()
        else:
            expected_hash_value = filename.name

        file_size = max(1, filename.stat().st_size)

        status_template = "Validating file {} of {} [{}] {{:.02f}}%...".format(
            filename_index + 1,
            len(all_filenames),
            TextwrapEx.GetSizeDisplay(file_size),
        )

        actual_hash_value = Common.CalculateHash(
            data_store,
            filename,
            lambda bytes_transferred: status(
                status_template.format((bytes_transferred / file_size) * 100),
            ),
        )

        if actual_hash_value != expected_hash_value:
            errors.append(
                textwrap.dedent(
                    """\
                    Filename:   {}
                    Expected:   {}
                    Actual:     {}
                    """,
                ).format(
                    Path(*filename.parts[len(contents_dir.parts):]),
                    expected_hash_value,
                    actual_hash_value,
                ),
            )

    if errors:
        raise Exception(
            textwrap.dedent(
                """\
                Corrupt files were encountered in the directory '{}'.

                    {}

                """,
            ).format(
                directory_name,
                TextwrapEx.Indent(
                    "\n".join(errors),
                    4,
                    skip_first_line=True,
                ).rstrip(),
            ),
        )
