# ----------------------------------------------------------------------
# |
# |  FileSystemDestination.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-12 10:59:45
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Contains the `FileSystemDestination` object"""

import itertools
import os
import shutil
import textwrap

from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx
from Common_Foundation.Shell.All import CurrentShell
from Common_Foundation.Streams.DoneManager import DoneManager
from Common_Foundation import TextwrapEx
from Common_Foundation.Types import overridemethod

from Common_FoundationEx import ExecuteTasks
from Common_FoundationEx.InflectEx import inflect

from .Destination import Destination, Snapshot, ValidateType


# ----------------------------------------------------------------------
class FileSystemDestination(Destination):
    """Preserves content to the local destination"""

    PENDING_COMMIT_EXTENSION                = ".__pending_commit__"
    PENDING_DELETE_EXTENSION                = ".__pending_delete__"

    @staticmethod
    def GetSnapshotContentDir(
        root: Path,
    ) -> Path:
        return root / "Content"

    # ----------------------------------------------------------------------
    def __init__(
        self,
        root: Path,
        *,
        force: bool,
        is_ssd: bool,
        quiet: bool,
    ):
        self.root                           = root.resolve()
        self.force                          = force
        self.is_ssd                         = is_ssd
        self.quiet                          = quiet

    # ----------------------------------------------------------------------
    @overridemethod
    def GetMirroredSnapshot(
        self,
        dm: DoneManager,
    ) -> Snapshot:
        self.root.mkdir(parents=True, exist_ok=True)

        if self.force or not Snapshot.IsPersisted(self.root):
            # ----------------------------------------------------------------------
            def FilterFunc(
                fullpath: Path,
            ) -> bool:
                return fullpath.suffix not in [
                    self.__class__.PENDING_COMMIT_EXTENSION,
                    self.__class__.PENDING_DELETE_EXTENSION,
                ]

            # ----------------------------------------------------------------------

            content_dir = self.__class__.GetSnapshotContentDir(self.root)  # pylint: disable=protected-access

            content_dir.mkdir(parents=True, exist_ok=True)

            return Snapshot.Calculate(
                dm,
                [content_dir, ],
                is_ssd=self.is_ssd,
                quiet=self.quiet,
                filter_filename_func=FilterFunc,
            )

        return Snapshot.LoadPersisted(dm, self.root)

    # ----------------------------------------------------------------------
    @overridemethod
    def ProcessMirroredSnapshot(
        self,
        dm: DoneManager,
        local_snapshot: Snapshot,
        mirrored_snapshot: Snapshot,        # Returned from `GetMirroredSnapshot`
    ) -> None:
        diffs: Dict[Snapshot.DiffOperation, List[Snapshot.DiffResult]] = {
            Snapshot.DiffOperation.add: [],
            Snapshot.DiffOperation.modify: [],
            Snapshot.DiffOperation.remove: [],
        }

        with dm.Nested(
            "Calculating diffs...",
            lambda: "{} found".format(inflect.no("diff", sum(len(diff_items) for diff_items in diffs.values()))),
        ):
            for diff in local_snapshot.Diff(mirrored_snapshot):
                assert diff.operation in diffs, diff.operation
                diffs[diff.operation].append(diff)


        if not any(diff_items for diff_items in diffs.values()):
            return

        # Ensure that there is enough space to handle the changes
        bytes_required = 0
        bytes_available = 0

        with dm.Nested(
            "Validating size requirements...",
            [
                lambda: "{} required".format(TextwrapEx.GetSizeDisplay(bytes_required)),
                lambda: "{} available".format(TextwrapEx.GetSizeDisplay(bytes_available)),
            ],
        ) as validate_dm:
            for diff in itertools.chain(diffs[Snapshot.DiffOperation.add], diffs[Snapshot.DiffOperation.modify]):
                if diff.path.is_dir():
                    continue

                if not diff.path.is_file():
                    if not diff.path.is_symlink():
                        validate_dm.WriteInfo("The local file '{}' is no longer available.\n".format(diff.path))

                    continue

                bytes_required += diff.path.stat().st_size

            bytes_available = shutil.disk_usage(self.root).free

            if (bytes_available * 0.85) <= bytes_required:
                validate_dm.WriteError("There is not enough disk space to process this request.\n")
                return

        self.CleanPreviousRun(dm)
        if dm.result != 0:
            return

        content_dir = self.__class__.GetSnapshotContentDir(self.root)  # pylint: disable=protected-access

        # Persist context to temp dir
        with dm.Nested("Persisting content...") as persist_dm:
            temp_directory = CurrentShell.CreateTempDirectory()

            with ExitStack(lambda: PathEx.RemoveTree(temp_directory)):
                with persist_dm.Nested(
                    "Persisting snapshot data...",
                    suffix="\n",
                ) as snapshot_dm:
                    with snapshot_dm.VerboseNested("...") as verbose_dm:
                        local_snapshot.Persist(verbose_dm, temp_directory)

                    if snapshot_dm.result != 0:
                        return

                executed_work = False

                if CurrentShell.family_name == "Windows":
                    # ----------------------------------------------------------------------
                    def CreateDestinationPathWindows(
                        path: Path,
                        extension: str,
                    ) -> Path:
                        assert ":" in path.parts[0], path.parts

                        return (
                            content_dir
                            / path.parts[0].replace(":", "_").rstrip("\\")
                            / Path(*path.parts[1:-1])
                            / (path.name + extension)
                        )

                    # ----------------------------------------------------------------------

                    create_destination_path_func = CreateDestinationPathWindows

                else:
                    # ----------------------------------------------------------------------
                    def CreateDestinationPathNotWindows(
                        path: Path,
                        extension: str,
                    ) -> Path:
                        assert path.parts[0] == "/", path.parts

                        return (
                            content_dir
                            / Path(*path.parts[1:-1])
                            / (path.name + extension)
                        )

                    # ----------------------------------------------------------------------

                    create_destination_path_func = CreateDestinationPathNotWindows

                temp_commit_items: List[Optional[Path]] = []
                temp_delete_items: List[Optional[Path]] = []

                # If force, decorate the original content dir to indicate that it should be removed
                if self.force and content_dir.is_dir():
                    with dm.Nested("Marking the previous mirrored content for deletion..."):
                        for child in content_dir.iterdir():
                            dest_child = child.with_suffix(child.suffix + self.__class__.PENDING_DELETE_EXTENSION)

                            shutil.move(child, dest_child)
                            temp_delete_items.append(dest_child)

                # Move added & modified files to temp files in dest dir
                if diffs[Snapshot.DiffOperation.add] or diffs[Snapshot.DiffOperation.modify]:
                    with persist_dm.Nested(
                        "Persisting added content...",
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

                            dest_filename = create_destination_path_func(
                                source_filename,
                                self.__class__.PENDING_COMMIT_EXTENSION,
                            )

                            source_file_handle = None
                            content_size = None

                            if source_filename.is_file():
                                # Open this file early to decrease the likelihood that it is deleted
                                # before we read its contents.
                                source_file_handle = source_filename.open("rb")
                                content_size = source_filename.stat().st_size

                            elif source_filename.is_dir():
                                content_size = 1

                            # ----------------------------------------------------------------------
                            def Execute(
                                status: ExecuteTasks.Status,
                            ) -> Tuple[Optional[Path], Optional[str]]:
                                if not source_file_handle and not source_filename.is_dir():
                                    return None, None

                                temp_dest_filename = dest_filename.parent / "{}.__temp__{}".format(
                                    dest_filename.stem,
                                    dest_filename.suffix,
                                )

                                if source_filename.is_dir():
                                    temp_dest_filename.mkdir(parents=True)
                                else:
                                    assert source_file_handle is not None

                                    with ExitStack(source_file_handle.close):
                                        # Write the file
                                        temp_dest_filename.parent.mkdir(parents=True, exist_ok=True)

                                        with temp_dest_filename.open("wb") as dest:
                                            assert source_file_handle is not None
                                            bytes_written = 0

                                            while True:
                                                content = source_file_handle.read(8192)
                                                if not content:
                                                    break

                                                dest.write(content)

                                                bytes_written += len(content)
                                                status.OnProgress(bytes_written, None)

                                # Rename the item
                                if dest_filename.is_file():
                                    PathEx.RemoveFile(dest_filename)
                                elif dest_filename.is_dir():
                                    PathEx.RemoveTree(dest_filename)

                                shutil.move(temp_dest_filename, dest_filename)
                                return dest_filename, None

                            # ----------------------------------------------------------------------

                            return content_size, Execute

                        # ----------------------------------------------------------------------

                        temp_commit_items += ExecuteTasks.Transform(
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
                            quiet=self.quiet,
                            max_num_threads=1 if not self.is_ssd else None,
                        )

                        if this_dm.result != 0:
                            return

                        executed_work = True

                # Rename removed & modified files to to-be-deleted
                if diffs[Snapshot.DiffOperation.modify] or diffs[Snapshot.DiffOperation.remove]:
                    with persist_dm.Nested(
                        "Persisting removed content...",
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
                            on_simple_status_func: Callable[[str], None],  # pylint: disable=unused-argument
                        ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[Optional[Path]]]:

                            source_filename = context

                            dest_filename = create_destination_path_func(
                                source_filename,
                                self.__class__.PENDING_DELETE_EXTENSION,
                            )

                            # ----------------------------------------------------------------------
                            def Execute(
                                status: ExecuteTasks.Status,  # pylint: disable=unused-argument
                            ) -> Tuple[Optional[Path], Optional[str]]:
                                original_dest_filename = dest_filename.with_suffix("")

                                if not original_dest_filename.exists():
                                    return None, None

                                shutil.move(original_dest_filename, dest_filename)
                                return dest_filename, None

                            # ----------------------------------------------------------------------

                            return None, Execute

                        # ----------------------------------------------------------------------

                        temp_delete_items += ExecuteTasks.Transform(
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
                            quiet=self.quiet,
                            max_num_threads=1 if not self.is_ssd else None,
                        )

                        if this_dm.result != 0:
                            return

                        executed_work = True

                if executed_work:
                    for desc, items, func in [
                        ("Committing removed content...", temp_delete_items, lambda fullpath: fullpath.unlink(fullpath) if fullpath.is_file() else PathEx.RemoveTree(fullpath)),
                        ("Committing added content...", temp_commit_items, lambda fullpath: shutil.move(fullpath, fullpath.with_suffix(""))),
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
                                        if fullpath.exists():
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
                                    quiet=self.quiet,
                                    max_num_threads=1 if not self.is_ssd else None,
                                )

                                if this_dm.result != 0:
                                    return

                    with persist_dm.Nested("Committing snapshot data...") as commit_dm:
                        snapshot_items = list(temp_directory.iterdir())
                        assert len(snapshot_items) == 1, snapshot_items

                        PathEx.RemoveFile(self.root / snapshot_items[0].name)
                        shutil.move(snapshot_items[0], self.root)

                        if commit_dm.result != 0:
                            return

    # ----------------------------------------------------------------------
    @overridemethod
    def CleanPreviousRun(
        self,
        dm: DoneManager,
    ) -> None:
        items_restored = 0

        with dm.Nested(
            "Cleaning destination content...",
            lambda: "{} restored".format(inflect.no("item", items_restored)),
        ) as clean_dm:
            content_dir = self.GetSnapshotContentDir(self.root)

            if not content_dir.is_dir():
                clean_dm.WriteInfo("Content does not exist in '{}'.\n".format(self.root))
                return

            for root, directories, filenames in os.walk(content_dir):
                root = Path(root)

                for directory in directories:
                    fullpath = root / directory

                    if fullpath.suffix == self.__class__.PENDING_COMMIT_EXTENSION:
                        PathEx.RemoveTree(fullpath)

                        clean_dm.WriteVerbose("Removed '{}'.\n".format(fullpath))
                        items_restored += 1

                    elif fullpath.suffix == self.__class__.PENDING_DELETE_EXTENSION:
                        original_filename = fullpath.with_suffix("")

                        PathEx.RemoveItem(original_filename)
                        shutil.move(fullpath, original_filename)

                        clean_dm.WriteVerbose("Restored '{}'.\n".format(fullpath))
                        items_restored += 1

                for filename in filenames:
                    fullpath = root / filename

                    if fullpath.suffix == self.__class__.PENDING_COMMIT_EXTENSION:
                        PathEx.RemoveFile(fullpath)

                        clean_dm.WriteVerbose("Removing '{}'.\n".format(fullpath))
                        items_restored += 1

                    elif fullpath.suffix == self.__class__.PENDING_DELETE_EXTENSION:
                        original_filename = fullpath.with_suffix("")

                        PathEx.RemoveItem(original_filename)
                        shutil.move(fullpath, original_filename)

                        clean_dm.WriteVerbose("Restored '{}'.\n".format(fullpath))
                        items_restored += 1

    # ----------------------------------------------------------------------
    @overridemethod
    def Validate(
        self,
        dm: DoneManager,
        validate_type: ValidateType,
    ) -> None:
        if not Snapshot.IsPersisted(self.root):
            dm.WriteError("No snapshot was found at '{}'.\n".format(self.root))
            return

        mirrored_snapshot = Snapshot.LoadPersisted(dm, self.root)

        content_dir = self.__class__.GetSnapshotContentDir(self.root)  # pylint: disable=protected-access

        with dm.Nested(
            "\nExtracting local files...",
            suffix="\n",
        ) as extract_dm:
            self.CleanPreviousRun(extract_dm)

            actual_snapshot = Snapshot.Calculate(
                extract_dm,
                [content_dir],
                is_ssd=self.is_ssd,
                quiet=self.quiet,
                calculate_hashes=validate_type == ValidateType.complete,
            )

        # The values in the mirrored snapshot are based on the original values provided during the backup
        # while the values of the actual snapshot are based on what is on the filesystem. Convert
        # the data in the mirror snapshot so it matches the values in the actual snapshot before
        # we do the comparison.
        new_root = Snapshot.Node(None, None, Snapshot.DirHashPlaceholder(explicitly_added=False), None)

        content_root = new_root
        for part in content_dir.parts:
            new_content_root = Snapshot.Node(
                part,
                content_root,
                Snapshot.DirHashPlaceholder(explicitly_added=False),
                None,
            )

            content_root.children[part] = new_content_root
            content_root = new_content_root

        if CurrentShell.family_name == "Windows":
            for mirrored_root, mirrored_root_value in mirrored_snapshot.node.children.items():
                assert ":" in mirrored_root, mirrored_root

                new_mirrored_root = mirrored_root.replace(":", "_").rstrip("\\")

                mirrored_root_value.name = new_mirrored_root
                mirrored_root_value.parent = content_root

                content_root.children[mirrored_root_value.name] = mirrored_root_value

        else:
            assert len(mirrored_snapshot.node.children) == 1, mirrored_snapshot
            assert "/" in mirrored_snapshot.node.children, mirrored_snapshot

            for mirrored_root_value in mirrored_snapshot.node.children["/"].children.values():
                mirrored_root_value.parent = content_root

                assert mirrored_root_value.name is not None
                content_root.children[mirrored_root_value.name] = mirrored_root_value

        with dm.Nested(
            "Validating content...",
            suffix="\n" if dm.is_verbose else "",
        ) as validate_dm:
            # Windows and Linux have different sorting orders, so capture and sort the list before
            # displaying the contents.
            diffs = list(
                actual_snapshot.Diff(
                    Snapshot(new_root),
                    compare_hashes=validate_type == ValidateType.complete,
                ),
            )

            if not diffs:
                validate_dm.WriteInfo("The content is valid.\n")
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
