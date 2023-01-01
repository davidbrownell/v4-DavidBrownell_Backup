# ----------------------------------------------------------------------
# |
# |  Offsite_IntegrationTest.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-12-06 07:36:49
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Local integration tests for ../Offsite.py"""

import os
import re
import sys
import textwrap
import uuid

from dataclasses import dataclass
from pathlib import Path
from typing import cast, Dict, Match
from unittest import mock

import pytest

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx
from Common_Foundation.Shell.All import CurrentShell
from Common_Foundation.Streams.DoneManager import DoneManager
from Common_Foundation.TestHelpers.StreamTestHelpers import GenerateDoneManagerAndSink


# ----------------------------------------------------------------------
sys.path.insert(0, str(PathEx.EnsureDir(Path(__file__).parent.parent.parent.parent)))
with ExitStack(lambda: sys.path.pop(0)):
    from Backup.Impl.Offsite import Backup, Restore, SnapshotFilenames
    from Backup.Impl import TestHelpers


# ----------------------------------------------------------------------
@pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
@pytest.mark.parametrize("compress", [False, True])
class TestFileSystemBackup(object):
    # ----------------------------------------------------------------------
    def test(self, tmp_path_factory, _working_dir, compress, encryption_password):
        backup_destination = tmp_path_factory.mktemp("backup")
        snapshot_destination = tmp_path_factory.mktemp("snapshot")

        with mock.patch(
            "{}.{}.user_directory".format(CurrentShell.__module__, type(CurrentShell).__qualname__),
            new_callable=mock.PropertyMock(return_value=snapshot_destination),
        ):
            snapshot_filenames = SnapshotFilenames.Create("TestBackup")

            # ----------------------------------------------------------------------
            @dataclass(frozen=True)
            class BackupAndRestoreResult(object):
                output: str
                destination_dir_length: int

            # ----------------------------------------------------------------------
            def BackupAndRestore(
                expected_num_files: int,
                *,
                backup: bool=True,
                restore: bool=True,
                force: bool=False,
                ignore_pending_snapshot: bool=False,
                commit_pending_snapshot: bool=True,
            ) -> BackupAndRestoreResult:
                assert backup or restore, (backup, restore)

                backup_working_dir = tmp_path_factory.mktemp("backup_working")
                restore_working_destination = tmp_path_factory.mktemp("restore_working")
                restored_destination = tmp_path_factory.mktemp("restored")

                dm_and_sink = iter(GenerateDoneManagerAndSink(expected_result=0))

                dm = cast(DoneManager, next(dm_and_sink))

                # ----------------------------------------------------------------------
                def GetSinkOutput() -> str:
                    content = cast(str, next(dm_and_sink))

                    content = TestHelpers.OutputScrubber().Replace(content)

                    content = content.replace(str(backup_working_dir), "<backup working dir>")
                    content = content.replace(str(restore_working_destination), "<restore working dir>")
                    content = content.replace(str(restored_destination), "<restored dir>")

                    return content

                # ----------------------------------------------------------------------

                if backup:
                    Backup(
                        dm,
                        [_working_dir],
                        snapshot_filenames.backup_name,
                        backup_destination,
                        encryption_password,
                        backup_working_dir,
                        compress=compress,
                        ssd=False,
                        force=force,
                        quiet=False,
                        file_includes=None,
                        file_excludes=None,
                        ignore_pending_snapshot=ignore_pending_snapshot,
                        commit_pending_snapshot=commit_pending_snapshot,
                    )

                    assert dm.result == 0, (dm.result, GetSinkOutput())

                if restore:
                    Restore(
                        dm,
                        snapshot_filenames.backup_name,
                        backup_destination,
                        encryption_password,
                        restore_working_destination,
                        {
                            _working_dir.as_posix(): restored_destination.as_posix(),
                        },
                        ssd=False,
                        quiet=False,
                        dry_run=False,
                        overwrite=True,
                    )

                    assert dm.result == 0, (dm.result, GetSinkOutput())

                    TestHelpers.CompareFileSystemSourceAndDestination(
                        _working_dir,
                        restored_destination,
                        expected_num_files,
                        is_mirror=False,
                        compare_file_contents=True,
                    )

                return BackupAndRestoreResult(
                    GetSinkOutput(),
                    len(str(restored_destination)),
                )

            # ----------------------------------------------------------------------

            # Initial backup
            result = BackupAndRestore(10)

            if not compress and encryption_password is None:
                assert result.output == textwrap.dedent(
                    """\
                    Heading...
                      Creating the local snapshot...
                        Discovering files...
                          Processing 1 item...DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                        DONE! (0, <scrubbed duration>, 9 files found, 1 empty directory found)

                        Calculating hashes...
                          Processing 9 items...DONE! (0, <scrubbed duration>, 9 items succeeded, no items with errors, no items with warnings)
                        DONE! (0, <scrubbed duration>)

                        Organizing results...DONE! (0, <scrubbed duration>)
                      DONE! (0, <scrubbed duration>)

                      Calculating diffs...DONE! (0, <scrubbed duration>, 10 diffs found)

                      Preparing file content...
                        Validating size requirements...DONE! (0, <scrubbed duration>, <scrubbed space required>, <scrubbed space available>)

                        Preserving files...
                          Processing 9 items...DONE! (0, <scrubbed duration>, 9 items succeeded, no items with errors, no items with warnings)
                        DONE! (0, <scrubbed duration>)

                        Preserving index...DONE! (0, <scrubbed duration>)

                      DONE! (0, <scrubbed duration>)

                      Validating destination size requirements...DONE! (0, <scrubbed duration>, <scrubbed space required>, <scrubbed space available>)

                      Transferring content to the destination...
                        Processing 11 items...DONE! (0, <scrubbed duration>, 11 items succeeded, no items with errors, no items with warnings)
                      DONE! (0, <scrubbed duration>)

                      Committing content on the destination...
                        Processing 11 items...DONE! (0, <scrubbed duration>, 11 items succeeded, no items with errors, no items with warnings)
                      DONE! (0, <scrubbed duration>)

                      Committing snapshot locally...
                        Writing '{snapshot_destination}{sep}OffsiteBackup.TestBackup.json'...DONE! (0, <scrubbed duration>)
                      DONE! (0, <scrubbed duration>)
                      Processing file content...
                        Processing 1 item...DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                        Staging working content...
                          Processing '<Folder0>' (1 of 1)...DONE! (0, <scrubbed duration>, 10 instructions added)
                        DONE! (0, <scrubbed duration>)
                      DONE! (0, <scrubbed duration>, 10 instructions found)

                      Processing instructions...

                        Processing '<Folder0>' (1 of 1)...

                          Operation  Local Location{restored_destination_whitespace_delta}                                                                                                                                                                                                            Original Location
                          ---------  {restored_destination_sep_delta}------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------  {working_dir_sep_delta}------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                            [ADD]    <restored dir>{sep}EmptyDirTest{sep}EmptyDir                                                                                                                                                                                                    {working_dir}/EmptyDirTest/EmptyDir
                            [ADD]    <restored dir>{sep}VeryLongPaths{sep}11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111   {working_dir}/VeryLongPaths/11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
                            [ADD]    <restored dir>{sep}VeryLongPaths{sep}222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222  {working_dir}/VeryLongPaths/222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222
                            [ADD]    <restored dir>{sep}one{sep}A                                                                                                                                                                                                                    {working_dir}/one/A
                            [ADD]    <restored dir>{sep}one{sep}BC                                                                                                                                                                                                                   {working_dir}/one/BC
                            [ADD]    <restored dir>{sep}two{sep}Dir1{sep}File3                                                                                                                                                                                                           {working_dir}/two/Dir1/File3
                            [ADD]    <restored dir>{sep}two{sep}Dir1{sep}File4                                                                                                                                                                                                           {working_dir}/two/Dir1/File4
                            [ADD]    <restored dir>{sep}two{sep}Dir2{sep}Dir3{sep}File5                                                                                                                                                                                                      {working_dir}/two/Dir2/Dir3/File5
                            [ADD]    <restored dir>{sep}two{sep}File1                                                                                                                                                                                                                {working_dir}/two/File1
                            [ADD]    <restored dir>{sep}two{sep}File2                                                                                                                                                                                                                {working_dir}/two/File2

                          Restoring the directory '<restored dir>{sep}EmptyDirTest{sep}EmptyDir' (1 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}VeryLongPaths{sep}11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111' (2 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}VeryLongPaths{sep}222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222' (3 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}one{sep}A' (4 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}one{sep}BC' (5 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}two{sep}Dir1{sep}File3' (6 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}two{sep}Dir1{sep}File4' (7 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}two{sep}Dir2{sep}Dir3{sep}File5' (8 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}two{sep}File1' (9 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}two{sep}File2' (10 of 10)...DONE! (0, <scrubbed duration>)

                        DONE! (0, <scrubbed duration>)

                        Committing content...DONE! (0, <scrubbed duration>)
                      DONE! (0, <scrubbed duration>)
                    DONE! (0, <scrubbed duration>)
                    """,
                ).format(
                    restored_destination_sep_delta="-" * result.destination_dir_length,
                    restored_destination_whitespace_delta=" " * result.destination_dir_length,
                    snapshot_destination=snapshot_destination,
                    working_dir=_working_dir.as_posix(),
                    working_dir_sep_delta="-" * len(str(_working_dir)),
                    sep=os.path.sep,
                )

            # No changes
            result = BackupAndRestore(10)

            if not compress and encryption_password is None:
                assert result.output == textwrap.dedent(
                    """\
                    Heading...
                      Creating the local snapshot...
                        Discovering files...
                          Processing 1 item...DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                        DONE! (0, <scrubbed duration>, 9 files found, 1 empty directory found)

                        Calculating hashes...
                          Processing 9 items...DONE! (0, <scrubbed duration>, 9 items succeeded, no items with errors, no items with warnings)
                        DONE! (0, <scrubbed duration>)

                        Organizing results...DONE! (0, <scrubbed duration>)
                      DONE! (0, <scrubbed duration>)

                      Reading the most recent offsite snapshot...
                        Reading '{snapshot_destination}{sep}OffsiteBackup.TestBackup.json'...


                        DONE! (0, <scrubbed duration>)
                      DONE! (0, <scrubbed duration>)

                      Calculating diffs...DONE! (0, <scrubbed duration>, no diffs found)

                      Processing file content...
                        Processing 1 item...DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                        Staging working content...
                          Processing '<Folder0>' (1 of 1)...DONE! (0, <scrubbed duration>, 10 instructions added)
                        DONE! (0, <scrubbed duration>)
                      DONE! (0, <scrubbed duration>, 10 instructions found)

                      Processing instructions...

                        Processing '<Folder0>' (1 of 1)...

                          Operation  Local Location{restored_destination_whitespace_delta}                                                                                                                                                                                                            Original Location
                          ---------  {restored_destination_sep_delta}------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------  {working_dir_sep_delta}------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                            [ADD]    <restored dir>{sep}EmptyDirTest{sep}EmptyDir                                                                                                                                                                                                    {working_dir}/EmptyDirTest/EmptyDir
                            [ADD]    <restored dir>{sep}VeryLongPaths{sep}11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111   {working_dir}/VeryLongPaths/11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
                            [ADD]    <restored dir>{sep}VeryLongPaths{sep}222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222  {working_dir}/VeryLongPaths/222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222
                            [ADD]    <restored dir>{sep}one{sep}A                                                                                                                                                                                                                    {working_dir}/one/A
                            [ADD]    <restored dir>{sep}one{sep}BC                                                                                                                                                                                                                   {working_dir}/one/BC
                            [ADD]    <restored dir>{sep}two{sep}Dir1{sep}File3                                                                                                                                                                                                           {working_dir}/two/Dir1/File3
                            [ADD]    <restored dir>{sep}two{sep}Dir1{sep}File4                                                                                                                                                                                                           {working_dir}/two/Dir1/File4
                            [ADD]    <restored dir>{sep}two{sep}Dir2{sep}Dir3{sep}File5                                                                                                                                                                                                      {working_dir}/two/Dir2/Dir3/File5
                            [ADD]    <restored dir>{sep}two{sep}File1                                                                                                                                                                                                                {working_dir}/two/File1
                            [ADD]    <restored dir>{sep}two{sep}File2                                                                                                                                                                                                                {working_dir}/two/File2

                          Restoring the directory '<restored dir>{sep}EmptyDirTest{sep}EmptyDir' (1 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}VeryLongPaths{sep}11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111' (2 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}VeryLongPaths{sep}222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222' (3 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}one{sep}A' (4 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}one{sep}BC' (5 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}two{sep}Dir1{sep}File3' (6 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}two{sep}Dir1{sep}File4' (7 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}two{sep}Dir2{sep}Dir3{sep}File5' (8 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}two{sep}File1' (9 of 10)...DONE! (0, <scrubbed duration>)
                          Restoring the file '<restored dir>{sep}two{sep}File2' (10 of 10)...DONE! (0, <scrubbed duration>)

                        DONE! (0, <scrubbed duration>)

                        Committing content...DONE! (0, <scrubbed duration>)
                      DONE! (0, <scrubbed duration>)
                    DONE! (0, <scrubbed duration>)
                    """,
                ).format(
                    restored_destination_sep_delta="-" * result.destination_dir_length,
                    restored_destination_whitespace_delta=" " * result.destination_dir_length,
                    snapshot_destination=snapshot_destination,
                    working_dir=_working_dir.as_posix(),
                    working_dir_sep_delta="-" * len(str(_working_dir)),
                    sep=os.path.sep,
                )

            # Add 2 files
            new_file_1 = _working_dir / "one" / "NewFile1"
            new_file_2 = _working_dir / "two" / "NewDir1" / "NewDir2" / "NewFile2"

            with new_file_1.open("w") as f:
                f.write("New file 1")

            new_file_2.parent.mkdir(parents=True, exist_ok=True)
            with new_file_2.open("w") as f:
                f.write("New file 2")

            BackupAndRestore(12)

            # Add a file to what was an empty dir; the file count should remain the same because the
            # empty dir won't be listed but the new file will be listed.
            new_file_3 = _working_dir / "EmptyDirTest" / "EmptyDir" / "NewFile3"
            with new_file_3.open("w") as f:
                f.write("New file 3")

            BackupAndRestore(12)

            # Modify 3 files (2 new, 1 original (although, it shouldn't matter if a file is original or not))
            new_file_1_size = new_file_1.stat().st_size
            with new_file_1.open("w") as f:
                f.write("_" * new_file_1_size)

            new_file_3_size = new_file_3.stat().st_size
            with new_file_3.open("w") as f:
                f.write("*" * new_file_3_size)

            original_filename = _working_dir / "one" / "BC"
            original_filename_size = original_filename.stat().st_size

            with original_filename.open("w") as f:
                f.write("_" * original_filename_size)

            # The file sizes are the same, so we shouldn't see a difference when not comparing the contents
            BackupAndRestore(12)

            # Remove a file
            new_file_1.unlink()
            BackupAndRestore(11)

            # Remove a directory
            PathEx.RemoveTree(_working_dir / "two" / "Dir1")
            BackupAndRestore(9)

            # Change a file to an empty dir
            file_to_dir_1 = _working_dir / "one" / "A"

            file_to_dir_1.unlink()
            file_to_dir_1.mkdir()

            BackupAndRestore(9)

            # Change a file to a dir with contents
            file_to_dir_2 = _working_dir / "two" / "File2"

            file_to_dir_2.unlink()
            file_to_dir_2.mkdir()

            with (file_to_dir_2 / "NewFile4").open("w") as f:
                f.write("New file 4")

            with (file_to_dir_2 / "NewFile5").open("w") as f:
                f.write("New file 5")

            with (file_to_dir_2 / "NewFile6").open("w") as f:
                f.write("New file 6")

            BackupAndRestore(11)


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
def _MakeFile(
    root: Path,
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w") as f:
        f.write(PathEx.CreateRelativePath(root, path).as_posix())


# ----------------------------------------------------------------------
@pytest.fixture()
def _working_dir(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("source")

    _MakeFile(root, root / "one" / "A")
    _MakeFile(root, root / "one" / "BC")

    _MakeFile(root, root / "two" / "File1")
    _MakeFile(root, root / "two" / "File2")
    _MakeFile(root, root / "two" / "Dir1" / "File3")
    _MakeFile(root, root / "two" / "Dir1" / "File4")
    _MakeFile(root, root / "two" / "Dir2" / "Dir3" / "File5")

    _MakeFile(root, root / "VeryLongPaths" / ("1" * 200))
    _MakeFile(root, root / "VeryLongPaths" / ("2" * 201))

    (root / "EmptyDirTest" / "EmptyDir").mkdir(parents=True)

    return root
