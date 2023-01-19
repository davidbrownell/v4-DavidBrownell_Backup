# ----------------------------------------------------------------------
# |
# |  Offsite_UnitTest.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-12-07 08:33:57
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022-23
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Unit tests for Offsite.py"""

import os
import sys
import textwrap
import uuid

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import cast, Iterator, List, Optional
from unittest import mock

import pytest

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx
from Common_Foundation.Shell.All import CurrentShell
from Common_Foundation.Streams.DoneManager import DoneManager
from Common_Foundation.TestHelpers.StreamTestHelpers import GenerateDoneManagerAndSink


# ----------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
with ExitStack(lambda: sys.path.pop(0)):
    from Backup.Impl.DataStores.FileSystemDataStore import FileSystemDataStore
    from Backup.Impl.Offsite import Backup, Commit, Restore, SnapshotFilenames
    from Backup.Impl import TestHelpers


# ----------------------------------------------------------------------
def test_SnapshotFilenames(tmp_path_factory):
    snapshot_destination = tmp_path_factory.mktemp("snapshot_destination")

    with mock.patch(
            "{}.{}.user_directory".format(CurrentShell.__module__, type(CurrentShell).__qualname__),
            new_callable=mock.PropertyMock(return_value=snapshot_destination),
        ):
            name = str(uuid.uuid4()).replace("-", "").lower()

            snapshot_filenames = SnapshotFilenames.Create(name)

            assert snapshot_filenames.backup_name == name
            assert snapshot_filenames.standard == snapshot_destination / "OffsiteBackup.{}.json".format(name)
            assert snapshot_filenames.pending == snapshot_destination / "OffsiteBackup.{}.__pending__.json".format(name)


# ----------------------------------------------------------------------
class TestBackup(object):
    # ----------------------------------------------------------------------
    def test_InvalidInput(self):
        with pytest.raises(Exception, match="'foo' is not a valid filename or directory"):
            Backup(
                mock.MagicMock(),
                [Path("foo"), ],
                "Backup",
                None,
                None,
                Path(),
                compress=False,
                ssd=False,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_Standard(self, _working_dir, tmp_path_factory, compress, encryption_password):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as helper:
            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_NoChanges(self, _working_dir, tmp_path_factory, compress, encryption_password):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as helper:
            # No changes

            helper.ExecuteBackup(_working_dir, compress, encryption_password)

            result = helper.GetBackupInfo()

            assert len(result.primary_dirs) == 1
            assert len(result.delta_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_AddSingleFile(self, _working_dir, tmp_path_factory, compress, encryption_password):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as helper:
            with (_working_dir / "New File").open("w") as f:
                f.write("New File")

            helper.ExecuteBackup(_working_dir, compress, encryption_password)

            result = helper.GetBackupInfo()

            assert len(result.primary_dirs) == 1
            assert len(result.delta_dirs) == 1

            backup_item_info = _PathInfo.Create(result.delta_dirs[0])

            if not compress and encryption_password is None:
                assert len(backup_item_info.filenames) == 3
            else:
                assert len(backup_item_info.filenames) == 1

            assert len(backup_item_info.empty_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_AddMultipleFiles(self, _working_dir, tmp_path_factory, compress, encryption_password):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as helper:
            with (_working_dir / "New File 1").open("w") as f:
                f.write("New File 1")

            with (_working_dir / "New File 2").open("w") as f:
                f.write("New File 2")

            helper.ExecuteBackup(_working_dir, compress, encryption_password)

            result = helper.GetBackupInfo()

            assert len(result.primary_dirs) == 1
            assert len(result.delta_dirs) == 1

            backup_item_info = _PathInfo.Create(result.delta_dirs[0])

            if not compress and encryption_password is None:
                assert len(backup_item_info.filenames) == 4
            else:
                assert len(backup_item_info.filenames) == 1

            assert len(backup_item_info.empty_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_AddMultipleFilesSameContent(self, _working_dir, tmp_path_factory, compress, encryption_password):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as helper:
            with (_working_dir / "New File 1").open("w") as f:
                f.write("New File")

            with (_working_dir / "New File 2").open("w") as f:
                f.write("New File")

            helper.ExecuteBackup(_working_dir, compress, encryption_password)

            result = helper.GetBackupInfo()

            assert len(result.primary_dirs) == 1
            assert len(result.delta_dirs) == 1

            backup_item_info = _PathInfo.Create(result.delta_dirs[0])

            if not compress and encryption_password is None:
                assert len(backup_item_info.filenames) == 3
            else:
                assert len(backup_item_info.filenames) == 1

            assert len(backup_item_info.empty_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_AddDir(self, _working_dir, tmp_path_factory, compress, encryption_password):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as helper:
            (_working_dir / "New Directory 1").mkdir()

            helper.ExecuteBackup(_working_dir, compress, encryption_password)

            result = helper.GetBackupInfo()

            assert len(result.primary_dirs) == 1
            assert len(result.delta_dirs) == 1

            backup_item_info = _PathInfo.Create(result.delta_dirs[0])

            if not compress and encryption_password is None:
                # index and index hash
                assert len(backup_item_info.filenames) == 2
            else:
                assert len(backup_item_info.filenames) == 1

            assert len(backup_item_info.empty_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_AddMultipleDirs(self, _working_dir, tmp_path_factory, compress, encryption_password):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as helper:
            (_working_dir / "New Directory 1").mkdir()
            (_working_dir / "New Directory 2").mkdir()

            helper.ExecuteBackup(_working_dir, compress, encryption_password)

            result = helper.GetBackupInfo()

            assert len(result.primary_dirs) == 1
            assert len(result.delta_dirs) == 1

            backup_item_info = _PathInfo.Create(result.delta_dirs[0])

            if not compress and encryption_password is None:
                # index and index hash
                assert len(backup_item_info.filenames) == 2
            else:
                assert len(backup_item_info.filenames) == 1

            assert len(backup_item_info.empty_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_RemoveFile(self, _working_dir, tmp_path_factory, compress, encryption_password):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as helper:
            PathEx.RemoveFile(_working_dir / "one" / "A")

            helper.ExecuteBackup(_working_dir, compress, encryption_password)

            result = helper.GetBackupInfo()

            assert len(result.primary_dirs) == 1
            assert len(result.delta_dirs) == 1

            backup_item_info = _PathInfo.Create(result.delta_dirs[0])

            if not compress and encryption_password is None:
                # index and index hash
                assert len(backup_item_info.filenames) == 2
            else:
                assert len(backup_item_info.filenames) == 1

            assert len(backup_item_info.empty_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_RemoveMultipleFile(self, _working_dir, tmp_path_factory, compress, encryption_password):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as helper:
            PathEx.RemoveFile(_working_dir / "one" / "A")
            PathEx.RemoveFile(_working_dir / "two" / "Dir1" / "File3")

            helper.ExecuteBackup(_working_dir, compress, encryption_password)

            result = helper.GetBackupInfo()

            assert len(result.primary_dirs) == 1
            assert len(result.delta_dirs) == 1

            backup_item_info = _PathInfo.Create(result.delta_dirs[0])

            if not compress and encryption_password is None:
                # index and index hash
                assert len(backup_item_info.filenames) == 2
            else:
                assert len(backup_item_info.filenames) == 1

            assert len(backup_item_info.empty_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_RemoveDir(self, _working_dir, tmp_path_factory, compress, encryption_password):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as helper:
            PathEx.RemoveTree(_working_dir / "one")

            helper.ExecuteBackup(_working_dir, compress, encryption_password)

            result = helper.GetBackupInfo()

            assert len(result.primary_dirs) == 1
            assert len(result.delta_dirs) == 1

            backup_item_info = _PathInfo.Create(result.delta_dirs[0])

            if not compress and encryption_password is None:
                # index and index hash
                assert len(backup_item_info.filenames) == 2
            else:
                assert len(backup_item_info.filenames) == 1

            assert len(backup_item_info.empty_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_RemoveMultipleDirs(self, _working_dir, tmp_path_factory, compress, encryption_password):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as helper:
            PathEx.RemoveTree(_working_dir / "one")
            PathEx.RemoveTree(_working_dir / "two" / "Dir2")

            helper.ExecuteBackup(_working_dir, compress, encryption_password)

            result = helper.GetBackupInfo()

            assert len(result.primary_dirs) == 1
            assert len(result.delta_dirs) == 1

            backup_item_info = _PathInfo.Create(result.delta_dirs[0])

            if not compress and encryption_password is None:
                # index and index hash
                assert len(backup_item_info.filenames) == 2
            else:
                assert len(backup_item_info.filenames) == 1

            assert len(backup_item_info.empty_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    def test_FileToDir(self, _working_dir, tmp_path_factory):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, False, None) as helper:
            PathEx.RemoveFile(_working_dir / "one" / "A")
            (_working_dir / "one" / "A").mkdir()

            helper.ExecuteBackup(_working_dir, False, None)

            result = helper.GetBackupInfo()

            assert len(result.primary_dirs) == 1
            assert len(result.delta_dirs) == 1

            backup_item_info = _PathInfo.Create(result.delta_dirs[0])

            # index and index hash
            assert len(backup_item_info.filenames) == 2
            assert len(backup_item_info.empty_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    def test_DirToFile(self, _working_dir, tmp_path_factory):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, False, None) as helper:
            PathEx.RemoveTree(_working_dir / "one" / "Dir1")

            with (_working_dir / "one" / "Dir1").open("w") as f:
                f.write("This is a change")

            helper.ExecuteBackup(_working_dir, False, None)

            result = helper.GetBackupInfo()

            assert len(result.primary_dirs) == 1
            assert len(result.delta_dirs) == 1

            backup_item_info = _PathInfo.Create(result.delta_dirs[0])

            # index and index hash and file content
            assert len(backup_item_info.filenames) == 3
            assert len(backup_item_info.empty_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    @pytest.mark.skipif(CurrentShell.family_name != "Windows", reason="This test is running into what I believe to be timing issues associated with the quick turnaround time on Linux and MacOS")
    def test_MultipleChanges(self, _working_dir, tmp_path_factory):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, False, None) as helper:
            num_deltas = 3
            num_new_files = 0

            for backup_ctr in range(num_deltas):
                for file_ctr in range(backup_ctr + 1):
                    with (_working_dir / "NewFile-MultipleChanges-{}-{}.txt".format(backup_ctr, file_ctr)).open("w") as f:
                        f.write("{}-{}\n{}\n".format(backup_ctr, file_ctr, uuid.uuid4()))

                num_new_files += (backup_ctr + 1)

                helper.ExecuteBackup(_working_dir, False, None)

                backup_info = helper.GetBackupInfo()

                assert len(backup_info.primary_dirs) == 1
                assert len(backup_info.delta_dirs) == backup_ctr + 1

                backup_item_info = _PathInfo.Create(backup_info.delta_dirs[-1])

                # Index and index hash + number of files written
                assert len(backup_item_info.filenames) == 2 + backup_ctr + 1
                assert len(backup_item_info.empty_dirs) == 0

                assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

            # Force a backup
            helper.ExecuteBackup(_working_dir, False, None, force=True)

            backup_info = helper.GetBackupInfo()

            assert len(backup_info.primary_dirs) == 2
            assert len(backup_info.delta_dirs) == num_deltas

            # Original backup
            backup_item_info = _PathInfo.Create(backup_info.primary_dirs[0])

            assert len(backup_item_info.filenames) == 11
            assert len(backup_item_info.empty_dirs) == 0

            # Latests backup
            backup_item_info = _PathInfo.Create(backup_info.primary_dirs[1])

            assert len(backup_item_info.filenames) == 11 + num_new_files
            assert len(backup_item_info.empty_dirs) == 0

            assert len(_PathInfo.Create(helper.snapshot_dir).filenames) == 1

    # ----------------------------------------------------------------------
    @pytest.mark.skipif(CurrentShell.family_name != "Windows", reason="This test is running into what I believe to be timing issues associated with the quick turnaround time on Linux and MacOS")
    def test_NoDestination(self, _working_dir, tmp_path_factory):
        with _YieldBackupHelper(tmp_path_factory) as helper:
            output = helper.ExecuteBackup(_working_dir, False, None, provide_destination=False)

            assert output == textwrap.dedent(
                """\
                Heading...
                  Creating the local snapshot...
                    Discovering files...
                      Processing (1 item)...DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>, 9 files found, 1 empty directory found)

                    Calculating hashes...
                      Processing (9 items)...DONE! (0, <scrubbed duration>, 9 items succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>)

                    Organizing results...DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>)

                  Calculating diffs...DONE! (0, <scrubbed duration>, 10 diffs found)

                  Preparing file content...
                    Validating size requirements...DONE! (0, <scrubbed duration>, <scrubbed space required>, <scrubbed space available>)

                    Preserving files...
                      Processing (9 items)...DONE! (0, <scrubbed duration>, 9 items succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>)

                    Preserving index...DONE! (0, <scrubbed duration>)

                  DONE! (0, <scrubbed duration>)

                  Preserving the pending snapshot...
                    Writing '{snapshot_dir}{sep}OffsiteBackup.BackupTest.__pending__.json'...DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>)



                  INFO: Content has been written to '{backup_working_dir}{sep}<Folder0>',
                        however the changes have not been committed yet.

                        After the generated content is transferred to an offsite location, run this script
                        again with the 'commit' command using the backup name 'BackupTest' to ensure that
                        these changes are not processed when this offsite backup is run again.


                DONE! (0, <scrubbed duration>)
                """,
            ).format(
                snapshot_dir=helper.snapshot_dir,
                backup_working_dir=helper.backup_working_dir,
                sep=os.path.sep,
            )

            snapshot_filenames: List[Path] = [item for item in helper.snapshot_dir.iterdir() if item.is_file()]

            assert len(snapshot_filenames) == 1
            assert snapshot_filenames[0].stem.endswith("__pending__")

            # Backup w/pending
            output = helper.ExecuteBackup(_working_dir, False, None)

            assert output == textwrap.dedent(
                """\
                Heading...

                  ERROR: A pending snapshot exists for the backup '{}'; this snapshot should be committed before creating updates
                         to the backup.

                         To commit the pending snapshot, run this script with the 'commit' command.

                         To ignore this error and delete the pending snapshot, run this script with the '--ignore-pending-snapshot'
                         argument.


                DONE! (-1, <scrubbed duration>)
                """,
            ).format(helper.backup_name)

            # With ignore pending snapshot
            helper.ExecuteBackup(_working_dir, False, None, ignore_pending_snapshot=True)

            backup_info = helper.GetBackupInfo()

            assert len(backup_info.primary_dirs) == 1
            assert len(backup_info.delta_dirs) == 0

            backup_item_info = _PathInfo.Create(backup_info.primary_dirs[0])

            assert len(backup_item_info.filenames) == 11
            assert len(backup_item_info.empty_dirs) == 0

            snapshot_filenames: List[Path] = [item for item in helper.snapshot_dir.iterdir() if item.is_file()]

            assert len(snapshot_filenames) == 1
            assert not snapshot_filenames[0].stem.endswith("__pending__")

            # Delta
            (_working_dir / "New Dir").mkdir()

            output = helper.ExecuteBackup(_working_dir, False, None, provide_destination=False)

            assert output == textwrap.dedent(
                """\
                Heading...
                  Creating the local snapshot...
                    Discovering files...
                      Processing (1 item)...DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>, 9 files found, 2 empty directories found)

                    Calculating hashes...
                      Processing (9 items)...DONE! (0, <scrubbed duration>, 9 items succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>)

                    Organizing results...DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>)

                  Reading the most recent offsite snapshot...
                    Reading '{snapshot_dir}{sep}OffsiteBackup.BackupTest.json'...


                    DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>)

                  Calculating diffs...DONE! (0, <scrubbed duration>, 1 diff found)

                  Preparing file content...

                    Preserving index...DONE! (0, <scrubbed duration>)

                  DONE! (0, <scrubbed duration>)

                  Preserving the pending snapshot...
                    Writing '{snapshot_dir}{sep}OffsiteBackup.BackupTest.__pending__.json'...DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>)



                  INFO: Content has been written to '{backup_working_dir}{sep}<Folder0>',
                        however the changes have not been committed yet.

                        After the generated content is transferred to an offsite location, run this script
                        again with the 'commit' command using the backup name 'BackupTest' to ensure that
                        these changes are not processed when this offsite backup is run again.


                DONE! (0, <scrubbed duration>)
                """,
            ).format(
                backup_working_dir=helper.backup_working_dir,
                snapshot_dir=helper.snapshot_dir,
                sep=os.path.sep,
            )

            snapshot_filenames: List[Path] = [item for item in helper.snapshot_dir.iterdir() if item.is_file()]

            assert len(snapshot_filenames) == 2
            assert not snapshot_filenames[0].stem.endswith("__pending__")
            assert snapshot_filenames[1].stem.endswith("__pending__")

            # Backup w/pending
            output = helper.ExecuteBackup(_working_dir, False, None)

            assert output == textwrap.dedent(
                """\
                Heading...

                  ERROR: A pending snapshot exists for the backup 'BackupTest'; this snapshot should be committed before creating updates
                         to the backup.

                         To commit the pending snapshot, run this script with the 'commit' command.

                         To ignore this error and delete the pending snapshot, run this script with the '--ignore-pending-snapshot'
                         argument.


                DONE! (-1, <scrubbed duration>)
                """,
            )

            # With ignore pending snapshot
            helper.ExecuteBackup(_working_dir, False, None, ignore_pending_snapshot=True)

            backup_info = helper.GetBackupInfo()

            assert len(backup_info.primary_dirs) == 1
            assert len(backup_info.delta_dirs) == 1

            backup_item_info = _PathInfo.Create(backup_info.primary_dirs[0])

            assert len(backup_item_info.filenames) == 11
            assert len(backup_item_info.empty_dirs) == 0

            snapshot_filenames: List[Path] = [item for item in helper.snapshot_dir.iterdir() if item.is_file()]

            assert len(snapshot_filenames) == 1
            assert not snapshot_filenames[0].stem.endswith("__pending__")

    # ----------------------------------------------------------------------
    def test_InvalidIgnorePending(self, _working_dir, tmp_path_factory):
        with _YieldBackupHelper(tmp_path_factory) as helper:
            output = helper.ExecuteBackup(_working_dir, False, None, ignore_pending_snapshot=True)

            assert output == textwrap.dedent(
                """\
                Heading...
                  ERROR: A pending snapshot for 'BackupTest' was not found.
                DONE! (-1, <scrubbed duration>)
                """,
            )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("result", [-1, 1])
    def test_UncleanExit(self, _working_dir, tmp_path_factory, result):
        with _YieldBackupHelper(tmp_path_factory) as helper:
            dm_and_sink = iter(GenerateDoneManagerAndSink())

            dm = cast(DoneManager, next(dm_and_sink))

            # ----------------------------------------------------------------------
            def NewValidateSizeRequriements(
                dm: DoneManager,
                *args,
                **kwargs,
            ):
                dm.result = result

            # ----------------------------------------------------------------------

            with mock.patch("Backup.Impl.Common.ValidateSizeRequirements", side_effect=NewValidateSizeRequriements):
                Backup(
                    dm,
                    [_working_dir],
                    helper.backup_name,
                    str(helper.output_dir),
                    encryption_password=None,
                    working_dir=helper.backup_working_dir,
                    compress=False,
                    ssd=False,
                    force=False,
                    quiet=False,
                    file_includes=None,
                    file_excludes=None,
                )

                sink = TestHelpers.OutputScrubber().Replace(cast(str, next(dm_and_sink)))

                if result == -1:
                    desc = "errors"
                elif result == 1:
                    desc = "warnings"
                else:
                    assert False, result  # pragma: no cover

                assert sink == textwrap.dedent(
                    """\
                    Heading...
                      Creating the local snapshot...
                        Discovering files...
                          Processing (1 item)...DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                        DONE! (0, <scrubbed duration>, 9 files found, 1 empty directory found)

                        Calculating hashes...
                          Processing (9 items)...DONE! (0, <scrubbed duration>, 9 items succeeded, no items with errors, no items with warnings)
                        DONE! (0, <scrubbed duration>)

                        Organizing results...DONE! (0, <scrubbed duration>)
                      DONE! (0, <scrubbed duration>)

                      Calculating diffs...DONE! (0, <scrubbed duration>, 10 diffs found)

                      Preparing file content...DONE! ({result}, <scrubbed duration>)


                      INFO: The temporary directory '{backup_working_dir}{sep}<Folder0>' was preserved due to {desc}.
                    DONE! ({result}, <scrubbed duration>)
                    """,
                ).format(
                    backup_working_dir=helper.backup_working_dir,
                    result=result,
                    desc=desc,
                    sep=os.path.sep,
                )


# ----------------------------------------------------------------------
class TestCommit(object):
    # ----------------------------------------------------------------------
    def test_CommitNothingAvailable(self, tmp_path_factory):
        with _YieldBackupHelper(tmp_path_factory) as helper:
            dm_and_sink = iter(GenerateDoneManagerAndSink())

            Commit(
                cast(DoneManager, next(dm_and_sink)),
                helper.backup_name,
            )

            output = cast(str, next(dm_and_sink))

            assert output == textwrap.dedent(
                """\
                Heading...
                  ERROR: A pending snapshot for the backup 'BackupTest' was not found.
                DONE! (-1, <scrubbed duration>)
                """,
            )

    # ----------------------------------------------------------------------
    def test_Standard(self, _working_dir, tmp_path_factory):
        with _YieldBackupHelper(tmp_path_factory) as helper:
            helper.ExecuteBackup(_working_dir, False, None, provide_destination=False)

            snapshot_filenames: List[Path] = [item for item in helper.snapshot_dir.iterdir() if item.is_file()]

            assert len(snapshot_filenames) == 1
            assert snapshot_filenames[0].stem.endswith("__pending__")

            # Commit
            dm_and_sink = iter(GenerateDoneManagerAndSink())

            Commit(cast(DoneManager, next(dm_and_sink)), helper.backup_name)

            snapshot_filenames: List[Path] = [item for item in helper.snapshot_dir.iterdir() if item.is_file()]

            assert len(snapshot_filenames) == 1
            assert not snapshot_filenames[0].stem.endswith("__pending__")


# ----------------------------------------------------------------------
class TestRestore(object):
    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("is_local_filesystem", [True, False])
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_RestoreSingleBackup(self, _working_dir, tmp_path_factory, compress, encryption_password, is_local_filesystem):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as backup_helper:
            restore_helper = _RestoreHelper.Create(
                _working_dir,
                tmp_path_factory,
                encryption_password,
                is_local_filesystem,
                backup_helper.backup_name,
                backup_helper.output_dir,
            )

            restore_helper.ExecuteRestore(
                10,
                overwrite=False,
            )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("is_local_filesystem", [True, False])
    @pytest.mark.parametrize("encryption_password", [None, str(uuid.uuid4())])
    @pytest.mark.parametrize("compress", [False, True])
    def test_RestoreMultipleBackups(self, _working_dir, tmp_path_factory, compress, encryption_password, is_local_filesystem):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, compress, encryption_password) as backup_helper:
            restore_helper = _RestoreHelper.Create(
                _working_dir,
                tmp_path_factory,
                encryption_password,
                is_local_filesystem,
                backup_helper.backup_name,
                backup_helper.output_dir,
            )

            # Add file and dir
            new_file1 = _working_dir / "New File 1.txt"
            new_dir1 = _working_dir / "New Dir"

            with new_file1.open("w") as f:
                f.write("This is a new file")

            new_dir1.mkdir()

            backup_helper.ExecuteBackup(_working_dir, compress, encryption_password)
            restore_helper.ExecuteRestore(12)

            # Modify file (1 of N)
            with new_file1.open("w") as f:
                f.write("This is change 1")

            backup_helper.ExecuteBackup(_working_dir, compress, encryption_password)
            restore_helper.ExecuteRestore(
                12,
                overwrite=True,
            )

            # Add new files
            new_file2 = _working_dir / "New file 2.txt"

            with new_file2.open("w") as f:
                f.write("This is a new file 2")

            backup_helper.ExecuteBackup(_working_dir, compress, encryption_password)
            restore_helper.ExecuteRestore(
                13,
                overwrite=True,
            )

            # Modify (2 of N), Remove file and dir
            with new_file1.open("w") as f:
                f.write("This is change 2")

            PathEx.RemoveFile(_working_dir / "one" / "A")
            PathEx.RemoveTree(_working_dir / "two" / "Dir1")

            backup_helper.ExecuteBackup(_working_dir, compress, encryption_password)
            restore_helper.ExecuteRestore(
                10,
                overwrite=True,
            )

            # Change dir to file and file to dir
            empty_dir = _working_dir / "EmptyDirTest" / "EmptyDir"

            PathEx.RemoveTree(empty_dir)

            with empty_dir.open("w") as f:
                f.write("This was a directory")

            file_to_dir = _working_dir / "one" / "BC"

            PathEx.RemoveItem(file_to_dir)
            file_to_dir.mkdir()

            file_to_dir_with_files = _working_dir / "two" / "Dir2" / "Dir3" / "File5"

            PathEx.RemoveFile(file_to_dir_with_files)
            file_to_dir_with_files.mkdir()

            with (file_to_dir_with_files / "Another New File 1").open("w") as f:
                f.write("Content1")

            with (file_to_dir_with_files / "Another New File 2").open("w") as f:
                f.write("Content2")

            backup_helper.ExecuteBackup(_working_dir, compress, encryption_password)
            restore_helper.ExecuteRestore(
                11,
                overwrite=True,
            )

    # ----------------------------------------------------------------------
    def test_OverwriteError(self, _working_dir, tmp_path_factory):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, False, None) as backup_helper:
            restore_helper = _RestoreHelper.Create(
                _working_dir,
                tmp_path_factory,
                None,
                None,
                backup_helper.backup_name,
                backup_helper.output_dir,
            )

            output = restore_helper.ExecuteRestore(
                None,
                expected_result=-1,
                decorate_restored_files=False,
            )

            assert output == textwrap.dedent(
                """\
                Heading...
                  Processing file content...
                    Processing (1 item)...DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                    Staging working content...
                      Processing '<Folder0>' (1 of 1)...DONE! (0, <scrubbed duration>, 10 instructions added)
                    DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>, 10 instructions found)

                  Processing instructions...

                    Processing '<Folder0>' (1 of 1)...

                      Operation  Local Location{working_dir_whitespace_delta}                                                                                                                                                                                                            Original Location
                      ---------  {working_dir_sep_delta}------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------  {restore_dir_sep_delta}------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                        [ADD]    {working_dir}{sep}EmptyDirTest{sep}EmptyDir                                                                                                                                                                                                    {restore_dir}/EmptyDirTest/EmptyDir
                        [ADD]    {working_dir}{sep}VeryLongPaths{sep}11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111   {restore_dir}/VeryLongPaths/11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
                        [ADD]    {working_dir}{sep}VeryLongPaths{sep}222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222  {restore_dir}/VeryLongPaths/222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222
                        [ADD]    {working_dir}{sep}one{sep}A                                                                                                                                                                                                                    {restore_dir}/one/A
                        [ADD]    {working_dir}{sep}one{sep}BC                                                                                                                                                                                                                   {restore_dir}/one/BC
                        [ADD]    {working_dir}{sep}two{sep}Dir1{sep}File3                                                                                                                                                                                                           {restore_dir}/two/Dir1/File3
                        [ADD]    {working_dir}{sep}two{sep}Dir1{sep}File4                                                                                                                                                                                                           {restore_dir}/two/Dir1/File4
                        [ADD]    {working_dir}{sep}two{sep}Dir2{sep}Dir3{sep}File5                                                                                                                                                                                                      {restore_dir}/two/Dir2/Dir3/File5
                        [ADD]    {working_dir}{sep}two{sep}File1                                                                                                                                                                                                                {restore_dir}/two/File1
                        [ADD]    {working_dir}{sep}two{sep}File2                                                                                                                                                                                                                {restore_dir}/two/File2

                      Restoring the directory '{working_dir}{sep}EmptyDirTest{sep}EmptyDir' (1 of 10)...
                        ERROR: The local item '{working_dir}{sep}EmptyDirTest{sep}EmptyDir' exists and will not be overwritten.
                      DONE! (-1, <scrubbed duration>)

                    DONE! (-1, <scrubbed duration>)

                    Committing content...DONE! (0, <scrubbed duration>)
                  DONE! (-1, <scrubbed duration>)
                DONE! (-1, <scrubbed duration>)
                """,
            ).format(
                working_dir=_working_dir,
                working_dir_sep_delta="-" * len(str(_working_dir)),
                working_dir_whitespace_delta=" " * len(str(_working_dir)),
                restore_dir=_working_dir.as_posix(),
                restore_dir_sep_delta="-" * len(_working_dir.as_posix()),
                sep=os.path.sep,
            )

    # ----------------------------------------------------------------------
    def test_Overwrite(self, _working_dir, tmp_path_factory):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, False, None) as backup_helper:
            # Remove a file to show that things have been restored as expected
            path_info = _PathInfo.Create(_working_dir)

            assert len(path_info.filenames) == 9
            assert len(path_info.empty_dirs) == 1

            PathEx.RemoveFile(_working_dir / "one" / "A")
            PathEx.RemoveTree(_working_dir / "EmptyDirTest")

            path_info = _PathInfo.Create(_working_dir)

            assert len(path_info.filenames) == 8
            assert len(path_info.empty_dirs) == 0

            # Restore w/overwrite
            restore_helper = _RestoreHelper.Create(
                _working_dir,
                tmp_path_factory,
                None,
                None,
                backup_helper.backup_name,
                backup_helper.output_dir,
            )

            restore_helper.ExecuteRestore(
                None,
                decorate_restored_files=False,
                overwrite=True,
            )

            path_info = _PathInfo.Create(_working_dir)

            assert len(path_info.filenames) == 9
            assert len(path_info.empty_dirs) == 1

    # ----------------------------------------------------------------------
    def test_DryRun(self, _working_dir, tmp_path_factory):
        with _YieldInitializedBackupHelper(tmp_path_factory, _working_dir, False, None) as backup_helper:
            # Remove a file to show that things are not restored
            path_info = _PathInfo.Create(_working_dir)

            assert len(path_info.filenames) == 9
            assert len(path_info.empty_dirs) == 1

            PathEx.RemoveFile(_working_dir / "one" / "A")
            PathEx.RemoveTree(_working_dir / "EmptyDirTest")

            path_info = _PathInfo.Create(_working_dir)

            assert len(path_info.filenames) == 8
            assert len(path_info.empty_dirs) == 0

            # Restore as dry run
            restore_helper = _RestoreHelper.Create(
                _working_dir,
                tmp_path_factory,
                None,
                None,
                backup_helper.backup_name,
                backup_helper.output_dir,
            )

            output = restore_helper.ExecuteRestore(
                None,
                dry_run=True,
                overwrite=True,
                decorate_restored_files=False,
            )

            # Nothing changed
            path_info = _PathInfo.Create(_working_dir)

            assert len(path_info.filenames) == 8
            assert len(path_info.empty_dirs) == 0

            assert output == textwrap.dedent(
                """\
                Heading...
                  Processing file content...
                    Processing (1 item)...DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                    Staging working content...
                      Processing '<Folder0>' (1 of 1)...DONE! (0, <scrubbed duration>, 10 instructions added)
                    DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>, 10 instructions found)

                  Processing instructions...

                    Processing '<Folder0>' (1 of 1)...

                      Operation  Local Location{working_dir_whitespace_delta}                                                                                                                                                                                                            Original Location
                      ---------  {working_dir_sep_delta}------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------  {restore_dir_sep_delta}------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                        [ADD]    {working_dir}{sep}EmptyDirTest{sep}EmptyDir                                                                                                                                                                                                    {restore_dir}/EmptyDirTest/EmptyDir
                        [ADD]    {working_dir}{sep}VeryLongPaths{sep}11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111   {restore_dir}/VeryLongPaths/11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
                        [ADD]    {working_dir}{sep}VeryLongPaths{sep}222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222  {restore_dir}/VeryLongPaths/222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222
                        [ADD]    {working_dir}{sep}one{sep}A                                                                                                                                                                                                                    {restore_dir}/one/A
                        [ADD]    {working_dir}{sep}one{sep}BC                                                                                                                                                                                                                   {restore_dir}/one/BC
                        [ADD]    {working_dir}{sep}two{sep}Dir1{sep}File3                                                                                                                                                                                                           {restore_dir}/two/Dir1/File3
                        [ADD]    {working_dir}{sep}two{sep}Dir1{sep}File4                                                                                                                                                                                                           {restore_dir}/two/Dir1/File4
                        [ADD]    {working_dir}{sep}two{sep}Dir2{sep}Dir3{sep}File5                                                                                                                                                                                                      {restore_dir}/two/Dir2/Dir3/File5
                        [ADD]    {working_dir}{sep}two{sep}File1                                                                                                                                                                                                                {restore_dir}/two/File1
                        [ADD]    {working_dir}{sep}two{sep}File2                                                                                                                                                                                                                {restore_dir}/two/File2

                    DONE! (0, <scrubbed duration>)

                    Committing content...DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>)
                DONE! (0, <scrubbed duration>)
                """,
            ).format(
                working_dir=_working_dir,
                working_dir_sep_delta="-" * len(str(_working_dir)),
                working_dir_whitespace_delta=" " * len(str(_working_dir)),
                restore_dir=_working_dir.as_posix(),
                restore_dir_sep_delta="-" * len(_working_dir.as_posix()),
                sep=os.path.sep,
            )


# ----------------------------------------------------------------------
class TestRestoreErrors(object):
    # ----------------------------------------------------------------------
    def test_InvalidStore(self):
        dm_and_sink = iter(GenerateDoneManagerAndSink())

        Restore(
            cast(DoneManager, next(dm_and_sink)),
            "Test",
            "fast_glacier://account@region",
            None,
            Path(),
            {},
            ssd=False,
            quiet=False,
            dry_run=False,
            overwrite=False,
        )

        output = cast(str, next(dm_and_sink))

        assert output == textwrap.dedent(
            """\
            Heading...
              ERROR: 'fast_glacier://account@region' does not resolve to a file-based data store, which is required when restoring content.

                     Most often, this error is encountered when attempting to restore an offsite backup that was
                     originally transferred to a cloud-based data store.

                     To restore these types of offsite backups, copy the content from the original data store
                     to your local file system and run this script again while pointing to that
                     location on your file system. This local directory should contain the primary directory
                     created during the initial backup and all directories created as a part of subsequent backups.

            DONE! (-1, <scrubbed duration>)
            """,
        )


    # ----------------------------------------------------------------------
    def test_FilesInBackupContent(self, tmp_path_factory):
        temp_dir = tmp_path_factory.mktemp("backup_with_invalid_files") / "Backup"

        temp_dir.mkdir()

        with (temp_dir / "Invalid File").open("w") as f:
            f.write("This will cause an error")

        dm_and_sink = iter(GenerateDoneManagerAndSink())

        Restore(
            cast(DoneManager, next(dm_and_sink)),
            temp_dir.name,
            str(temp_dir.parent),
            None,
            Path(),
            {},
            ssd=False,
            quiet=False,
            dry_run=False,
            overwrite=False,
        )

        output = cast(str, next(dm_and_sink))

        assert output == textwrap.dedent(
            """\
            Heading...
              Processing file content...
                ERROR: Files were not expected:

                           - 'Invalid File'

              DONE! (-1, <scrubbed duration>, no instructions found)
            DONE! (-1, <scrubbed duration>)
            """,
        )

    # ----------------------------------------------------------------------
    def test_InvalidDirectory(self, tmp_path_factory):
        temp_dir = tmp_path_factory.mktemp("backup_with_invalid_dir") / "Backup"

        temp_dir.mkdir()

        (temp_dir / "Invalid Directory").mkdir()

        dm_and_sink = iter(GenerateDoneManagerAndSink())

        Restore(
            cast(DoneManager, next(dm_and_sink)),
            temp_dir.name,
            str(temp_dir.parent),
            None,
            Path(),
            {},
            ssd=False,
            quiet=False,
            dry_run=False,
            overwrite=False,
        )

        output = cast(str, next(dm_and_sink))

        assert output == textwrap.dedent(
            """\
            Heading...
              Processing file content...
                ERROR: 'Invalid Directory' is not a recognized directory name.
              DONE! (-1, <scrubbed duration>, no instructions found)
            DONE! (-1, <scrubbed duration>)
            """,
        )

    # ----------------------------------------------------------------------
    def test_NoDirectories(self, tmp_path_factory):
        temp_dir = tmp_path_factory.mktemp("backup_with_invalid_dir") / "Backup"

        temp_dir.mkdir()

        dm_and_sink = iter(GenerateDoneManagerAndSink())

        Restore(
            cast(DoneManager, next(dm_and_sink)),
            temp_dir.name,
            str(temp_dir.parent),
            None,
            Path(),
            {},
            ssd=False,
            quiet=False,
            dry_run=False,
            overwrite=False,
        )

        output = cast(str, next(dm_and_sink))

        assert output == textwrap.dedent(
            """\
            Heading...
              Processing file content...
                ERROR: No directories were found.
              DONE! (-1, <scrubbed duration>, no instructions found)
            DONE! (-1, <scrubbed duration>)
            """,
        )

    # ----------------------------------------------------------------------
    def test_NoPrimaryDirectories(self, tmp_path_factory):
        temp_dir = tmp_path_factory.mktemp("backup_with_invalid_dir") / "Backup"

        temp_dir.mkdir()

        (temp_dir / "2022.12.07.17.10.00-000000.delta").mkdir()
        (temp_dir / "2022.12.07.17.10.00-000001.delta").mkdir()

        dm_and_sink = iter(GenerateDoneManagerAndSink())

        Restore(
            cast(DoneManager, next(dm_and_sink)),
            temp_dir.name,
            str(temp_dir.parent),
            None,
            Path(),
            {},
            ssd=False,
            quiet=False,
            dry_run=False,
            overwrite=False,
        )

        output = cast(str, next(dm_and_sink))

        assert output == textwrap.dedent(
            """\
            Heading...
              Processing file content...
                ERROR: No primary directories were found.
              DONE! (-1, <scrubbed duration>, no instructions found)
            DONE! (-1, <scrubbed duration>)
            """,
        )

    # ----------------------------------------------------------------------
    def test_MultiplePrimaryDirectories(self, tmp_path_factory):
        temp_dir = tmp_path_factory.mktemp("backup_with_invalid_dir") / "Backup"

        temp_dir.mkdir()

        (temp_dir / "2022.12.07.17.10.00-000000").mkdir()
        (temp_dir / "2022.12.07.17.10.00-000001").mkdir()

        dm_and_sink = iter(GenerateDoneManagerAndSink())

        Restore(
            cast(DoneManager, next(dm_and_sink)),
            temp_dir.name,
            str(temp_dir.parent),
            None,
            Path(),
            {},
            ssd=False,
            quiet=False,
            dry_run=False,
            overwrite=False,
        )

        output = cast(str, next(dm_and_sink))

        assert output == textwrap.dedent(
            """\
            Heading...
              Processing file content...
                ERROR: Multiple primary directories were found.

                       Primary Directories found:

                           - '2022.12.07.17.10.00-000000'
                           - '2022.12.07.17.10.00-000001'

              DONE! (-1, <scrubbed duration>, no instructions found)
            DONE! (-1, <scrubbed duration>)
            """,
        )


# ----------------------------------------------------------------------
# |
# |  Private Types
# |
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class _BackupHelper(object):
    # ----------------------------------------------------------------------
    # |  Public Types
    @dataclass(frozen=True)
    class BackupInfo(object):
        # ----------------------------------------------------------------------
        primary_dirs: List[Path]
        delta_dirs: List[Path]

    # ----------------------------------------------------------------------
    # |  Public Data
    backup_name: str
    output_dir: Path
    snapshot_dir: Path
    backup_working_dir: Path

    # ----------------------------------------------------------------------
    # |  Public Methods
    def ExecuteBackup(
        self,
        _working_dir,
        compress: bool,
        encryption_password: Optional[str],
        *,
        provide_destination: bool=True,
        force: bool=False,
        ignore_pending_snapshot: bool=False,
    ) -> str:
        dm_and_sink = iter(GenerateDoneManagerAndSink())

        Backup(
            cast(DoneManager, next(dm_and_sink)),
            [_working_dir],
            self.backup_name,
            str(self.output_dir) if provide_destination else None,
            encryption_password=encryption_password,
            working_dir=self.backup_working_dir,
            compress=compress,
            ssd=False,
            force=force,
            quiet=False,
            file_includes=None,
            file_excludes=None,
            ignore_pending_snapshot=ignore_pending_snapshot,
        )

        return TestHelpers.OutputScrubber().Replace(cast(str, next(dm_and_sink)))

    # ----------------------------------------------------------------------
    def GetBackupInfo(self) -> "_BackupHelper.BackupInfo":
        backup_dir = self.output_dir / self.backup_name
        assert backup_dir.is_dir(), backup_dir

        primary_dirs: List[Path] = []
        delta_dirs: List[Path] = []

        for item in backup_dir.iterdir():
            assert item.is_dir(), item

            if item.name.endswith(".delta"):
                delta_dirs.append(item)
            else:
                primary_dirs.append(item)

        return _BackupHelper.BackupInfo(primary_dirs, delta_dirs)


# ----------------------------------------------------------------------
@dataclass(frozen=True)
class _RestoreHelper(object):
    # ----------------------------------------------------------------------
    # |  Public Data
    original_dir: Path
    encryption_password: Optional[str]
    is_local_filesystem: Optional[bool]

    backup_name: str
    backup_dir: Path

    output_dir: Path
    restore_working_dir: Path

    # ----------------------------------------------------------------------
    # |  Public Methods
    @classmethod
    def Create(
        cls,
        original_dir: Path,
        tmp_path_factory,
        encryption_password: Optional[str],
        is_local_filesystem: Optional[bool],
        backup_name: str,
        backup_dir: Path,
    ) -> "_RestoreHelper":
        return cls(
            original_dir,
            encryption_password,
            is_local_filesystem,
            backup_name,
            backup_dir,
            tmp_path_factory.mktemp("restore_destination"),
            tmp_path_factory.mktemp("restore_working"),
        )

    # ----------------------------------------------------------------------
    def ExecuteRestore(
        self,
        expected_num_files: Optional[int],
        *,
        expected_result: int=0,
        clear_working_dir: bool=False,
        dry_run: bool=False,
        overwrite: bool=False,
        decorate_restored_files: bool=True,
    ) -> str:
        dm_and_sink = iter(GenerateDoneManagerAndSink())

        if clear_working_dir:
            PathEx.RemoveTree(self.restore_working_dir)
            self.restore_working_dir.mkdir()

        dm = cast(DoneManager, next(dm_and_sink))

        Restore(
            dm,
            self.backup_name,
            "{}{}".format(
                "[nonlocal]" if self.is_local_filesystem is False else "",
                self.backup_dir.as_posix(),
            ),
            self.encryption_password,
            self.restore_working_dir,
            {} if not decorate_restored_files else {
                self.original_dir.as_posix() : self.output_dir.as_posix(),
            },
            ssd=False,
            quiet=False,
            dry_run=dry_run,
            overwrite=overwrite,
        )

        assert dm.result == expected_result

        output = TestHelpers.OutputScrubber().Replace(cast(str, next(dm_and_sink)))

        if expected_num_files is not None:
            TestHelpers.CompareFileSystemSourceAndDestination(
                self.original_dir,
                self.output_dir,
                expected_num_files,
                is_mirror=False,
                compare_file_contents=True,
            )

        return output


# ----------------------------------------------------------------------
@dataclass(frozen=True)
class _PathInfo(object):
    # ----------------------------------------------------------------------
    filenames: List[Path]
    empty_dirs: List[Path]

    # ----------------------------------------------------------------------
    @classmethod
    def Create(
        cls,
        path: Path,
    ) -> "_PathInfo":
        all_files: List[Path] = []
        empty_dirs: List[Path] = []

        for root, directories, filenames in os.walk(path):
            root = Path(root)

            if not directories and not filenames:
                empty_dirs.append(root)

            all_files += [root / filename for filename in filenames]

        return _PathInfo(all_files, empty_dirs)


# ----------------------------------------------------------------------
# |
# |  Private Methods
# |
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


# ----------------------------------------------------------------------
@contextmanager
def _YieldBackupHelper(tmp_path_factory) -> Iterator[_BackupHelper]:
    destination_dir = tmp_path_factory.mktemp("backup_destination")
    snapshot_dir = tmp_path_factory.mktemp("snapshot")
    backup_working_dir = tmp_path_factory.mktemp("backup_working")

    backup_name = "BackupTest"

    with mock.patch(
        "{}.{}.user_directory".format(CurrentShell.__module__, type(CurrentShell).__qualname__),
        new_callable=mock.PropertyMock(return_value=snapshot_dir),
    ):
        yield _BackupHelper(backup_name, destination_dir, snapshot_dir, backup_working_dir)


# ----------------------------------------------------------------------
@contextmanager
def _YieldInitializedBackupHelper(
    tmp_path_factory,
    _working_dir,
    compress,
    encryption_password,
) -> Iterator[_BackupHelper]:
    with _YieldBackupHelper(tmp_path_factory) as helper:
        helper.ExecuteBackup(_working_dir, compress, encryption_password)

        backup_info = helper.GetBackupInfo()

        assert len(backup_info.primary_dirs) == 1
        assert len(backup_info.delta_dirs) == 0

        backup_item_info = _PathInfo.Create(backup_info.primary_dirs[0])

        if not compress and encryption_password is None:
            # The number of original files in 9, but we have added the index file
            # and index file hash file
            assert len(backup_item_info.filenames) == 11

            # The empty dirs are captured in the index file but not explicitly
            # stored
            assert len(backup_item_info.empty_dirs) == 0
        else:
            # All content has been compressed
            assert len(backup_item_info.filenames) == 1
            assert len(backup_item_info.empty_dirs) == 0

        yield helper
