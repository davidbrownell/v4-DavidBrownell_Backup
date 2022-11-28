# ----------------------------------------------------------------------
# |
# |  Mirror_IntegrationTest.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-21 07:44:24
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Local integration tests for ../Mirror.py"""

import sys

from io import StringIO
from pathlib import Path

import pytest


from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx
from Common_Foundation.Streams.DoneManager import DoneManager


# ----------------------------------------------------------------------
sys.path.insert(0, str(PathEx.EnsureDir(Path(__file__).parent.parent.parent.parent)))
with ExitStack(lambda: sys.path.pop(0)):
        from Backup.Impl.Mirror import Backup, CONTENT_DIR_NAME
        from Backup.Impl import TestHelpers


# TODO: Add tests that ensure content can be reverted after any stage

# ----------------------------------------------------------------------
class TestFileSystemBackup(object):
    # ----------------------------------------------------------------------
    def test(self, tmp_path_factory, _working_dir):
        destination = tmp_path_factory.mktemp("destination")

        backup_func = lambda **kwargs: self.__class__._Backup(_working_dir, destination, **kwargs)  # pylint: disable=protected-access

        # Before Backup
        with pytest.raises(AssertionError):
            TestHelpers.CompareFileSystemSourceAndDestination(
                _working_dir,
                destination,
                20,
            )

        # After initial Backup
        backup_func()
        TestHelpers.CompareFileSystemSourceAndDestination(
            _working_dir,
            destination,
            10,
            compare_file_contents=True,
        )

        # Add 2 files
        new_file_1 = _working_dir / "one" / "NewFile1"
        new_file_2 = _working_dir / "two" / "NewDir1" / "NewDir2" / "NewFile2"

        with new_file_1.open("w") as f:
            f.write("New file 1")

        new_file_2.parent.mkdir(parents=True, exist_ok=True)
        with new_file_2.open("w") as f:
            f.write("New file 2")

        backup_func()
        TestHelpers.CompareFileSystemSourceAndDestination(
            _working_dir,
            destination,
            12,
            compare_file_contents=True,
        )

        # Add a file to what was an empty dir; the file count should remain the same because the
        # empty dir won't be listed but the new file will be listed.
        new_file_3 = _working_dir / "EmptyDirTest" / "EmptyDir" / "NewFile3"
        with new_file_3.open("w") as f:
            f.write("New file 3")

        backup_func()
        TestHelpers.CompareFileSystemSourceAndDestination(
            _working_dir,
            destination,
            12,
            compare_file_contents=True,
        )

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
        TestHelpers.CompareFileSystemSourceAndDestination(
            _working_dir,
            destination,
            12,
            compare_file_contents=False,
        )

        # We should see a difference when comparing the contents
        with pytest.raises(AssertionError):
            TestHelpers.CompareFileSystemSourceAndDestination(
                _working_dir,
                destination,
                12,
                compare_file_contents=True,
            )

        # Backup and compare
        backup_func()
        TestHelpers.CompareFileSystemSourceAndDestination(
            _working_dir,
            destination,
            12,
            compare_file_contents=True,
        )

        # Remove a file
        new_file_1.unlink()
        with pytest.raises(AssertionError):
            TestHelpers.CompareFileSystemSourceAndDestination(
                _working_dir,
                destination,
                11,
                compare_file_contents=True,
            )

        backup_func()
        TestHelpers.CompareFileSystemSourceAndDestination(
            _working_dir,
            destination,
            11,
            compare_file_contents=True,
        )

        # Remove a directory
        PathEx.RemoveTree(_working_dir / "two" / "Dir1")
        with pytest.raises(AssertionError):
            TestHelpers.CompareFileSystemSourceAndDestination(
                _working_dir,
                destination,
                9,
                compare_file_contents=True,
            )

        backup_func()
        TestHelpers.CompareFileSystemSourceAndDestination(
            _working_dir,
            destination,
            9,
            compare_file_contents=True,
        )

        # Change a file to an empty dir
        file_to_dir_1 = _working_dir / "one" / "A"

        file_to_dir_1.unlink()
        file_to_dir_1.mkdir()

        with pytest.raises(AssertionError):
            TestHelpers.CompareFileSystemSourceAndDestination(
                _working_dir,
                destination,
                9,
                compare_file_contents=True,
            )

        backup_func()
        TestHelpers.CompareFileSystemSourceAndDestination(
            _working_dir,
            destination,
            9,
            compare_file_contents=True,
        )

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

        with pytest.raises(AssertionError):
            TestHelpers.CompareFileSystemSourceAndDestination(
                _working_dir,
                destination,
                9,
                compare_file_contents=True,
            )

        backup_func()

        TestHelpers.CompareFileSystemSourceAndDestination(
            _working_dir,
            destination,
            11,
            compare_file_contents=True,
        )

        # Remove file from mirror
        destination_content_dir = TestHelpers.GetOutputPath(
            destination / CONTENT_DIR_NAME,
            _working_dir,
        )

        PathEx.RemoveTree(PathEx.EnsureDir(destination_content_dir / "two" / "File2"))

        with pytest.raises(AssertionError):
            TestHelpers.CompareFileSystemSourceAndDestination(
                _working_dir,
                destination,
                11,
                compare_file_contents=True,
            )

        backup_func()

        # After backup, we will still see the error because the backup is using the mirror's snapshot
        # data to determine the content that needs to change.
        with pytest.raises(AssertionError):
            TestHelpers.CompareFileSystemSourceAndDestination(
                _working_dir,
                destination,
                11,
                compare_file_contents=True,
            )

        backup_func(force=True)

        TestHelpers.CompareFileSystemSourceAndDestination(
            _working_dir,
            destination,
            11,
            compare_file_contents=True,
        )

    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    @staticmethod
    def _Backup(
        working_dir: Path,
        destination: Path,
        *,
        force: bool=False,
    ) -> None:
        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                str(destination),
                [working_dir],
                ssd=False,
                force=force,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )



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
def _working_dir(tmp_path_factory):
    root = tmp_path_factory.mktemp("root")

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
