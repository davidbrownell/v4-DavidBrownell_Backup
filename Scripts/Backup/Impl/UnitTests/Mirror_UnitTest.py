# ----------------------------------------------------------------------
# |
# |  Mirror_UnitTest.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-14 11:17:05
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Unit tests for Mirror.py"""

import os
import re
import shutil
import sys
import textwrap

from io import StringIO
from pathlib import Path
from typing import Callable, cast, List, Tuple
from unittest import mock

import pytest

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx
from Common_Foundation.Streams.Capabilities import Capabilities
from Common_Foundation.Streams.DoneManager import DoneManager
from Common_Foundation.TestHelpers.StreamTestHelpers import GenerateDoneManagerAndSink


# ----------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
with ExitStack(lambda: sys.path.pop(0)):
    from Backup.Impl import TestHelpers
    from Backup.Impl.Common import PENDING_COMMIT_EXTENSION, PENDING_DELETE_EXTENSION
    from Backup.Impl.Mirror import Backup, Cleanup, CONTENT_DIR_NAME, Validate, ValidateType


# Note that this exercises based functionality; ../IntegrationTests/Mirror_Test.py exercises
# backups with changes to the file system over time.

# ----------------------------------------------------------------------
class TestFileSystemBackup(object):
    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_Standard(self, tmp_path_factory, _working_dir, run_in_parallel):
        destination = tmp_path_factory.mktemp("destination")

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                [_working_dir],
                ssd=run_in_parallel,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        TestHelpers.CompareFileSystemSourceAndDestination(
            _working_dir,
            destination,
            compare_file_contents=False,
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_SingleFile(self, tmp_path_factory, _working_dir, run_in_parallel):
        destination = tmp_path_factory.mktemp("destination")

        source_dir = _working_dir / "two" / "Dir2" / "Dir3"

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                [
                    source_dir / "File5",
                ],
                ssd=run_in_parallel,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        TestHelpers.CompareFileSystemSourceAndDestination(
            source_dir,
            destination,
            1,
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_SingleDir(self, tmp_path_factory, _working_dir, run_in_parallel):
        destination = tmp_path_factory.mktemp("destination")

        source_dir = _working_dir / "one"

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                [
                    source_dir,
                ],
                ssd=run_in_parallel,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        TestHelpers.CompareFileSystemSourceAndDestination(
            source_dir,
            destination,
            2,
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_SingleFileAndDir(self, tmp_path_factory, _working_dir, run_in_parallel):
        destination = tmp_path_factory.mktemp("destination")

        source_dir = _working_dir / "one"
        source_file = _working_dir / "two" / "Dir2" / "Dir3" / "File5"

        assert source_dir.is_dir()
        assert source_file.is_file()

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                [
                    source_dir,
                    source_file,
                ],
                ssd=run_in_parallel,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        TestHelpers.CompareFileSystemSourceAndDestination(
            [source_dir, source_file],
            destination,
            3,
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_EmptyDir(self, tmp_path_factory, _working_dir, run_in_parallel):
        destination = tmp_path_factory.mktemp("destination")

        source_dir = _working_dir / "EmptyDirTest" / "EmptyDir"

        assert source_dir.is_dir()

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                [
                    source_dir,
                ],
                ssd=run_in_parallel,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        TestHelpers.CompareFileSystemSourceAndDestination(
            source_dir,
            destination,
            1,
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_MultpleDirs(self, tmp_path_factory, _working_dir, run_in_parallel):
        destination = tmp_path_factory.mktemp("destination")

        source_dirs: List[Path] = [
            _working_dir / "one",
            _working_dir / "two",
            _working_dir / "EmptyDirTest",
        ]

        assert all(source_dir.is_dir() for source_dir in source_dirs)

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                source_dirs,
                ssd=run_in_parallel,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        TestHelpers.CompareFileSystemSourceAndDestination(
            source_dirs,
            destination,
            8,
        )

    # ----------------------------------------------------------------------
    def test_FilterInclude(self, tmp_path_factory, _working_dir):
        destination = tmp_path_factory.mktemp("destination")

        source_dirs: List[Path] = [
            _working_dir / "one",
            _working_dir / "two",
            _working_dir / "EmptyDirTest",
        ]

        assert all(source_dir.is_dir() for source_dir in source_dirs)

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                source_dirs,
                ssd=False,
                force=False,
                quiet=False,
                file_includes=[
                    re.compile(".*/two/.*"),
                    re.compile(".*/EmptyDirTest/.*"),
                ],
                file_excludes=None,
            )

            assert dm.result == 0

        content_dir = destination / CONTENT_DIR_NAME
        content_dir_prefix = TestHelpers.GetOutputPath(content_dir, _working_dir)

        assert set(file_info.path for file_info in TestHelpers.Enumerate(content_dir)) == set(
            [
                content_dir_prefix / "EmptyDirTest" / "EmptyDir",
                content_dir_prefix / "two" / "File1",
                content_dir_prefix / "two" / "File2",
                content_dir_prefix / "two" / "Dir1" / "File3",
                content_dir_prefix / "two" / "Dir1" / "File4",
                content_dir_prefix / "two" / "Dir2" / "Dir3" / "File5",
            ],
        )

    # ----------------------------------------------------------------------
    def test_FilterExclude(self, tmp_path_factory, _working_dir):
        destination = tmp_path_factory.mktemp("destination")

        source_dirs: List[Path] = [
            _working_dir / "one",
            _working_dir / "two",
            _working_dir / "EmptyDirTest",
        ]

        assert all(source_dir.is_dir() for source_dir in source_dirs)

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                source_dirs,
                ssd=False,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=[
                    re.compile(".*/two/.*"),
                ],
            )

            assert dm.result == 0

        content_dir = destination / CONTENT_DIR_NAME
        content_dir_prefix = TestHelpers.GetOutputPath(content_dir, _working_dir)

        assert set(file_info.path for file_info in TestHelpers.Enumerate(content_dir)) == set(
            [
                content_dir_prefix / "EmptyDirTest" / "EmptyDir",
                content_dir_prefix / "one" / "A",
                content_dir_prefix / "one" / "BC",
            ],
        )

    # ----------------------------------------------------------------------
    def test_FilterIncludeAndExclude(self, tmp_path_factory, _working_dir):
        destination = tmp_path_factory.mktemp("destination")

        source_dirs: List[Path] = [
            _working_dir / "one",
            _working_dir / "two",
            _working_dir / "EmptyDirTest",
        ]

        assert all(source_dir.is_dir() for source_dir in source_dirs)

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                source_dirs,
                ssd=False,
                force=False,
                quiet=False,
                file_includes=[
                    re.compile(".*/(?:BC|EmptyDir|File1)"),
                ],
                file_excludes=[
                    re.compile(".*/two/.*"),
                ],
            )

            assert dm.result == 0

        content_dir = destination / CONTENT_DIR_NAME
        content_dir_prefix = TestHelpers.GetOutputPath(content_dir, _working_dir)

        assert set(file_info.path for file_info in TestHelpers.Enumerate(content_dir)) == set(
            [
                content_dir_prefix / "EmptyDirTest" / "EmptyDir",
                content_dir_prefix / "one" / "BC",
            ],
        )

    # ----------------------------------------------------------------------
    def test_SingleFileInputWithSiblings(self, tmp_path_factory, _working_dir):
        destination = tmp_path_factory.mktemp("destination")

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                [
                    _working_dir / "one" / "BC",
                ],
                ssd=False,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        content_dir = destination / CONTENT_DIR_NAME
        content_dir_prefix = TestHelpers.GetOutputPath(content_dir, _working_dir)

        assert [file_info.path for file_info in TestHelpers.Enumerate(content_dir)] == [
            content_dir_prefix / "one" / "BC",
        ]

    # ----------------------------------------------------------------------
    def test_ErrorInvalidInput(self, tmp_path_factory):
        does_not_exist = Path("does/not/exist/test")

        with pytest.raises(
            Exception,
            match=re.escape("'{}' is not a valid filename or directory.".format(does_not_exist)),
        ):
            with DoneManager.Create(StringIO(), "") as dm:
                Backup(
                    dm,
                    tmp_path_factory.mktemp("destination"),
                    [
                        does_not_exist,
                    ],
                    ssd=False,
                    force=False,
                    quiet=False,
                    file_includes=None,
                    file_excludes=None,
                )

                assert dm.result == 0

    # ----------------------------------------------------------------------
    def test_ErrorOverlappingPaths(self, _working_dir):
        with pytest.raises(
            Exception,
            match=re.escape(
                "The directory '{}' overlaps with the destination path '{}'.".format(
                    _working_dir / "two",
                    _working_dir / "two" / "Dir1",
                ),
            ),
        ):
            with DoneManager.Create(StringIO(), "") as dm:
                Backup(
                    dm,
                    _working_dir / "two" / "Dir1",
                    [
                        _working_dir / "one",
                        _working_dir / "two",
                    ],
                    ssd=False,
                    force=False,
                    quiet=False,
                    file_includes=None,
                    file_excludes=None,
                )

                assert dm.result == 0

    # ----------------------------------------------------------------------
    @mock.patch("shutil.disk_usage")
    def test_ErrorInadequateDiskSpace(self, disk_usage_mock, tmp_path_factory, _working_dir):
        # ----------------------------------------------------------------------
        class MockedResult(object):
            # ----------------------------------------------------------------------
            def __init__(self):
                self.free = 5

        # ----------------------------------------------------------------------

        disk_usage_mock.side_effect = lambda _: MockedResult()

        destination = tmp_path_factory.mktemp("destination")

        sink = StringIO()

        with DoneManager.Create(sink, "") as dm:
            Backup(
                dm,
                destination,
                [_working_dir],
                ssd=True,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == -1

        sink = sink.getvalue()

        assert "There is not enough disk space to process this request." in sink
        assert "1 KB required, 1 KB available" in sink

    # ----------------------------------------------------------------------
    def test_ChangeNone(self, _existing_content):
        working_dir, destination = _existing_content

        sink = StringIO()

        with DoneManager.Create(sink, "") as dm:
            Backup(
                dm,
                destination,
                [working_dir],
                ssd=True,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        sink = sink.getvalue()

        assert "no diffs found" in sink

        TestHelpers.CompareFileSystemSourceAndDestination(
            working_dir,
            destination,
            10,
            compare_file_contents=True,
        )

    # ----------------------------------------------------------------------
    def test_ChangeFileRemoved(self, _existing_content):
        working_dir, destination = _existing_content

        (working_dir / "two" / "File1").unlink()

        sink = StringIO()

        with DoneManager.Create(sink, "") as dm:
            Backup(
                dm,
                destination,
                [working_dir],
                ssd=True,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        sink = sink.getvalue()

        assert "1 diff found" in sink

        TestHelpers.CompareFileSystemSourceAndDestination(
            working_dir,
            destination,
            9,
            compare_file_contents=True,
        )

    # ----------------------------------------------------------------------
    def test_ChangeDirRemoved(self, _existing_content):
        working_dir, destination = _existing_content

        PathEx.RemoveTree(working_dir / "two")

        sink = StringIO()

        with DoneManager.Create(sink, "") as dm:
            Backup(
                dm,
                destination,
                [working_dir],
                ssd=True,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        sink = sink.getvalue()

        assert "1 diff found" in sink

        TestHelpers.CompareFileSystemSourceAndDestination(
            working_dir,
            destination,
            5,
            compare_file_contents=True,
        )

    # ----------------------------------------------------------------------
    def test_ChangeFileAdded(self, _existing_content):
        working_dir, destination = _existing_content

        with (working_dir / "one" / "NewFile").open("w") as f:
            f.write("New content!")

        sink = StringIO()

        with DoneManager.Create(sink, "") as dm:
            Backup(
                dm,
                destination,
                [working_dir],
                ssd=True,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        sink = sink.getvalue()

        assert "1 diff found" in sink

        TestHelpers.CompareFileSystemSourceAndDestination(
            working_dir,
            destination,
            11,
            compare_file_contents=True,
        )

    # ----------------------------------------------------------------------
    def test_ChangeDirAdded(self, _existing_content):
        working_dir, destination = _existing_content

        (working_dir / "one" / "NewDir").mkdir()

        sink = StringIO()

        with DoneManager.Create(sink, "") as dm:
            Backup(
                dm,
                destination,
                [working_dir],
                ssd=True,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        sink = sink.getvalue()

        assert "1 diff found" in sink

        TestHelpers.CompareFileSystemSourceAndDestination(
            working_dir,
            destination,
            11,
            compare_file_contents=True,
        )

    # ----------------------------------------------------------------------
    def test_ChangeModifyContent(self, _existing_content):
        working_dir, destination = _existing_content

        with (working_dir / "one" / "A").open("w") as f:
            f.write("New content A")
        with (working_dir / "one" / "BC").open("w") as f:
            f.write("New content BC")
        with (working_dir / "two" / "Dir1" / "File4").open("w") as f:
            f.write("New content File4")

        sink = StringIO()

        with DoneManager.Create(sink, "") as dm:
            Backup(
                dm,
                destination,
                [working_dir],
                ssd=True,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        sink = sink.getvalue()

        assert "3 diffs found" in sink

        TestHelpers.CompareFileSystemSourceAndDestination(
            working_dir,
            destination,
            10,
            compare_file_contents=True,
        )

    # ----------------------------------------------------------------------
    def test_Force(self, _existing_content):
        working_dir, destination = _existing_content

        sink = StringIO()

        with DoneManager.Create(sink, "") as dm:
            Backup(
                dm,
                destination,
                [working_dir],
                ssd=True,
                force=True,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        sink = sink.getvalue()

        assert "No diffs found" not in sink
        assert "Committing snapshot data" in sink

        TestHelpers.CompareFileSystemSourceAndDestination(
            working_dir,
            destination,
            10,
            compare_file_contents=True,
        )

    # ----------------------------------------------------------------------
    def test_ErrorBulkStorage(self, _working_dir):
        dm_and_sink = iter(GenerateDoneManagerAndSink())

        Backup(
            cast(DoneManager, next(dm_and_sink)),
            "fast_glacier://account@region",
            [_working_dir],
            ssd=False,
            force=False,
            quiet=False,
            file_includes=None,
            file_excludes=None,
        )

        output = cast(str, next(dm_and_sink))

        assert output == textwrap.dedent(
            """\
            Heading...
              ERROR: 'fast_glacier://account@region' does not resolve to a file-based data store, which is required when mirroring content.
            DONE! (-1, <scrubbed duration>)
            """,
        )

    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    @staticmethod
    @pytest.fixture()
    def _existing_content(tmp_path_factory, _working_dir) -> Tuple[Path, Path]:
        destination = tmp_path_factory.mktemp("destination")

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                [_working_dir],
                ssd=True,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        TestHelpers.CompareFileSystemSourceAndDestination(
            _working_dir,
            destination,
            compare_file_contents=False,
        )

        return _working_dir, destination


# ----------------------------------------------------------------------
class TestFileSystemCleanup(object):
    # ----------------------------------------------------------------------
    def test_DoesNotExist(self):
        sink = StringIO()

        does_not_exist = Path("does not exist").resolve()

        with DoneManager.Create(sink, "") as dm:
            Cleanup(dm, str(does_not_exist))

            assert dm.result == 0

        expected_text = "Content does not exist.".format(does_not_exist)
        sink = sink.getvalue()

        assert expected_text in sink

    # ----------------------------------------------------------------------
    def test_AddFiles(self, tmp_path_factory, _working_dir):
        destination = tmp_path_factory.mktemp("destination")

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                [_working_dir],
                ssd=False,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

            content_output_dir = TestHelpers.GetOutputPath(
                destination / CONTENT_DIR_NAME,
                _working_dir,
            )

            original_num_files = sum(1 for _ in TestHelpers.Enumerate(content_output_dir))

            # Pending deletes will be restored
            pending_delete_source = PathEx.EnsureFile(content_output_dir / "one" / "A")
            pending_delete_file = pending_delete_source.parent / (pending_delete_source.name + PENDING_DELETE_EXTENSION)

            # Pending commits will be removed
            pending_commit_file = content_output_dir / "one" / ("BC" + PENDING_COMMIT_EXTENSION)

            shutil.move(pending_delete_source, pending_delete_file)

            with pending_commit_file.open("w") as f:
                f.write("New value")

            assert sum(1 for _ in TestHelpers.Enumerate(content_output_dir)) == original_num_files + 1

            Cleanup(dm, destination)
            assert dm.result == 0

            assert sum(1 for _ in TestHelpers.Enumerate(content_output_dir)) == original_num_files
            assert pending_delete_source.is_file()
            assert not pending_delete_file.is_file()
            assert not pending_commit_file.is_file()

            # Regular files will not be touched
            with (content_output_dir / "A New File").open("w") as f:
                f.write("Some new content")

            assert sum(1 for _ in TestHelpers.Enumerate(content_output_dir)) == original_num_files + 1

            Cleanup(dm, destination)
            assert dm.result == 0

            assert sum(1 for _ in TestHelpers.Enumerate(content_output_dir)) == original_num_files + 1

    # ----------------------------------------------------------------------
    def test_ContentIsFile(self, tmp_path_factory):
        destination = tmp_path_factory.mktemp("root")

        with (destination / CONTENT_DIR_NAME).open("w") as f:
            f.write("This shouldn't be a file")

        sink = StringIO()

        with DoneManager.Create(sink, "") as dm:
            Cleanup(dm, destination)
            assert dm.result == 0

        sink = sink.getvalue()

        assert "Removing the file '{}'...DONE!".format(CONTENT_DIR_NAME) in sink

    # ----------------------------------------------------------------------
    def test_ContentIsSymlink(self, tmp_path_factory):
        destination = tmp_path_factory.mktemp("root")

        os.symlink(destination, destination / CONTENT_DIR_NAME)

        sink = StringIO()

        with DoneManager.Create(sink, "") as dm:
            with pytest.raises(
                Exception,
                match="'Content' is not a valid directory.",
            ):
                Cleanup(dm, destination)

    # ----------------------------------------------------------------------
    def test_ErrorBulkStorage(self, _working_dir):
        dm_and_sink = iter(GenerateDoneManagerAndSink())

        Cleanup(
            cast(DoneManager, next(dm_and_sink)),
            "fast_glacier://account@region",
        )

        output = cast(str, next(dm_and_sink))

        assert output == textwrap.dedent(
            """\
            Heading...
              ERROR: 'fast_glacier://account@region' does not resolve to a file-based data store, which is required when mirroring content.
            DONE! (-1, <scrubbed duration>)
            """,
        )


# ----------------------------------------------------------------------
class TestFileSystemValidate(object):
    # ----------------------------------------------------------------------
    def test_NoOutput(self):
        does_not_exist = Path("does/not/exist/test").resolve()

        sink = StringIO()

        # Ensure that colors aren't displayed, even when environment var overrides are set (as they
        # will be when running on the CI machine).
        Capabilities.Create(
            sink,
            is_interactive=False,
            supports_colors=False,
            is_headless=True,
        )

        with DoneManager.Create(sink, "Sink output...") as dm:
            Validate(
                dm,
                str(does_not_exist),
                ValidateType.standard,
                ssd=False,
                quiet=False,
            )

            assert dm.result == -1

        sink = sink.getvalue()

        assert TestHelpers.ScrubDurations(sink) == textwrap.dedent(
            """\
            Sink output...
              ERROR: No snapshot was found.
            DONE! (-1, <scrubbed duration>)
            """,
        ).format(does_not_exist)

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("validate_type", [ValidateType.standard, ValidateType.complete])
    def test_NoChange(self, tmp_path_factory, _working_dir, validate_type):
        self._Test(
            lambda content_dir: (
                textwrap.dedent(
                    """\
                    Sink output...
                      Reading 'BackupSnapshot.json'...


                      DONE! (0, <scrubbed duration>)
                      Reverting partially committed content at the destination...DONE! (0, <scrubbed duration>, no items reverted)

                      Extracting files...
                        Discovering files...
                          Processing 1 item...


                          DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                        DONE! (0, <scrubbed duration>, 9 files found, 1 empty directory found)

                        {}
                          Processing 9 items...


                          DONE! (0, <scrubbed duration>, 9 items succeeded, no items with errors, no items with warnings)
                        DONE! (0, <scrubbed duration>)

                        Organizing results...DONE! (0, <scrubbed duration>)
                      DONE! (0, <scrubbed duration>)

                      Validating content...
                        INFO: No differences were found.
                      DONE! (0, <scrubbed duration>)
                    DONE! (0, <scrubbed duration>)
                    """,
                ).format(
                    "Retrieving file information..." if validate_type == ValidateType.standard else "Calculating hashes...",
                )
            ),
            tmp_path_factory,
            _working_dir,
            validate_type,
            expected_validate_result=0,
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("validate_type", [ValidateType.standard, ValidateType.complete])
    def test_AddFiles(self, tmp_path_factory, _working_dir, validate_type):
        # ----------------------------------------------------------------------
        def Impl(
            content_dir: Path,
        ) -> str:
            file1 = content_dir / "one" / "NewFile1"
            file2 = content_dir / "EmptyDirTest" / "EmptyDir" / "NewFile2"

            with file1.open("w") as f:
                f.write("123456")

            with file2.open("w") as f:
                f.write("abc")

            return textwrap.dedent(
                """\
                Sink output...
                  Reading 'BackupSnapshot.json'...


                  DONE! (0, <scrubbed duration>)
                  Reverting partially committed content at the destination...DONE! (0, <scrubbed duration>, no items reverted)

                  Extracting files...
                    Discovering files...
                      Processing 1 item...


                      DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>, 11 files found, 0 empty directories found)

                    {hash_header}
                      Processing 11 items...


                      DONE! (0, <scrubbed duration>, 11 items succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>)

                    Organizing results...DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>)

                  Validating content...
                    ERROR: '{file2}' has been added.
                    ERROR: '{file1}' has been added.
                  DONE! (-1, <scrubbed duration>)
                DONE! (-1, <scrubbed duration>)
                """,
            ).format(
                hash_header="Retrieving file information..." if validate_type == ValidateType.standard else "Calculating hashes...",
                file1=file1,
                file2=file2,
            )

        # ----------------------------------------------------------------------

        self._Test(
            Impl,
            tmp_path_factory,
            _working_dir,
            validate_type,
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("validate_type", [ValidateType.standard, ValidateType.complete])
    def test_RemoveFiles(self, tmp_path_factory, _working_dir, validate_type):
        # ----------------------------------------------------------------------
        def Impl(
            content_dir: Path,
        ) -> str:
            file1 = content_dir / "one" / "A"
            file2 = content_dir / "EmptyDirTest" / "EmptyDir"

            PathEx.RemoveFile(file1)
            PathEx.RemoveTree(file2)

            return textwrap.dedent(
                """\
                Sink output...
                  Reading 'BackupSnapshot.json'...


                  DONE! (0, <scrubbed duration>)
                  Reverting partially committed content at the destination...DONE! (0, <scrubbed duration>, no items reverted)

                  Extracting files...
                    Discovering files...
                      Processing 1 item...


                      DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>, 8 files found, 1 empty directory found)

                    {hash_header}
                      Processing 8 items...


                      DONE! (0, <scrubbed duration>, 8 items succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>)

                    Organizing results...DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>)

                  Validating content...
                    ERROR: '{file2}' has been removed.
                    ERROR: '{file1}' has been removed.
                  DONE! (-1, <scrubbed duration>)
                DONE! (-1, <scrubbed duration>)
                """,
            ).format(
                hash_header="Retrieving file information..." if validate_type == ValidateType.standard else "Calculating hashes...",
                file1=file1,
                file2=file2,
            )

        # ----------------------------------------------------------------------

        self._Test(
            Impl,
            tmp_path_factory,
            _working_dir,
            validate_type,
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("validate_type", [ValidateType.standard, ValidateType.complete])
    def test_FileChangedStandardSameSize(self, tmp_path_factory, _working_dir, validate_type):
        # ----------------------------------------------------------------------
        def Impl(
            content_dir: Path,
        ) -> str:
            file = PathEx.EnsureFile(content_dir / "one" / "A")

            # Do not modify the file's size
            file_size = file.stat().st_size

            with file.open("w") as f:
                f.write(" " * file_size)

            if validate_type == ValidateType.standard:
                # No changes will be detected with standard compare (because the size didn't change)
                validating_content_section = "INFO: No differences were found."
                return_code = 0

            elif validate_type == ValidateType.complete:
                validating_content_section = textwrap.dedent(
                    """\
                    WARNING: '{file}' has been modified.

                                     Expected file size:     {file_size}
                                     Actual file size:       {file_size}
                                     Expected hash value:    38818bc4ba444583f537b9ed36a2fb4e7fd49694efd4a06b8fe0c1b00161e904f4edb7a9713543b74f283261d3000671b6c0567d6abea2b19686870d8b344b4e
                                     Actual hash value:      e524ccd3ddf10b82db1c2f36d38ceeda6ed76eecb56d3cb326cd298d96706deef8cb895322343edb5069a068223c590cee6a821fc424a7e785b03d6c82b9e79d
                    """,
                ).format(
                    file=file,
                    file_size=file_size,
                )

                return_code = 1
            else:
                assert False, validate_type  # pragma: no cover

            return textwrap.dedent(
                """\
                Sink output...
                  Reading 'BackupSnapshot.json'...


                  DONE! (0, <scrubbed duration>)
                  Reverting partially committed content at the destination...DONE! (0, <scrubbed duration>, no items reverted)

                  Extracting files...
                    Discovering files...
                      Processing 1 item...


                      DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>, 9 files found, 1 empty directory found)

                    {hash_header}
                      Processing 9 items...


                      DONE! (0, <scrubbed duration>, 9 items succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>)

                    Organizing results...DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>)

                  Validating content...
                    {validating_content}
                  DONE! ({return_code}, <scrubbed duration>)
                DONE! ({return_code}, <scrubbed duration>)
                """,
            ).format(
                hash_header="Retrieving file information..." if validate_type == ValidateType.standard else "Calculating hashes...",
                validating_content=validating_content_section,
                return_code=return_code,
            )

        # ----------------------------------------------------------------------

        self._Test(
            Impl,
            tmp_path_factory,
            _working_dir,
            validate_type,
            expected_validate_result=0 if validate_type == ValidateType.standard else 1,
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("validate_type", [ValidateType.standard, ValidateType.complete])
    def test_FileChangedStandardDifferentSize(self, tmp_path_factory, _working_dir, validate_type):
        # ----------------------------------------------------------------------
        def Impl(
            content_dir: Path,
        ) -> str:
            file = PathEx.EnsureFile(content_dir / "one" / "A")

            # Do not modify the file's size
            file_size = file.stat().st_size
            new_file_size = file_size + 10

            with file.open("w") as f:
                f.write(" " * new_file_size)

            if validate_type == ValidateType.standard:
                validating_content_section = textwrap.dedent(
                    """\
                    WARNING: '{}' has been modified.

                                     Expected file size:     {}
                                     Actual file size:       {}
                    """,
                ).format(file, file_size, new_file_size)

            elif validate_type == ValidateType.complete:
                validating_content_section = textwrap.dedent(
                    """\
                    WARNING: '{}' has been modified.

                                     Expected file size:     {}
                                     Actual file size:       {}
                                     Expected hash value:    38818bc4ba444583f537b9ed36a2fb4e7fd49694efd4a06b8fe0c1b00161e904f4edb7a9713543b74f283261d3000671b6c0567d6abea2b19686870d8b344b4e
                                     Actual hash value:      13d9ef706bf97bf8dc6e2a2e1a8d12008f61dffccac88d1214acdd2ab0d4e27b18efa2d7bdc47bf490f5787cda318f2380676d96691f9971bad4e73bc39ac4f8
                    """,
                ).format(file, file_size, new_file_size)

            else:
                assert False, validate_type  # pragma: no cover

            return textwrap.dedent(
                """\
                Sink output...
                  Reading 'BackupSnapshot.json'...


                  DONE! (0, <scrubbed duration>)
                  Reverting partially committed content at the destination...DONE! (0, <scrubbed duration>, no items reverted)

                  Extracting files...
                    Discovering files...
                      Processing 1 item...


                      DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>, 9 files found, 1 empty directory found)

                    {hash_header}
                      Processing 9 items...


                      DONE! (0, <scrubbed duration>, 9 items succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>)

                    Organizing results...DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>)

                  Validating content...
                    {validating_content}
                  DONE! (1, <scrubbed duration>)
                DONE! (1, <scrubbed duration>)
                """,
            ).format(
                hash_header="Retrieving file information..." if validate_type == ValidateType.standard else "Calculating hashes...",
                validating_content=validating_content_section,
            )

        # ----------------------------------------------------------------------

        self._Test(
            Impl,
            tmp_path_factory,
            _working_dir,
            validate_type,
            expected_validate_result=1,
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("validate_type", [ValidateType.standard, ValidateType.complete])
    def test_DirectoriesAdded(self, tmp_path_factory, _working_dir, validate_type):
        # ----------------------------------------------------------------------
        def Impl(
            content_dir: Path,
        ) -> str:
            new_dir = content_dir / "New Empty Dir"
            new_file = content_dir / "New Dir with Content" / "File1"

            new_dir.mkdir(parents=True, exist_ok=True)
            new_file.parent.mkdir(parents=True, exist_ok=True)

            with new_file.open("w") as f:
                f.write("new content")

            return textwrap.dedent(
                """\
                Sink output...
                  Reading 'BackupSnapshot.json'...


                  DONE! (0, <scrubbed duration>)
                  Reverting partially committed content at the destination...DONE! (0, <scrubbed duration>, no items reverted)

                  Extracting files...
                    Discovering files...
                      Processing 1 item...


                      DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>, 10 files found, 2 empty directories found)

                    {hash_header}
                      Processing 10 items...


                      DONE! (0, <scrubbed duration>, 10 items succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>)

                    Organizing results...DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>)

                  Validating content...
                    ERROR: '{new_file}' has been added.
                    ERROR: '{new_dir}' has been added.
                  DONE! (-1, <scrubbed duration>)
                DONE! (-1, <scrubbed duration>)
                """,
            ).format(
                hash_header="Retrieving file information..." if validate_type == ValidateType.standard else "Calculating hashes...",
                new_file=new_file,
                new_dir=new_dir,
            )

        # ----------------------------------------------------------------------

        self._Test(
            Impl,
            tmp_path_factory,
            _working_dir,
            validate_type,
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("validate_type", [ValidateType.standard, ValidateType.complete])
    def test_DirectoriesRemoved(self, tmp_path_factory, _working_dir, validate_type):
        # ----------------------------------------------------------------------
        def Impl(
            content_dir: Path,
        ) -> str:
            dir1 = PathEx.EnsureDir(content_dir / "EmptyDirTest" / "EmptyDir")
            dir2 = PathEx.EnsureDir(content_dir / "one")

            PathEx.RemoveTree(dir1)
            PathEx.RemoveTree(dir2)


            return textwrap.dedent(
                """\
                Sink output...
                  Reading 'BackupSnapshot.json'...


                  DONE! (0, <scrubbed duration>)
                  Reverting partially committed content at the destination...DONE! (0, <scrubbed duration>, no items reverted)

                  Extracting files...
                    Discovering files...
                      Processing 1 item...


                      DONE! (0, <scrubbed duration>, 1 item succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>, 7 files found, 1 empty directory found)

                    {hash_header}
                      Processing 7 items...


                      DONE! (0, <scrubbed duration>, 7 items succeeded, no items with errors, no items with warnings)
                    DONE! (0, <scrubbed duration>)

                    Organizing results...DONE! (0, <scrubbed duration>)
                  DONE! (0, <scrubbed duration>)

                  Validating content...
                    ERROR: '{dir1}' has been removed.
                    ERROR: '{dir2}' has been removed.
                  DONE! (-1, <scrubbed duration>)
                DONE! (-1, <scrubbed duration>)
                """,
            ).format(
                hash_header="Retrieving file information..." if validate_type == ValidateType.standard else "Calculating hashes...",
                dir1=dir1,
                dir2=dir2,
            )

        # ----------------------------------------------------------------------

        self._Test(
            Impl,
            tmp_path_factory,
            _working_dir,
            validate_type,
        )

    # ----------------------------------------------------------------------
    def test_ErrorBulkStorage(self, _working_dir):
        dm_and_sink = iter(GenerateDoneManagerAndSink())

        Validate(
            cast(DoneManager, next(dm_and_sink)),
            "fast_glacier://account@region",
            ValidateType.standard,
            ssd=False,
            quiet=False,
        )

        output = cast(str, next(dm_and_sink))

        assert output == textwrap.dedent(
            """\
            Heading...
              ERROR: 'fast_glacier://account@region' does not resolve to a file-based data store, which is required when mirroring content.
            DONE! (-1, <scrubbed duration>)
            """,
        )


    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    @staticmethod
    def _Test(
        alter_backup_func: Callable[
            [
                Path,                       # Content dir
            ],
            str,
        ],
        tmp_path_factory,
        _working_dir,
        validate_type: ValidateType,
        *,
        expected_validate_result: int=-1,
    ) -> None:
        destination = tmp_path_factory.mktemp("destination")

        with DoneManager.Create(StringIO(), "") as dm:
            Backup(
                dm,
                destination,
                [_working_dir],
                ssd=False,
                force=False,
                quiet=False,
                file_includes=None,
                file_excludes=None,
            )

            assert dm.result == 0

        expected_template = alter_backup_func(
            TestHelpers.GetOutputPath(
                destination / CONTENT_DIR_NAME,
                _working_dir,
            ),
        )

        sink = StringIO()

        # Ensure that colors aren't displayed, even when environment var overrides are set (as they
        # will be when running on the CI machine).
        Capabilities.Create(
            sink,
            is_interactive=False,
            supports_colors=False,
            is_headless=True,
        )

        with DoneManager.Create(sink, "Sink output...") as dm:
            Validate(
                dm,
                destination,
                validate_type,
                ssd=False,
                quiet=True,
            )

            assert dm.result == expected_validate_result

        sink = sink.getvalue()
        sink = TestHelpers.ScrubDurations(sink)

        assert sink == expected_template


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
