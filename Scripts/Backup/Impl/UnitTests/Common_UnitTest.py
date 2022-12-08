# ----------------------------------------------------------------------
# |
# |  Common_UnitTest.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-12-02 15:08:06
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Unit tests for Common.py"""

import copy
import os
import sys

from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation.Streams.DoneManager import DoneManager
from Common_Foundation.TestHelpers.StreamTestHelpers import GenerateDoneManagerAndSink


# ----------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
with ExitStack(lambda: sys.path.pop(0)):
    from Backup.Impl import TestHelpers
    from Backup.Impl.DataStores.DataStore import DataStore
    from Backup.Impl.Common import *
    from Backup.Impl.Snapshot import Snapshot


# ----------------------------------------------------------------------
class TestDirHashPlaceholder(object):
    # ----------------------------------------------------------------------
    def test_Equal(self):
        assert DirHashPlaceholder(explicitly_added=True) == DirHashPlaceholder(explicitly_added=False)
        assert DirHashPlaceholder(explicitly_added=True) != 10
        assert DirHashPlaceholder(explicitly_added=False) != "foo"


# ----------------------------------------------------------------------
class TestDiffResult(object):
    # ----------------------------------------------------------------------
    def test_Construct(self):
        result = DiffResult(DiffOperation.modify, Path("foo/bar"), "hash1", 20, "hash2", 30)

        assert result.operation == DiffOperation.modify
        assert result.path == Path("foo/bar")
        assert result.this_hash == "hash1"
        assert result.this_file_size == 20
        assert result.other_hash == "hash2"
        assert result.other_file_size == 30

        DiffResult(DiffOperation.add, Path("file_added"), "hash1", 20, None, None)
        DiffResult(DiffOperation.add, Path("dir_added"), DirHashPlaceholder(explicitly_added=True), None, None, None)

        DiffResult(DiffOperation.modify, Path("file_modified"), "hash1", 20, "hash2", 30)

        DiffResult(DiffOperation.remove, Path("file_removed"), None, None, "hash2", 30)
        DiffResult(DiffOperation.remove, Path("dir_removed"), None, None, DirHashPlaceholder(explicitly_added=True), None)

    # ----------------------------------------------------------------------
    def test_ErrorInconsistentState(self):
        for func in [
            lambda: DiffResult(DiffOperation.add, Path("foo"), None, 20, None, None),
            lambda: DiffResult(DiffOperation.modify, Path("foo"), None, 20, "hash2", 30),
            lambda: DiffResult(DiffOperation.modify, Path("foo"), "hash1", 20, None, 30),
            lambda: DiffResult(DiffOperation.remove, Path("foo"), None, None, None, 30),

        ]:
            with pytest.raises(AssertionError, match="Instance is in an inconsistent state"):
                func()

    # ----------------------------------------------------------------------
    def test_ErrorThisInconsistentState(self):
        for func in [
            lambda: DiffResult(DiffOperation.remove, Path("foo"), None, 20, "hash2", 30),
            lambda: DiffResult(DiffOperation.add, Path("foo"), "hash1", None, None, None),
            lambda: DiffResult(DiffOperation.add, Path("foo"), DirHashPlaceholder(explicitly_added=True), 20, None, None),
        ]:
            with pytest.raises(AssertionError, match="'this' values are in an inconsistent state"):
                func()

    # ----------------------------------------------------------------------
    def test_ErrorOtherInconsistentState(self):
        for func in [
            lambda: DiffResult(DiffOperation.add, Path("foo"), "hash1", 20, None, 30),
            lambda: DiffResult(DiffOperation.remove, Path("foo"), None, None, "hash2", None),
            lambda: DiffResult(DiffOperation.remove, Path("foo"), None, None, DirHashPlaceholder(explicitly_added=True), 30),
        ]:
            with pytest.raises(AssertionError, match="'other' values are in an inconsistent state"):
                func()

    # ----------------------------------------------------------------------
    def test_ModifyInconsistentState(self):
        for func in [
            lambda: DiffResult(DiffOperation.modify, Path("foo"), DirHashPlaceholder(explicitly_added=True), None, "hash2", 30),
            lambda: DiffResult(DiffOperation.modify, Path("foo"), "hash1", 20, DirHashPlaceholder(explicitly_added=True), None),
            lambda: DiffResult(DiffOperation.modify, Path("foo"), "same_hash", 20, "same_hash", 30),
        ]:
            with pytest.raises(AssertionError, match="modify values are in an inconsistent state"):
                func()

    # ----------------------------------------------------------------------
    def test_ToJson(self):
        assert DiffResult(DiffOperation.add, Path("foo/bar"), "hash1", 20, None, None).ToJson() == {
            "operation": "add",
            "path": "foo/bar",
            "this_hash": "hash1",
            "this_file_size": 20,
        }

        assert DiffResult(DiffOperation.add, Path("foo/bar"), DirHashPlaceholder(explicitly_added=True), None, None, None).ToJson() == {
            "operation": "add",
            "path": "foo/bar",
        }

        assert DiffResult(DiffOperation.modify, Path("one"), "hash1", 20, "hash2", 30).ToJson() == {
            "operation": "modify",
            "path": "one",
            "this_hash": "hash1",
            "this_file_size": 20,
            "other_hash": "hash2",
            "other_file_size": 30,
        }

        assert DiffResult(DiffOperation.remove, Path("one/two/three"), None, None, "hash2", 30).ToJson() == {
            "operation": "remove",
            "path": "one/two/three",
            "other_hash": "hash2",
            "other_file_size": 30,
        }

        assert DiffResult(DiffOperation.remove, Path("one/two/three"), None, None, DirHashPlaceholder(explicitly_added=True), None).ToJson() == {
            "operation": "remove",
            "path": "one/two/three",
        }

    # ----------------------------------------------------------------------
    def test_FromJson(self):
        assert DiffResult.FromJson(
            {
                "operation": "add",
                "path": "foo/bar",
                "this_hash": "hash1",
                "this_file_size": 20,
            },
        ) == DiffResult(DiffOperation.add, Path("foo/bar"), "hash1", 20, None, None)

        assert DiffResult.FromJson(
            {
                "operation": "add",
                "path": "foo/bar",
            },
        ) == DiffResult(DiffOperation.add, Path("foo/bar"), DirHashPlaceholder(explicitly_added=False), None, None, None)

        assert DiffResult.FromJson(
            {
                "operation": "modify",
                "path": "one",
                "this_hash": "hash1",
                "this_file_size": 20,
                "other_hash": "hash2",
                "other_file_size": 30,
            },
        ) == DiffResult(DiffOperation.modify, Path("one"), "hash1", 20, "hash2", 30)

        assert DiffResult.FromJson(
            {
                "operation": "remove",
                "path": "one/two/three",
                "other_hash": "hash2",
                "other_file_size": 30,
            },
        ) == DiffResult(DiffOperation.remove, Path("one/two/three"), None, None, "hash2", 30)

        assert DiffResult.FromJson(
            {
                "operation": "remove",
                "path": "one/two/three",
            },
        ) == DiffResult(DiffOperation.remove, Path("one/two/three"), None, None, DirHashPlaceholder(explicitly_added=False), None)


# ----------------------------------------------------------------------
def test_GetDestinationHelp():
    # Here only for coverage
    assert GetDestinationHelp()


# ----------------------------------------------------------------------
def test_GetTaskDisplayName():
    assert GetTaskDisplayName(Path("0123456789")) ==                                                                                                                  "0123456789                                                                                          "
    assert GetTaskDisplayName(Path("012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")) ==    "012345678901234567890123456789012345678901234567...1234567890123456789012345678901234567890123456789"
    assert GetTaskDisplayName(Path("012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_")) ==   "012345678901234567890123456789012345678901234567...234567890123456789012345678901234567890123456789_"


# ----------------------------------------------------------------------
class TestYieldDataStore(object):
    # ----------------------------------------------------------------------
    @mock.patch("Backup.Impl.Common.FileSystemDataStore")
    def test_FileSystemDataStore(self, mocked_class):
        for connection in [
            "one/two/three",
            Path("a/b/c"),
        ]:
            with YieldDataStore(mock.MagicMock(), connection, ssd=False):
                pass

            args, kwargs = mocked_class.call_args
            assert args == (Path(connection), )
            assert kwargs == {"ssd": False, "is_local_filesystem_override_value_for_testing": None}

            mocked_class.reset_mock()

    # ----------------------------------------------------------------------
    @mock.patch("Backup.Impl.Common.FileSystemDataStore")
    def test_NonlocalFileSystemDataStore(self, mocked_class):
        dm_and_sink = iter(GenerateDoneManagerAndSink())

        with YieldDataStore(
            cast(DoneManager, next(dm_and_sink)),
            "[nonlocal]this_is_the_filename",
            ssd=True,
        ):
            pass

        args, kwargs = mocked_class.call_args
        assert args == (Path("this_is_the_filename"), )
        assert kwargs == {"ssd": True, "is_local_filesystem_override_value_for_testing": False}

        output = cast(str, next(dm_and_sink))

        assert output == textwrap.dedent(
            """\
            Heading...
              INFO: The destination string used to create a 'FileSystemDataStore' instance has been explicitly declared as nonlocal;
                    this should only be used in testing scenarios.

                        Connection:  [nonlocal]this_is_the_filename
                        Filename:    this_is_the_filename

            DONE! (0, <scrubbed duration>)
            """,
        )

    # ----------------------------------------------------------------------
    @mock.patch("Backup.Impl.Common.SFTPDataStore")
    def test_SFTPDataStore(self, mocked_class):
        for connection, host, username, key_or_password, working_dir, port in [
            (
                "ftp://username:password@hostname",
                "hostname",
                "username",
                "password",
                None,
                22,
            ),
            (
                "ftp://username:password@hostname:20",
                "hostname",
                "username",
                "password",
                None,
                20,
            ),
            (
                "ftp://username:password@hostname/path/to/working/dir",
                "hostname",
                "username",
                "password",
                Path("path/to/working/dir"),
                22,
            ),
            (
                "ftp://username:password@hostname:33/path/to/working/dir",
                "hostname",
                "username",
                "password",
                Path("path/to/working/dir"),
                33,
            ),
            (
                "ftp://username:{}@hostname".format(Path(__file__).as_posix()),
                "hostname",
                "username",
                Path(__file__),
                None,
                22,
            ),
            (
                "ftp://username:{}@hostname/path/to/working/dir".format(Path(__file__).as_posix()),
                "hostname",
                "username",
                Path(__file__),
                Path("path/to/working/dir"),
                22,
            ),
        ]:
            with YieldDataStore(mock.MagicMock(), connection, ssd=False):
                pass

            args, kwargs = mocked_class.Create.call_args

            assert args[1:] == (host, username, key_or_password, working_dir, )
            assert kwargs == {"port": port}

            mocked_class.reset_mock()


# ----------------------------------------------------------------------
class TestCreateFilterFunc(object):
    # ----------------------------------------------------------------------
    def test_Empty(self):
        func = CreateFilterFunc(None, None)
        assert func is None

    # ----------------------------------------------------------------------
    def test_Include(self):
        func = CreateFilterFunc([re.compile("foo/")], None)
        assert func is not None

        assert func(Path("foo/bar")) is True
        assert func(Path("baz/biz")) is False

    # ----------------------------------------------------------------------
    def test_Exclude(self):
        func = CreateFilterFunc(None, [re.compile("foo/")])
        assert func is not None

        assert func(Path("foo/bar")) is False
        assert func(Path("baz/biz")) is True

    # ----------------------------------------------------------------------
    def test_IncludeAndExclude(self):
        func = CreateFilterFunc(
            [re.compile("foo/"), ],
            [re.compile("/two"), ],
        )

        assert func is not None

        assert func(Path("foo/one")) is True
        assert func(Path("foo/two")) is False
        assert func(Path("foo/one/two")) is False


# ----------------------------------------------------------------------
class TestCalculateDiffs(object):
    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("verbose", [False, True])
    def test_Match(self, _snapshot, verbose):
        dm_and_sink = iter(GenerateDoneManagerAndSink(verbose=verbose))

        CalculateDiffs(cast(DoneManager, next(dm_and_sink)), _snapshot, _snapshot)

        sink = cast(str, next(dm_and_sink))

        assert sink == textwrap.dedent(
            """\
            Heading...

              Calculating diffs...DONE! (0, <scrubbed duration>, no diffs found)

            DONE! (0, <scrubbed duration>)
            """,
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("verbose", [False, True])
    def test_Added(
        self,
        _snapshot: Snapshot,
        verbose: bool,
    ) -> None:
        other = copy.deepcopy(_snapshot)

        _snapshot.node.AddFile(Path("FileNew"), "hash", 100)
        _snapshot.node.AddFile(Path("UnknownItemType"), "hash", 0)
        _snapshot.node.AddDir(Path("DirNew"), force=True)

        dm_and_sink = iter(GenerateDoneManagerAndSink(verbose=verbose))

        with _MockPath():
            CalculateDiffs(cast(DoneManager, next(dm_and_sink)), _snapshot, other)

        sink = cast(str, next(dm_and_sink))

        if not verbose:
            assert sink == textwrap.dedent(
                """\
                Heading...

                  Calculating diffs...DONE! (0, <scrubbed duration>, 3 diffs found)

                DONE! (0, <scrubbed duration>)
                """,
            )
        else:
            assert sink == textwrap.dedent(
                """\
                Heading...

                  Calculating diffs...
                    VERBOSE: Adding:
                    VERBOSE:   1) [FILE] FileNew
                    VERBOSE:   2) [????] UnknownItemType
                    VERBOSE:   3) [DIR ] DirNew
                  DONE! (0, <scrubbed duration>, 3 diffs found)

                DONE! (0, <scrubbed duration>)
                """,
            )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("verbose", [False, True])
    def test_Removed(
        self,
        _snapshot: Snapshot,
        verbose: bool,
    ) -> None:
        other = copy.deepcopy(_snapshot)

        _snapshot.node.children["Dir1"].children["Dir2"].children.pop("File3")
        _snapshot.node.children["Dir1"].children["Dir2"].children.pop("Dir3")
        _snapshot.node.children["Dir1"].children.pop("UnknownFileType")

        dm_and_sink = iter(GenerateDoneManagerAndSink(verbose=verbose))

        with _MockPath():
            CalculateDiffs(cast(DoneManager, next(dm_and_sink)), _snapshot, other)

        sink = cast(str, next(dm_and_sink))

        if not verbose:
            assert sink == textwrap.dedent(
                """\
                Heading...

                  Calculating diffs...DONE! (0, <scrubbed duration>, 3 diffs found)

                DONE! (0, <scrubbed duration>)
                """,
            )
        else:
            assert sink == textwrap.dedent(
                """\
                Heading...

                  Calculating diffs...
                    VERBOSE: Removing:
                    VERBOSE:   1) [????] {}
                    VERBOSE:   2) [DIR ] {}
                    VERBOSE:   3) [FILE] {}
                  DONE! (0, <scrubbed duration>, 3 diffs found)

                DONE! (0, <scrubbed duration>)
                """,
            ).format(
                Path("Dir1/UnknownFileType"),
                Path("Dir1/Dir2/Dir3"),
                Path("Dir1/Dir2/File3"),
            )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("verbose", [False, True])
    def test_Modified(
        self,
        _snapshot: Snapshot,
        verbose: bool,
    ) -> None:
        other = copy.deepcopy(_snapshot)

        _snapshot.node.children["Dir1"].children["Dir2"].children["Dir3"].children["File2"].hash_value = "Different hash"
        _snapshot.node.children["Dir1"].children["File5"].hash_value = "Different hash"

        dm_and_sink = iter(GenerateDoneManagerAndSink(verbose=verbose))

        with _MockPath():
            CalculateDiffs(cast(DoneManager, next(dm_and_sink)), _snapshot, other)

        sink = cast(str, next(dm_and_sink))

        if not verbose:
            assert sink == textwrap.dedent(
                """\
                Heading...

                  Calculating diffs...DONE! (0, <scrubbed duration>, 2 diffs found)

                DONE! (0, <scrubbed duration>)
                """,
            )
        else:
            assert sink == textwrap.dedent(
                """\
                Heading...

                  Calculating diffs...
                    VERBOSE: Modifying:
                    VERBOSE:   1) [FILE] {}
                    VERBOSE:   2) [FILE] {}
                  DONE! (0, <scrubbed duration>, 2 diffs found)

                DONE! (0, <scrubbed duration>)
                """,
            ).format(
                Path("Dir1/Dir2/Dir3/File2"),
                Path("Dir1/File5"),
            )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("verbose", [False, True])
    def test_Multiple(
        self,
        _snapshot: Snapshot,
        verbose: bool,
    ) -> None:
        other = copy.deepcopy(_snapshot)

        _snapshot.node.children["Dir1"].children["Dir2"].children["File3"].hash_value = "Different hash"
        _snapshot.node.children["Dir1"].children["File5"].hash_value = "Different hash"
        _snapshot.node.AddFile(Path("FileAdded"), "hash", 100)
        _snapshot.node.children["Dir1"].children["Dir2"].children.pop("Dir3")
        _snapshot.node.children["Dir1"].children.pop("File6")

        dm_and_sink = iter(GenerateDoneManagerAndSink(verbose=verbose))

        with _MockPath():
            CalculateDiffs(cast(DoneManager, next(dm_and_sink)), _snapshot, other)

        sink = cast(str, next(dm_and_sink))

        if not verbose:
            assert sink == textwrap.dedent(
                """\
                Heading...

                  Calculating diffs...DONE! (0, <scrubbed duration>, 5 diffs found)

                DONE! (0, <scrubbed duration>)
                """,
            )
        else:
            assert sink == textwrap.dedent(
                """\
                Heading...

                  Calculating diffs...
                    VERBOSE: Adding:
                    VERBOSE:   1) [FILE] FileAdded
                    VERBOSE:
                    VERBOSE: Modifying:
                    VERBOSE:   1) [FILE] {}
                    VERBOSE:   2) [FILE] {}
                    VERBOSE:
                    VERBOSE: Removing:
                    VERBOSE:   1) [FILE] {}
                    VERBOSE:   2) [DIR ] {}
                  DONE! (0, <scrubbed duration>, 5 diffs found)

                DONE! (0, <scrubbed duration>)
                """,
            ).format(
                Path("Dir1/Dir2/File3"),
                Path("Dir1/File5"),
                Path("Dir1/File6"),
                Path("Dir1/Dir2/Dir3"),
            )


# ----------------------------------------------------------------------
class TestValidateSizeRequirements(object):
    # ----------------------------------------------------------------------
    def test_None(self, _diffs):
        dm_and_sink = iter(GenerateDoneManagerAndSink(verbose=False))

        with self.__class__._YieldMockDataStore(None) as data_store:
            ValidateSizeRequirements(cast(DoneManager, next(dm_and_sink)), data_store, data_store, _diffs)

        sink = cast(str, next(dm_and_sink))

        assert sink == textwrap.dedent(
            """\
            Heading...DONE! (0, <scrubbed duration>)
            """,
        )

    # ----------------------------------------------------------------------
    def test_EnoughSpace(self, _diffs):
        dm_and_sink = iter(GenerateDoneManagerAndSink(verbose=False))

        with self.__class__._YieldMockDataStore(2000000) as data_store:
            ValidateSizeRequirements(cast(DoneManager, next(dm_and_sink)), data_store, data_store, _diffs)

        sink = cast(str, next(dm_and_sink))

        assert sink == textwrap.dedent(
            """\
            Heading...
              Validating size requirements...
                INFO: The local file 'UnknownItemType' is no longer available.
              DONE! (0, <scrubbed duration>, 54 KB required, 2 MB available)
            DONE! (0, <scrubbed duration>)
            """,
        )

    # ----------------------------------------------------------------------
    def test_NoEnoughSpace(self, _diffs):
        dm_and_sink = iter(GenerateDoneManagerAndSink(verbose=False, expected_result=-1))

        with self.__class__._YieldMockDataStore(200) as data_store:
            ValidateSizeRequirements(cast(DoneManager, next(dm_and_sink)), data_store, data_store, _diffs)

        sink = cast(str, next(dm_and_sink))

        assert sink == textwrap.dedent(
            """\
            Heading...
              Validating size requirements...
                INFO: The local file 'UnknownItemType' is no longer available.
                ERROR: There is not enough disk space to process this request.
              DONE! (-1, <scrubbed duration>, 54 KB required, 1 KB available)
            DONE! (-1, <scrubbed duration>)
            """,
        )

    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    @staticmethod
    @pytest.fixture
    def _diffs(_snapshot):
        return [
            DiffResult(DiffOperation.add, Path("File1"), "hash1", 1, None, None),
            DiffResult(DiffOperation.add, Path("File20"), "hash2", 20, None, None),
            DiffResult(DiffOperation.add, Path("File300"), "hash3", 300, None, None),
            DiffResult(DiffOperation.add, Path("File4000"), "hash4", 4000, None, None),
            DiffResult(DiffOperation.add, Path("File50000"), "hash5", 50000, None, None),
            DiffResult(DiffOperation.add, Path("Dir"), DirHashPlaceholder(explicitly_added=True), None, None, None),
            DiffResult(DiffOperation.add, Path("UnknownItemType"), "unknown", 0, None, None),
        ]

    # ----------------------------------------------------------------------
    @staticmethod
    @contextmanager
    def _YieldMockDataStore(
        bytes_available: Optional[int],
    ) -> Iterator[Any]:
        data_store = mock.MagicMock()

        data_store.GetBytesAvailable.return_value = bytes_available

        # ----------------------------------------------------------------------
        def GetItemType(value) -> Optional[ItemType]:
            if value.name.startswith("File"):
                return ItemType.File

            if value.name.startswith("Dir"):
                return ItemType.Dir

            return None

        # ----------------------------------------------------------------------

        data_store.GetItemType.side_effect = GetItemType

        # ----------------------------------------------------------------------
        get_file_size_regex = re.compile(r"File(?P<value>\d+)")

        def GetFileSize(value) -> int:
            match = get_file_size_regex.match(value.name)
            assert match is not None, value

            return int(match.group("value"))

        # ----------------------------------------------------------------------

        data_store.GetFileSize.side_effect = GetFileSize

        yield data_store


# ----------------------------------------------------------------------
class TestWriteFile(object):
    # ----------------------------------------------------------------------
    def test_Standard(self, tmp_path_factory):
        root = tmp_path_factory.mktemp("temp")

        with self.__class__._YieldMockDataStore(root) as (source_filename, store):
            dest_filename = root / "DestFilename.txt"

            WriteFile(store, source_filename, dest_filename, lambda _: None)

            assert dest_filename.is_file(), dest_filename
            assert CalculateHash(store, dest_filename, lambda _: None)

    # ----------------------------------------------------------------------
    def test_Failure(self, tmp_path_factory):
        root = tmp_path_factory.mktemp("temp")

        with self.__class__._YieldMockDataStore(root) as (source_filename, store):
            with mock.patch.object(store, "Open") as open_mock:
                open_mock().__enter__().write.side_effect = Exception("Forced exception")

                # Invoke
                dest_filename = root / "DestFilename.txt"

                with pytest.raises(Exception, match="Forced exception"):
                    WriteFile(store, source_filename, dest_filename, lambda _:None)

                assert not dest_filename.exists(), dest_filename

    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    @staticmethod
    @contextmanager
    def _YieldMockDataStore(
        root: Path,
    ) -> Iterator[Tuple[Path, DataStore]]:
        source_filename = root / "SourceFilename.txt"

        with source_filename.open("w") as f:
            f.write("This is a test")

        yield source_filename, FileSystemDataStore(root)


# ----------------------------------------------------------------------
def test_CreateDestinationPathFuncFactory():
    func = CreateDestinationPathFuncFactory()

    if CurrentShell.family_name == "Windows":
        assert func(Path("C:/one/two/three"), ".foo") == Path("C_/one/two/three.foo")
        assert func(Path("C:/one/two/three"), ".bar") == Path("C_/one/two/three.bar")
    else:
        assert func(Path("/one/two/three"), ".foo") == Path("one/two/three.foo")
        assert func(Path("/one/two/three"), ".bar") == Path("one/two/three.bar")


# ----------------------------------------------------------------------
class TestCopyLocalContent(object):
    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("ssd", [False, True])
    @pytest.mark.parametrize("quiet", [False, True])
    @pytest.mark.parametrize("verbose", [False, True])
    def test_CopyLocalContent(self, verbose, quiet, ssd, _local_file_info, tmp_path_factory):
        root, diffs = _local_file_info

        destination = tmp_path_factory.mktemp("destination")

        dm_and_sink = iter(GenerateDoneManagerAndSink(verbose=verbose, expected_result=0))

        destination_path_func = CreateDestinationPathFuncFactory()

        results = CopyLocalContent(
            cast(DoneManager, next(dm_and_sink)),
            FileSystemDataStore(destination),
            diffs,
            destination_path_func,
            quiet=quiet,
            ssd=ssd,
        )

        assert results == [
            destination_path_func(Path(root / "File1"), PENDING_COMMIT_EXTENSION),
            destination_path_func(Path(root / "File2"), PENDING_COMMIT_EXTENSION),
            destination_path_func(Path(root / "File3"), PENDING_COMMIT_EXTENSION),
            destination_path_func(Path(root / "Dir1"), PENDING_COMMIT_EXTENSION),
            destination_path_func(Path(root / "Dir2"), PENDING_COMMIT_EXTENSION),
        ]

        sink = cast(str, next(dm_and_sink))

        assert sink == textwrap.dedent(
            """\
            Heading...
              Processing 5 items...


              DONE! (0, <scrubbed duration>, 5 items succeeded, no items with errors, no items with warnings)
            DONE! (0, <scrubbed duration>)
            """,
        )

    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    @staticmethod
    @pytest.fixture(scope="class")
    def _local_file_info(tmp_path_factory) -> Tuple[Path, List["DiffResult"]]:
        root = tmp_path_factory.mktemp("root")

        filenames = ["File1", "File2", "File3"]
        directories = ["Dir1", "Dir2"]

        for filename in filenames:
            with (root / filename).open("w") as f:
                f.write(filename)

        for directory in directories:
            (root / directory).mkdir(parents=True)

        return (
            root,
            [
                DiffResult(
                    DiffOperation.add,
                    root / filename,
                    str(filename_index),
                    filename_index,
                    None,
                    None,
                )
                for filename_index, filename in enumerate(filenames)
            ] + [
                DiffResult(
                    DiffOperation.add,
                    root / directory,
                    DirHashPlaceholder(explicitly_added=True),
                    None,
                    None,
                    None,
                )
                for directory in directories
            ],
        )


# ----------------------------------------------------------------------
def test_CalculateHash():
    store = mock.MagicMock()

    with mock.patch.object(store, "Open") as open_mock:
        open_mock().__enter__().read.side_effect = [
            "abcdef".encode("utf-8"),
            None,
            "abcdef".encode("utf-8"),
            None,
        ]

        hash1 = CalculateHash(store, Path(), lambda _: None)
        hash2 = CalculateHash(store, Path(), lambda _: None)

        assert hash1 == hash2

    with mock.patch.object(store, "Open") as open_mock:
        open_mock().__enter__().read.side_effect = ["abcdef_".encode("utf-8"), None]

        hash3 = CalculateHash(store, Path(), lambda _: None)
        assert hash3 != hash1


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
@pytest.fixture()
def _snapshot() -> Snapshot:
    root_node = Snapshot.Node(None, None, DirHashPlaceholder(explicitly_added=False), None)

    root_node.AddFile(Path("Dir1/Dir2/Dir3/File1"), "File1", 1)
    root_node.AddFile(Path("Dir1/Dir2/Dir3/File2"), "File2", 2)
    root_node.AddFile(Path("Dir1/Dir2/File3"), "File3", 3)
    root_node.AddFile(Path("Dir1/Dir2/File4"), "File4", 4)
    root_node.AddFile(Path("Dir1/File5"), "File5", 5)
    root_node.AddFile(Path("Dir1/File6"), "File6", 6)

    # This is a file, but the name doesn't conform to our mocked file or dir detection. This
    # will help verify scenarios where the file type is unknown.
    root_node.AddFile(Path("Dir1/UnknownFileType"), "Unknown", 0)

    return Snapshot(root_node)


# ----------------------------------------------------------------------
@contextmanager
def _MockPath() -> Iterator[None]:
    with mock.patch.object(Path, "is_file", lambda value: value.name.startswith("File") or os.path.isfile(value)):
        with mock.patch.object(Path, "is_dir", lambda value: value.name.startswith("Dir") or os.path.isdir(value)):
            yield
