# ----------------------------------------------------------------------
# |
# |  Snapshot_UnitTest.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-10-20 12:34:44
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Unit tests for Snapshot.py"""

import copy
import json
import os
import re
import sys

from io import StringIO
from pathlib import Path
from unittest import mock

import pytest

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx
from Common_Foundation.Shell.All import CurrentShell
from Common_Foundation.Streams.DoneManager import DoneManager


# ----------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
with ExitStack(lambda: sys.path.pop(0)):
    from Backup.Impl.Capabilities.FileSystemCapabilities import FileSystemCapabilities
    from Backup.Impl.Snapshot import Snapshot


# ----------------------------------------------------------------------
class TestCalculate(object):
    # ----------------------------------------------------------------------
    @classmethod
    @pytest.fixture(scope="function")
    def local_working_dir(cls, tmp_path_factory):
        root = tmp_path_factory.mktemp("root")

        _MakeFile(root, root / "StableFile1")
        _MakeFile(root, root / "DisappearingFile")
        _MakeFile(root, root / "StableFile3")

        return root

    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_Single(self, _working_dir, run_in_parallel):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "one",
            ],
            FileSystemCapabilities(_working_dir),
            run_in_parallel=run_in_parallel,
        )

        assert result.node == Snapshot.Node.Create(
            {
                _working_dir / "one" / "A": ("38818bc4ba444583f537b9ed36a2fb4e7fd49694efd4a06b8fe0c1b00161e904f4edb7a9713543b74f283261d3000671b6c0567d6abea2b19686870d8b344b4e", 5),
                _working_dir / "one" / "BC": ("7abfea86ce9ef5721ccea68e560d879bfd76ec3c4fa26c91ecbde49754ddebfa8ac92e45fa9ecae5f373b3baa761c4921feceb1eb62cd9b9dedcf9178f089958", 6),
            },
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_Multiple(self, _working_dir, run_in_parallel):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "two",
                _working_dir / "one",
            ],
            FileSystemCapabilities(_working_dir),
            run_in_parallel=run_in_parallel,
        )

        assert result.node == Snapshot.Node.Create(
            {
                _working_dir / "one" / "A": ("38818bc4ba444583f537b9ed36a2fb4e7fd49694efd4a06b8fe0c1b00161e904f4edb7a9713543b74f283261d3000671b6c0567d6abea2b19686870d8b344b4e", 5),
                _working_dir / "one" / "BC": ("7abfea86ce9ef5721ccea68e560d879bfd76ec3c4fa26c91ecbde49754ddebfa8ac92e45fa9ecae5f373b3baa761c4921feceb1eb62cd9b9dedcf9178f089958", 6),
                _working_dir / "two" / "File1": ("15703c6965d528c208f42bd8ca9d63a7ea4409652c5587734a233d875e7d1ee3f99510a238abfb83b45c51213535c98dede44222dc8596046da0b63078a55675", 9),
                _working_dir / "two" / "File2": ("0a8abfedd9153b65e90a93692bec11b14c36ddb7448a0b7bd61d0c9269693120b417a7872f552f0e274d7d7367ed41d5b7e8a84991266da4fcd53ee775420c5a", 9),
                _working_dir / "two" / "Dir1/File3": ("d919d6b3367c051892e6de02c85f7315067b813fa6aef32c9780a23ac0bb9fa63c0db2d1b5f5337114bcd9d7aea809cf1b21034bbb0a33d25eb7f53b55b9be9d", 14),
                _working_dir / "two" / "Dir1/File4": ("b4d800b7c6c78cf3248849d333925fea1c3063677461056d93854c47d578fbbea3aaa7f905edd2e2c79b3cc837e5f851d38703bfe68a3db3aaf6b2d012d1b221", 14),
                _working_dir / "two" / "Dir2/Dir3/File5": ("1d3fb9708c4737f09cf99ece9660e1a73d83296a8744d3e59ffe5bb781c3aed0ee7db6dabc075d280e1a61abf2d75d9b1102edb1fd210efd73ad12911f3cf408", 19),
            }
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_SingleWithoutHashes(self, _working_dir, run_in_parallel):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "one",
            ],
            FileSystemCapabilities(_working_dir),
            run_in_parallel=run_in_parallel,
            calculate_hashes=False,
        )

        assert result.node == Snapshot.Node.Create(
            {
                _working_dir / "one" / "A": ("ignored", 5),
                _working_dir / "one" / "BC": ("ignored", 6),
            },
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_FileInput(self, _working_dir, run_in_parallel):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "one" / "A",
            ],
            FileSystemCapabilities(_working_dir),
            run_in_parallel=run_in_parallel,
        )

        assert result.node == Snapshot.Node.Create(
            {
                _working_dir / "one" / "A": ("38818bc4ba444583f537b9ed36a2fb4e7fd49694efd4a06b8fe0c1b00161e904f4edb7a9713543b74f283261d3000671b6c0567d6abea2b19686870d8b344b4e", 5),
            },
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_MultipleWithoutHashes(self, _working_dir, run_in_parallel):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "two",
                _working_dir / "one",
            ],
            FileSystemCapabilities(_working_dir),
            run_in_parallel=run_in_parallel,
            calculate_hashes=False,
        )

        assert result.node == Snapshot.Node.Create(
            {
                _working_dir / "one" / "A": ("ignored", 5),
                _working_dir / "one" / "BC": ("ignored", 6),
                _working_dir / "two" / "File1": ("ignored", 9),
                _working_dir / "two" / "File2": ("ignored", 9),
                _working_dir / "two" / "Dir1/File3": ("ignored", 14),
                _working_dir / "two" / "Dir1/File4": ("ignored", 14),
                _working_dir / "two" / "Dir2/Dir3/File5": ("ignored", 19),
            }
        )

    # ----------------------------------------------------------------------
    def test_LongPaths(self, _working_dir):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "VeryLongPaths",
            ],
            FileSystemCapabilities(_working_dir),
            run_in_parallel=False,
        )

        assert result.node == Snapshot.Node.Create(
            {
                _working_dir / "VeryLongPaths" / ("1" * 200): ("b4b83b4558520fe120e24513a783e3a9c98d75d381f2b93d468538266918c2c6a0735a775b26f8c48f1317cf08a7c13c069de7307bc2700811cc093cdc10ccb3", 214),
                _working_dir / "VeryLongPaths" / ("2" * 201): ("70130b445ff3a02cac2e4c864743105e8d975253f758afee619993b77c913dd0faeae37f98bfb4a36da5e455a04f7fc3879fe3db091edd8baf9e0627ef75803e", 215),
            },
        )

    # ----------------------------------------------------------------------
    def test_EmptyDir(self, _working_dir):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "EmptyDirTest",
            ],
            FileSystemCapabilities(_working_dir),
            run_in_parallel=False,
        )

        assert result.node == Snapshot.Node.Create(
            {
                _working_dir / "EmptyDirTest" / "EmptyDir": None,
            },
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_WithFilterFilenameFunc(self, _working_dir, run_in_parallel):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "two",
                _working_dir / "one",
            ],
            FileSystemCapabilities(_working_dir),
            run_in_parallel=run_in_parallel,
            filter_filename_func=lambda value: value != (_working_dir / "two" / "Dir1" / "File3"),
        )

        assert result.node == Snapshot.Node.Create(
            {

                _working_dir / "one" / "A": ("38818bc4ba444583f537b9ed36a2fb4e7fd49694efd4a06b8fe0c1b00161e904f4edb7a9713543b74f283261d3000671b6c0567d6abea2b19686870d8b344b4e", 5),
                _working_dir / "one" / "BC": ("7abfea86ce9ef5721ccea68e560d879bfd76ec3c4fa26c91ecbde49754ddebfa8ac92e45fa9ecae5f373b3baa761c4921feceb1eb62cd9b9dedcf9178f089958", 6),
                _working_dir / "two" / "File1": ("15703c6965d528c208f42bd8ca9d63a7ea4409652c5587734a233d875e7d1ee3f99510a238abfb83b45c51213535c98dede44222dc8596046da0b63078a55675", 9),
                _working_dir / "two" / "File2": ("0a8abfedd9153b65e90a93692bec11b14c36ddb7448a0b7bd61d0c9269693120b417a7872f552f0e274d7d7367ed41d5b7e8a84991266da4fcd53ee775420c5a", 9),
                _working_dir / "two" / "Dir1/File4": ("b4d800b7c6c78cf3248849d333925fea1c3063677461056d93854c47d578fbbea3aaa7f905edd2e2c79b3cc837e5f851d38703bfe68a3db3aaf6b2d012d1b221", 14),
                _working_dir / "two" / "Dir2/Dir3/File5": ("1d3fb9708c4737f09cf99ece9660e1a73d83296a8744d3e59ffe5bb781c3aed0ee7db6dabc075d280e1a61abf2d75d9b1102edb1fd210efd73ad12911f3cf408", 19),
            },
        )

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("run_in_parallel", [False, True])
    def test_WithFilterFilenameFuncNoMatches(self, _working_dir, run_in_parallel):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "two",
                _working_dir / "one",
            ],
            FileSystemCapabilities(_working_dir),
            run_in_parallel=run_in_parallel,
            filter_filename_func=lambda value: False,
        )

        assert result.node == Snapshot.Node.Create({})

    # ----------------------------------------------------------------------
    def test_ErrorDuringCalculation(self, _working_dir):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = -1

        with pytest.raises(Exception, match=re.escape("Errors encountered when calculating files.")):
            Snapshot.Calculate(
                dm_mock,
                [_working_dir / "one"],
                FileSystemCapabilities(_working_dir),
                run_in_parallel=False,
            )

    # ----------------------------------------------------------------------
    def test_ErrorDuringHashing(self, _working_dir):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__.configure_mock(
            side_effect=[
                mock.MagicMock(result=0),
                mock.MagicMock(result=-1),
            ],
        )

        with pytest.raises(Exception, match=re.escape("Errors encountered when hashing files.")):
            Snapshot.Calculate(
                dm_mock,
                [_working_dir / "one"],
                FileSystemCapabilities(_working_dir),
                run_in_parallel=False,
            )

    # ----------------------------------------------------------------------
    def test_FileDisappears(self, local_working_dir):
        dm_mock = mock.MagicMock()

        nested_mock = mock.MagicMock(result=0)
        nested_mock.Nested().__enter__().result = 0

        call_count = 0

        # ----------------------------------------------------------------------
        def Func():
            nonlocal call_count

            call_count += 1

            if call_count == 2:
                disappearing_file = local_working_dir / "DisappearingFile"

                assert disappearing_file.is_file()
                disappearing_file.unlink()

            return nested_mock

        # ----------------------------------------------------------------------

        dm_mock.Nested().__enter__.configure_mock(side_effect=Func)

        result = Snapshot.Calculate(
            dm_mock,
            [
                local_working_dir,
            ],
            FileSystemCapabilities(local_working_dir),
            run_in_parallel=False,
        )

        assert result.node == Snapshot.Node.Create(
            {
                local_working_dir / "StableFile1": ("8107a23a413c1095854083ddf343a80d99d385484ffd9c166ca1979e0acfdfef9b0eb47d6211b558caf856711623fadc6413fbea7ca26e3f7c2641a3530d6c14", 11),
                local_working_dir / "StableFile3": ("247804a38c4ab666e6f83f673c321d0db6e816a8a0e2463e26a3841953e5ba191231f37099abe709e8157310891dfbaa2fe55294ed993f162d54625b0df2bf39", 11),
            },
        )

    # ----------------------------------------------------------------------
    def test_DoesNotExistError(self):
        with pytest.raises(
            Exception,
            match=re.escape("'{}' is not a valid file or directory.".format(Path("one/two/three"))),
        ):
            Snapshot.Calculate(
                mock.MagicMock(),
                [
                    Path("one/two/three"),
                ],
                FileSystemCapabilities(Path()),
                run_in_parallel=False,
            )

    # ----------------------------------------------------------------------
    @mock.patch.object(Path, "is_dir")
    @mock.patch.object(Path, "exists")
    def test_CalculateOverlapError(self, mocked_is_dir, mocked_exists):
        mocked_is_dir.return_value = True
        mocked_exists.return_value = True

        with pytest.raises(
            Exception,
            match=re.escape("The input '{}' overlaps with '{}'.".format(Path("one/two/three"), Path("one"))),
        ):
            Snapshot.Calculate(
                mock.MagicMock(),
                [
                    Path("one/two/three"),
                    Path("one"),
                ],
                FileSystemCapabilities(Path()),
                run_in_parallel=False,
            )

    # ----------------------------------------------------------------------
    def test_UnsupportedFileType(self, _working_dir):
        os.symlink(_working_dir / "two" / "File1", _working_dir / "two" / "symFile")
        os.symlink(_working_dir / "two" / "Dir1", _working_dir / "two" / "symDir", target_is_directory=True)

        sink = StringIO()

        with DoneManager.Create(sink, "") as dm:
            result = Snapshot.Calculate(
                dm,
                [
                    _working_dir / "two",
                ],
                FileSystemCapabilities(_working_dir),
                run_in_parallel=False,
            )

        sink = sink.getvalue()

        assert result.node == Snapshot.Node.Create(
            {
                _working_dir / "two" / "File1": ("15703c6965d528c208f42bd8ca9d63a7ea4409652c5587734a233d875e7d1ee3f99510a238abfb83b45c51213535c98dede44222dc8596046da0b63078a55675", 9),
                _working_dir / "two" / "File2": ("0a8abfedd9153b65e90a93692bec11b14c36ddb7448a0b7bd61d0c9269693120b417a7872f552f0e274d7d7367ed41d5b7e8a84991266da4fcd53ee775420c5a", 9),
                _working_dir / "two" / "Dir1/File3": ("d919d6b3367c051892e6de02c85f7315067b813fa6aef32c9780a23ac0bb9fa63c0db2d1b5f5337114bcd9d7aea809cf1b21034bbb0a33d25eb7f53b55b9be9d", 14),
                _working_dir / "two" / "Dir1/File4": ("b4d800b7c6c78cf3248849d333925fea1c3063677461056d93854c47d578fbbea3aaa7f905edd2e2c79b3cc837e5f851d38703bfe68a3db3aaf6b2d012d1b221", 14),
                _working_dir / "two" / "Dir1/File4": ("b4d800b7c6c78cf3248849d333925fea1c3063677461056d93854c47d578fbbea3aaa7f905edd2e2c79b3cc837e5f851d38703bfe68a3db3aaf6b2d012d1b221", 14),
                _working_dir / "two" / "Dir2/Dir3/File5": ("1d3fb9708c4737f09cf99ece9660e1a73d83296a8744d3e59ffe5bb781c3aed0ee7db6dabc075d280e1a61abf2d75d9b1102edb1fd210efd73ad12911f3cf408", 19),
            },
        )


# ----------------------------------------------------------------------
class TestPersistAndLoad(object):
    # ----------------------------------------------------------------------
    @pytest.mark.filterwarnings('ignore:install "ipywidgets" for Jupyter support')
    def test(self, _working_dir, tmp_path):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir,
            ],
            FileSystemCapabilities(_working_dir),
            run_in_parallel=False,
        )

        assert result.node

        capabilities = FileSystemCapabilities(tmp_path)

        assert result.IsPersisted(capabilities) is False
        result.Persist(dm_mock, capabilities)
        assert result.IsPersisted(capabilities) is True

        loaded_result = Snapshot.LoadPersisted(dm_mock, capabilities)

        assert loaded_result is not result
        assert loaded_result == result

    # ----------------------------------------------------------------------
    @pytest.mark.filterwarnings('ignore:install "ipywidgets" for Jupyter support')
    def test_InvalidJson(self, _working_dir, tmp_path):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir,
            ],
            FileSystemCapabilities(_working_dir),
            run_in_parallel=False,
        )

        assert result.node

        capabilities = FileSystemCapabilities(tmp_path)

        assert result.IsPersisted(capabilities) is False
        result.Persist(dm_mock, capabilities)
        assert result.IsPersisted(capabilities) is True

        json_filename = tmp_path / Snapshot.PERSISTED_FILE_NAME
        assert json_filename.is_file(), json_filename

        with json_filename.open() as f:
            original_content = json.load(f)

        original_content.pop("children")

        with json_filename.open("w") as f:
            json.dump(original_content, f)

        with pytest.raises(
            Exception,
            match=re.compile(r"The content at '.+?' is not valid\."),
        ):
            Snapshot.LoadPersisted(dm_mock, capabilities)


# ----------------------------------------------------------------------
class TestDiff(object):
    # ----------------------------------------------------------------------
    @staticmethod
    @pytest.fixture
    def snapshot():
        return Snapshot(
            Snapshot.Node.Create(
                {
                    Path("one") / "100": ("hash(100)", 100),
                    Path("one") / "101": ("hash(101)", 101),
                    Path("one") / "102": ("hash(102)", 102),

                    Path("two") / "200": ("hash(200)", 200),
                    Path("two") / "201": ("hash(201)", 201),
                    Path("two") / "202": ("hash(202)", 202),

                    Path("two") / "dir_no_content": None,

                    Path("two") / "dir_with_content" / "child1": ("hash(dir_with_content/child1)", 2001),
                    Path("two") / "dir_with_content" / "child2": ("hash(dir_with_content/child2)", 2002),

                    Path("two") / "nested" / "one" / "two" / "file1": ("hash(nested/one/two/file1)", 20001),
                    Path("two") / "nested" / "one" / "two" / "file2": ("hash(nested/one/two/file2)", 20002),

                    Path("three"): None,
                },
            ),
        )

    # ----------------------------------------------------------------------
    @staticmethod
    @pytest.fixture
    def other(snapshot):
        return copy.deepcopy(snapshot)

    # ----------------------------------------------------------------------
    def test_Same(self, snapshot, other):
        assert other is not snapshot
        assert other == snapshot

        assert list(snapshot.Diff(other)) == []
        assert list(other.Diff(snapshot)) == []

    # ----------------------------------------------------------------------
    def test_RootItem(self, snapshot, other):
        assert other == snapshot

        del other.node.children["two"]
        assert other != snapshot

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "200",
                "hash(200)",
                200,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "201",
                "hash(201)",
                201,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "202",
                "hash(202)",
                202,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "dir_no_content",
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "dir_with_content" / "child1",
                "hash(dir_with_content/child1)",
                2001,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "dir_with_content" / "child2",
                "hash(dir_with_content/child2)",
                2002,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "nested" / "one" / "two" / "file1",
                "hash(nested/one/two/file1)",
                20001,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "nested" / "one" / "two" / "file2",
                "hash(nested/one/two/file2)",
                20002,
                None,
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("two"),
                None,
                None,
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_EmptyRoot(self, snapshot, other):
        assert snapshot == other

        del other.node.children["three"]
        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("three"),
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
                None,
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("three"),
                None,
                None,
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_AddedFile(self, snapshot, other):
        assert snapshot == other

        other.node.AddFile(Path("three") / "300", "hash(300)", 300)

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("three") / "300",
                None,
                None,
                "hash(300)",
                300,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("three") / "300",
                "hash(300)",
                300,
                None,
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_AddedRootDirectory(self, snapshot, other):
        assert snapshot == other

        other.node.AddDir(Path("new_dir"))

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("new_dir"),
                None,
                None,
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("new_dir"),
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
                None,
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_AddedNestedDirectory(self, snapshot, other):
        assert snapshot == other

        other.node.AddDir(Path("three") / "new_dir")

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("three") / "new_dir",
                None,
                None,
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("three") / "new_dir",
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
                None,
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_Modified(self, snapshot, other):
        assert snapshot == other

        other.node.children["one"].children["100"].hash_value = "Modified(100)"
        other.node.children["one"].children["102"].hash_value = "Modified(102)"
        other.node.children["two"].children["201"].hash_value = "Modified(201)"

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modify,
                Path("one") / "100",
                "hash(100)",
                100,
                "Modified(100)",
                100,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modify,
                Path("one") / "102",
                "hash(102)",
                102,
                "Modified(102)",
                102,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modify,
                Path("two") / "201",
                "hash(201)",
                201,
                "Modified(201)",
                201,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modify,
                Path("one") / "100",
                "Modified(100)",
                100,
                "hash(100)",
                100,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modify,
                Path("one") / "102",
                "Modified(102)",
                102,
                "hash(102)",
                102,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modify,
                Path("two") / "201",
                "Modified(201)",
                201,
                "hash(201)",
                201,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_ModifiedWithoutComparingHashes(self, snapshot, other):
        assert snapshot == other

        other.node.children["one"].children["100"].hash_value = "different_hash_value"

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modify,
                Path("one") / "100",
                "hash(100)",
                100,
                "different_hash_value",
                100,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modify,
                Path("one") / "100",
                "different_hash_value",
                100,
                "hash(100)",
                100,
            ),
        ]

        assert list(snapshot.Diff(other, compare_hashes=False)) == []
        assert list(other.Diff(snapshot, compare_hashes=False)) == []

        other.node.children["one"].children["100"].file_size += 1

        assert list(snapshot.Diff(other, compare_hashes=False)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modify,
                Path("one") / "100",
                "hash(100)",
                100,
                "different_hash_value",
                101,
            ),
        ]

        assert list(other.Diff(snapshot, compare_hashes=False)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modify,
                Path("one") / "100",
                "different_hash_value",
                101,
                "hash(100)",
                100,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_RemoveRootWithoutContent(self, snapshot, other):
        assert snapshot == other

        other.node.children.pop("three")

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("three"),
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
                None,
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("three"),
                None,
                None,
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_RemoveRootWithContent(self, snapshot, other):
        assert snapshot == other

        other.node.children.pop("two")

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "200",
                "hash(200)",
                200,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "201",
                "hash(201)",
                201,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "202",
                "hash(202)",
                202,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "dir_no_content",
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "dir_with_content" / "child1",
                "hash(dir_with_content/child1)",
                2001,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "dir_with_content" / "child2",
                "hash(dir_with_content/child2)",
                2002,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "nested" / "one" / "two" / "file1",
                "hash(nested/one/two/file1)",
                20001,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "nested" / "one" / "two" / "file2",
                "hash(nested/one/two/file2)",
                20002,
                None,
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("two"),
                None,
                None,
                Snapshot.DirHashPlaceholder(explicitly_added=False),
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_RemoveDirWithNoContent(self, snapshot, other):
        assert snapshot == other

        other.node.children["two"].children.pop("dir_no_content")

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "dir_no_content",
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
                None,
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("two") / "dir_no_content",
                None,
                None,
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_RemoveDirWithContent(self, snapshot, other):
        assert snapshot == other

        other.node.children["two"].children.pop("dir_with_content")

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "dir_with_content" / "child1",
                "hash(dir_with_content/child1)",
                2001,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "dir_with_content" / "child2",
                "hash(dir_with_content/child2)",
                2002,
                None,
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("two") / "dir_with_content",
                None,
                None,
                Snapshot.DirHashPlaceholder(explicitly_added=False),
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_RemovePartialDirContent(self, snapshot, other):
        assert snapshot == other

        other.node.children["two"].children["dir_with_content"].children.pop("child2")

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "dir_with_content" / "child2",
                "hash(dir_with_content/child2)",
                2002,
                None,
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("two") / "dir_with_content" / "child2",
                None,
                None,
                "hash(dir_with_content/child2)",
                2002,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_RemoveNestedDirWithContent(self, snapshot, other):
        assert snapshot == other

        other.node.children["two"].children["nested"].children["one"].children.pop("two")

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "nested" / "one" / "two" / "file1",
                "hash(nested/one/two/file1)",
                20001,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "nested" / "one" / "two" / "file2",
                "hash(nested/one/two/file2)",
                20002,
                None,
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("two"),
                None,
                None,
                Snapshot.DirHashPlaceholder(explicitly_added=False),
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_RemovePartialNestedDirContent(self, snapshot, other):
        assert snapshot == other

        other.node.children["two"].children["nested"].children["one"].children["two"].children.pop("file1")

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "nested" / "one" / "two" / "file1",
                "hash(nested/one/two/file1)",
                20001,
                None,
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("two") / "nested" / "one" / "two" / "file1",
                None,
                None,
                "hash(nested/one/two/file1)",
                20001,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_RemoveFileKeepDir(self, snapshot, other):
        assert snapshot == other

        snapshot.node.AddDir(Path("two") / "nested" / "one" / "two", force=True)

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("two") / "nested" / "one" / "two" / "file1",
                None,
                None,
                "hash(nested/one/two/file1)",
                20001,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("two") / "nested" / "one" / "two" / "file2",
                None,
                None,
                "hash(nested/one/two/file2)",
                20002,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "nested" / "one" / "two" / "file1",
                "hash(nested/one/two/file1)",
                20001,
                None,
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("two") / "nested" / "one" / "two" / "file2",
                "hash(nested/one/two/file2)",
                20002,
                None,
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_TypeConversion(self, snapshot, other):
        assert snapshot == other

        other.node.AddDir(Path("one") / "100", force=True)

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("one") / "100",
                None,
                None,
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("one") / "100",
                "hash(100)",
                100,
                None,
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.remove,
                Path("one") / "100",
                None,
                None,
                "hash(100)",
                100,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.add,
                Path("one") / "100",
                Snapshot.DirHashPlaceholder(explicitly_added=True),
                None,
                None,
                None,
            ),
        ]


# ----------------------------------------------------------------------
class TestDirHashPlaceholder(object):
    # ----------------------------------------------------------------------
    def test_Equal(self):
        assert Snapshot.DirHashPlaceholder(explicitly_added=True) == Snapshot.DirHashPlaceholder(explicitly_added=False)
        assert Snapshot.DirHashPlaceholder(explicitly_added=True) != "Different type"


# ----------------------------------------------------------------------
class TestNodeProperties(object):
    # ----------------------------------------------------------------------
    def test_IsFile(self):
        node = Snapshot.Node(None, None, "simulated hash", 1)
        assert node.is_file
        assert not node.is_dir

    # ----------------------------------------------------------------------
    def test_IsDir(self):
        node = Snapshot.Node(None, None, Snapshot.DirHashPlaceholder(explicitly_added=True), None)
        assert not node.is_file
        assert node.is_dir

    # ----------------------------------------------------------------------
    def test_Fullpath(self):
        node = Snapshot.Node(None, None, Snapshot.DirHashPlaceholder(explicitly_added=True), None)

        assert node.fullpath == Path("")

        node.AddDir(Path("one") / "two" / "three")

        assert node.children["one"].children["two"].children["three"].is_dir
        assert node.children["one"].children["two"].children["three"].fullpath == Path("one") / "two" / "three"

    # ----------------------------------------------------------------------
    def test_Enum(self):
        root = Snapshot.Node(None, None, Snapshot.DirHashPlaceholder(explicitly_added=False), None)

        root.AddFile(Path("one/file1"), "file1", 1)
        root.AddFile(Path("one/two/file2"), "file2", 2)
        root.AddFile(Path("one/two/three/file3"), "file3", 3)
        root.AddDir(Path("one/empty_dir"))

        assert [
            (node.fullpath, node.hash_value, node.file_size)
            for node in root.Enum()
        ] == [
            (
                Path("one"),
                Snapshot.DirHashPlaceholder(explicitly_added=False),
                None,
            ),
            (
                Path("one/file1"),
                "file1",
                1,
            ),
            (
                Path("one/two"),
                Snapshot.DirHashPlaceholder(explicitly_added=False),
                None,
            ),
            (
                Path("one/two/file2"),
                "file2",
                2,
            ),
            (
                Path("one/two/three"),
                Snapshot.DirHashPlaceholder(explicitly_added=False),
                None,
            ),
            (
                Path("one/two/three/file3"),
                "file3",
                3,
            ),
            (
                Path("one/empty_dir"),
                Snapshot.DirHashPlaceholder(explicitly_added=False),
                None,
            ),
        ]


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
def _MakeFile(
    root: Path,
    path: Path,
):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w") as f:
        f.write(PathEx.CreateRelativePath(root, path).as_posix())


# ----------------------------------------------------------------------
@pytest.fixture(scope="module")
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
