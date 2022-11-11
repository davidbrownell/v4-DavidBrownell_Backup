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
import sys

from pathlib import Path
from unittest import mock

import pytest

from Common_Foundation.ContextlibEx import ExitStack


# ----------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))
with ExitStack(lambda: sys.path.pop(0)):
    from Snapshot import Snapshot


# ----------------------------------------------------------------------
class TestCalculate(object):
    # ----------------------------------------------------------------------
    @classmethod
    @pytest.fixture(scope="function")
    def local_working_dir(cls, tmp_path_factory):
        root = tmp_path_factory.mktemp("root")

        _MakeFile(root, root / "StableFile1")
        _MakeFile(root, root / "DisappearingFile")
        _MakeFile(root, root / "StableFile2")

        return root

    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("is_ssd", [False, True])
    def test_Single(self, _working_dir, is_ssd):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "one",
            ],
            is_ssd=is_ssd,
        )

        assert result.hash_values == {
            _working_dir / "one": {
                "A": "38818bc4ba444583f537b9ed36a2fb4e7fd49694efd4a06b8fe0c1b00161e904f4edb7a9713543b74f283261d3000671b6c0567d6abea2b19686870d8b344b4e",
                "BC": "7abfea86ce9ef5721ccea68e560d879bfd76ec3c4fa26c91ecbde49754ddebfa8ac92e45fa9ecae5f373b3baa761c4921feceb1eb62cd9b9dedcf9178f089958",
            },
        }

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("is_ssd", [False, True])
    def test_Multiple(self, _working_dir, is_ssd):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "two",
                _working_dir / "one",
            ],
            is_ssd=is_ssd,
        )

        assert result.hash_values == {
            _working_dir / "one": {
                "A": "38818bc4ba444583f537b9ed36a2fb4e7fd49694efd4a06b8fe0c1b00161e904f4edb7a9713543b74f283261d3000671b6c0567d6abea2b19686870d8b344b4e",
                "BC": "7abfea86ce9ef5721ccea68e560d879bfd76ec3c4fa26c91ecbde49754ddebfa8ac92e45fa9ecae5f373b3baa761c4921feceb1eb62cd9b9dedcf9178f089958",
            },
            _working_dir / "two": {
                "File1": "15703c6965d528c208f42bd8ca9d63a7ea4409652c5587734a233d875e7d1ee3f99510a238abfb83b45c51213535c98dede44222dc8596046da0b63078a55675",
                "Dir1/File2": "83fc5d340bf8f141d9936d7ec79b87d0fbe47e54fd9b476e3ce9540bf8577da7a43e5b38b8783efadf7c374ff4f0b9fc232cc046a4d1640cb604d624e3444f6e",
                "Dir1/File3": "d919d6b3367c051892e6de02c85f7315067b813fa6aef32c9780a23ac0bb9fa63c0db2d1b5f5337114bcd9d7aea809cf1b21034bbb0a33d25eb7f53b55b9be9d",
                "Dir2/Dir3/File4": "66eb5ff590efe01bfa476a9e5eada7d7f0c305a6e0f6c708ec460bdf37edb06385854185ae6df5fde9c22c5a2e3abb8306c96160868f2442e375401c88e2cbaf",
            },
        }

    # ----------------------------------------------------------------------
    def test_LongPaths(self, _working_dir):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "VeryLongPaths",
            ],
            is_ssd=False,
        )

        assert result.hash_values == {
            _working_dir / "VeryLongPaths": {
                "1" * 200: "b4b83b4558520fe120e24513a783e3a9c98d75d381f2b93d468538266918c2c6a0735a775b26f8c48f1317cf08a7c13c069de7307bc2700811cc093cdc10ccb3",
                "2" * 201: "70130b445ff3a02cac2e4c864743105e8d975253f758afee619993b77c913dd0faeae37f98bfb4a36da5e455a04f7fc3879fe3db091edd8baf9e0627ef75803e",
            },
        }

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("is_ssd", [False, True])
    def test_WithFilterFunc(self, _working_dir, is_ssd):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "two",
                _working_dir / "one",
            ],
            is_ssd=is_ssd,
            filter_func=lambda value: value != (_working_dir / "two" / "Dir1" / "File2"),
        )

        assert result.hash_values == {
            _working_dir / "one": {
                "A": "38818bc4ba444583f537b9ed36a2fb4e7fd49694efd4a06b8fe0c1b00161e904f4edb7a9713543b74f283261d3000671b6c0567d6abea2b19686870d8b344b4e",
                "BC": "7abfea86ce9ef5721ccea68e560d879bfd76ec3c4fa26c91ecbde49754ddebfa8ac92e45fa9ecae5f373b3baa761c4921feceb1eb62cd9b9dedcf9178f089958",
            },
            _working_dir / "two": {
                "File1": "15703c6965d528c208f42bd8ca9d63a7ea4409652c5587734a233d875e7d1ee3f99510a238abfb83b45c51213535c98dede44222dc8596046da0b63078a55675",
                "Dir1/File3": "d919d6b3367c051892e6de02c85f7315067b813fa6aef32c9780a23ac0bb9fa63c0db2d1b5f5337114bcd9d7aea809cf1b21034bbb0a33d25eb7f53b55b9be9d",
                "Dir2/Dir3/File4": "66eb5ff590efe01bfa476a9e5eada7d7f0c305a6e0f6c708ec460bdf37edb06385854185ae6df5fde9c22c5a2e3abb8306c96160868f2442e375401c88e2cbaf",
            }
        }

    # ----------------------------------------------------------------------
    @pytest.mark.parametrize("is_ssd", [False, True])
    def test_WithFilterFuncNoMatches(self, _working_dir, is_ssd):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "two",
                _working_dir / "one",
            ],
            is_ssd=is_ssd,
            filter_func=lambda value: False,
        )

        assert result.hash_values == {
            _working_dir / "one": {},
            _working_dir / "two": {},
        }

    # ----------------------------------------------------------------------
    def test_ErrorDuringCalculation(self, _working_dir):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = -1

        with pytest.raises(Exception, match="Errors encountered when calculating files."):
            Snapshot.Calculate(
                dm_mock,
                [_working_dir / "one"],
                is_ssd=False,
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

        with pytest.raises(Exception, match="Errors encountered when hashing files."):
            Snapshot.Calculate(
                dm_mock,
                [_working_dir / "one"],
                is_ssd=False,
            )

    # ----------------------------------------------------------------------
    def test_FileDisappears(self, local_working_dir):
        dm_mock = mock.MagicMock()

        is_first_call = True

        # ----------------------------------------------------------------------
        def Func():
            nonlocal is_first_call

            if is_first_call:
                is_first_call = False
            else:
                disappearing_file = local_working_dir / "DisappearingFile"

                if disappearing_file.is_file():
                    disappearing_file.unlink()

            return mock.MagicMock(result=0)

        # ----------------------------------------------------------------------

        dm_mock.Nested().__enter__.configure_mock(side_effect=Func)

        result = Snapshot.Calculate(
            dm_mock,
            [
                local_working_dir,
            ],
            is_ssd=False,
        )

        assert result.hash_values == {
            local_working_dir: {
                "StableFile1": "8107a23a413c1095854083ddf343a80d99d385484ffd9c166ca1979e0acfdfef9b0eb47d6211b558caf856711623fadc6413fbea7ca26e3f7c2641a3530d6c14",
                "StableFile2": "49f9c1979e0068d14ddaae22400c0301f522963ee44ca3a98f2926809a8f5d9eee29c88e875ce8c5414698456832637df2e643cfeff744eb3543eb763fed651f",
            },
        }


# ----------------------------------------------------------------------
class TestPersistAndLoad(object):
    # ----------------------------------------------------------------------
    def test(self, _working_dir, tmp_path):
        dm_mock = mock.MagicMock()

        dm_mock.Nested().__enter__().result = 0

        result = Snapshot.Calculate(
            dm_mock,
            [
                _working_dir / "two",
                _working_dir / "one",
            ],
            is_ssd=False,
        )

        assert result.hash_values

        result.Persist(dm_mock, tmp_path)

        loaded_result = Snapshot.LoadPersisted(dm_mock, tmp_path)

        assert loaded_result is not result
        assert loaded_result == result


# ----------------------------------------------------------------------
class TestDiff(object):
    # ----------------------------------------------------------------------
    @staticmethod
    @pytest.fixture
    def snapshot():
        return Snapshot(
            {
                Path("one"): {
                    "100": "hash(100)",
                    "101": "hash(101)",
                    "102": "hash(102)",
                },
                Path("two"): {
                    "200": "hash(200)",
                    "201": "hash(201)",
                    "202": "hash(202)",
                },
                Path("three"): {},
            },
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

        del other.hash_values[Path("two")]
        assert other != snapshot

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.added,
                Path("two"),
                "200",
                "hash(200)",
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.added,
                Path("two"),
                "201",
                "hash(201)",
                None,
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.added,
                Path("two"),
                "202",
                "hash(202)",
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.removed,
                Path("two"),
                "200",
                None,
                "hash(200)",
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.removed,
                Path("two"),
                "201",
                None,
                "hash(201)",
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.removed,
                Path("two"),
                "202",
                None,
                "hash(202)",
            ),
        ]

    # ----------------------------------------------------------------------
    def test_EmptyRoot(self, snapshot, other):
        assert snapshot == other

        del other.hash_values[Path("three")]
        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.added,
                Path("three"),
                None,
                None,
                None,
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.removed,
                Path("three"),
                None,
                None,
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_AddedItem(self, snapshot, other):
        assert snapshot == other

        other.hash_values[Path("three")]["300"] = "hash(300)"

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.removed,
                Path("three"),
                "300",
                None,
                "hash(300)",
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.added,
                Path("three"),
                "300",
                "hash(300)",
                None,
            ),
        ]

    # ----------------------------------------------------------------------
    def test_Modified(self, snapshot, other):
        assert snapshot == other

        other.hash_values[Path("one")]["100"] = "Modified(100)"
        other.hash_values[Path("one")]["102"] = "Modified(102)"
        other.hash_values[Path("two")]["201"] = "Modified(201)"

        assert snapshot != other

        assert list(snapshot.Diff(other)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modified,
                Path("one"),
                "100",
                "hash(100)",
                "Modified(100)",
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modified,
                Path("one"),
                "102",
                "hash(102)",
                "Modified(102)",
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modified,
                Path("two"),
                "201",
                "hash(201)",
                "Modified(201)",
            ),
        ]

        assert list(other.Diff(snapshot)) == [
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modified,
                Path("one"),
                "100",
                "Modified(100)",
                "hash(100)",
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modified,
                Path("one"),
                "102",
                "Modified(102)",
                "hash(102)",
            ),
            Snapshot.DiffResult(
                Snapshot.DiffOperation.modified,
                Path("two"),
                "201",
                "Modified(201)",
                "hash(201)",
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
        f.write(Path(*path.parts[len(root.parts):]).as_posix())


# ----------------------------------------------------------------------
@pytest.fixture(scope="module")
def _working_dir(tmp_path_factory):
    root = tmp_path_factory.mktemp("root")

    _MakeFile(root, root / "one" / "A")
    _MakeFile(root, root / "one" / "BC")

    _MakeFile(root, root / "two" / "File1")
    _MakeFile(root, root / "two" / "Dir1" / "File2")
    _MakeFile(root, root / "two" / "Dir1" / "File3")

    _MakeFile(root, root / "two" / "Dir2" / "Dir3" / "File4")

    _MakeFile(root, root / "VeryLongPaths" / ("1" * 200))
    _MakeFile(root, root / "VeryLongPaths" / ("2" * 201))

    return root
