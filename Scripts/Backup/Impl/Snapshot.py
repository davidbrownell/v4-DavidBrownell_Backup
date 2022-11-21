# ----------------------------------------------------------------------
# |
# |  Snapshot.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-10-20 08:31:28
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Contains the Snapshot object"""

import functools
import hashlib
import itertools
import json
import math
import os

from dataclasses import dataclass, field
from enum import auto, Enum
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Union

from Common_Foundation import PathEx
from Common_Foundation.Streams.DoneManager import DoneManager

from Common_FoundationEx import ExecuteTasks
from Common_FoundationEx.InflectEx import inflect


# ----------------------------------------------------------------------
@dataclass(frozen=True)
class Snapshot(object):
    """Collection of files and hashes"""

    # ----------------------------------------------------------------------
    # |
    # |  Public Types
    # |
    # ----------------------------------------------------------------------
    @dataclass(frozen=True)
    class DirHashPlaceholder(object):
        """Object that signals that absence of a hash value because the associated item is a directory"""

        # ----------------------------------------------------------------------
        explicitly_added: bool              = field(kw_only=True)

        # ----------------------------------------------------------------------
        def __eq__(self, other) -> bool:
            return isinstance(other, self.__class__)

        # ----------------------------------------------------------------------
        def __ne__(self, other) -> bool:
            return not self == other

        # ----------------------------------------------------------------------
        def ToJson(self) -> Dict[str, Any]:
            return {
                "explicitly_added": self.explicitly_added,
            }

        # ----------------------------------------------------------------------
        @classmethod
        def FromJson(
            cls,
            value: Dict[str, Any],
        ) -> "Snapshot.DirHashPlaceholder":
            return cls(explicitly_added=value["explicitly_added"])

    # ----------------------------------------------------------------------
    class DiffOperation(Enum):
        """Defines the cause of the diff"""

        add                                 = auto()
        remove                              = auto()
        modify                              = auto()

    # ----------------------------------------------------------------------
    @dataclass(frozen=True)
    class DiffResult(object):
        """Represents a difference between a file at a source and destination"""

        # ----------------------------------------------------------------------
        operation: "Snapshot.DiffOperation"
        path: Path

        # Used when operation is [add, update]
        this_hash: Union[None, str, "Snapshot.DirHashPlaceholder"]
        this_file_size: Optional[int]

        # Used when operation is [remove, update]
        other_hash: Union[None, str, "Snapshot.DirHashPlaceholder"]
        other_file_size: Optional[int]

        # ----------------------------------------------------------------------
        def __post_init__(self):
            assert (
                (self.operation == Snapshot.DiffOperation.add and self.this_hash is not None and self.other_hash is None)
                or (self.operation == Snapshot.DiffOperation.modify and self.this_hash is not None and self.other_hash is not None)
                or (self.operation == Snapshot.DiffOperation.remove and self.this_hash is None and self.other_hash is not None)
            ), "Instance is in an inconsistent state"

            assert (
                (self.this_hash is None and self.this_file_size is None)
                or (
                    self.this_hash is not None
                    and (
                        (isinstance(self.this_hash, Snapshot.DirHashPlaceholder) and self.this_file_size is None)
                        or (isinstance(self.this_hash, str) and self.this_file_size is not None)
                    )
                )
            ), "'this' values are in an inconsistent state"

            assert (
                (self.other_hash is None and self.other_file_size is None)
                or (
                    self.other_hash is not None
                    and (
                        (isinstance(self.other_hash, Snapshot.DirHashPlaceholder) and self.other_file_size is None)
                        or (isinstance(self.other_hash, str) and self.other_file_size is not None)
                    )
                )
            ), "'other' values are in an inconsistent state"

    # ----------------------------------------------------------------------
    @dataclass
    class Node(object):
        """Corresponds to a file or directory"""

        # ----------------------------------------------------------------------
        name: Optional[str]
        parent: Optional["Snapshot.Node"]               = field(compare=False)

        hash_value: Union[str, "Snapshot.DirHashPlaceholder"]
        file_size: Optional[int]

        children: Dict[str, "Snapshot.Node"]            = field(init=False, default_factory=dict)

        # ----------------------------------------------------------------------
        @property
        def is_dir(self) -> bool:
            return isinstance(self.hash_value, Snapshot.DirHashPlaceholder)

        @property
        def is_file(self) -> bool:
            return isinstance(self.hash_value, str)

        @functools.cached_property
        def fullpath(self) -> Path:
            names: List[str] = []

            node = self
            while True:
                assert node is not None

                if node.name is None:
                    break

                names.append(node.name)
                node = node.parent

            return Path(*reversed(names))

        # ----------------------------------------------------------------------
        def __post_init__(self):
            assert (
                (self.name is None and self.parent is None)
                or (self.name is not None and self.parent is not None)
            )

            assert (
                (isinstance(self.hash_value, Snapshot.DirHashPlaceholder) and self.file_size is None)
                or (isinstance(self.hash_value, str) and  self.file_size is not None and self.file_size >= 0)
            ), (self.hash_value, self.file_size)

            assert not self.children or self.is_dir

        # ----------------------------------------------------------------------
        @classmethod
        def Create(
            cls,
            values: Dict[Path, Optional[Tuple[str, int]]],
        ) -> "Snapshot.Node":
            root = cls(None, None, Snapshot.DirHashPlaceholder(explicitly_added=False), None)

            for path, path_values in values.items():
                if path_values is None:
                    root.AddDir(path)
                else:
                    hash_value, file_size = path_values
                    root.AddFile(path, hash_value, file_size)

            return root

        # ----------------------------------------------------------------------
        def AddFile(
            self,
            path: Path,
            hash_value: str,
            file_size: int,
            *,
            force: bool=False,
        ) -> "Snapshot.Node":
            return self._AddImpl(path, hash_value, file_size, force=force)

        # ----------------------------------------------------------------------
        def AddDir(
            self,
            path: Path,
            *,
            force: bool=False,
        ) -> "Snapshot.Node":
            return self._AddImpl(path, Snapshot.DirHashPlaceholder(explicitly_added=True), None, force=force)

        # ----------------------------------------------------------------------
        def ToJson(self) -> Dict[str, Any]:
            result: Dict[str, Any] = {}

            if isinstance(self.hash_value, str):
                assert self.file_size is not None
                assert not self.children

                result["hash_value"] = self.hash_value
                result["file_size"] = self.file_size

            else:
                assert self.file_size is None

                result["hash_value"] = self.hash_value.ToJson()
                result["children"] = {
                    k: v.ToJson()
                    for k, v in self.children.items()
                }

            return result

        # ----------------------------------------------------------------------
        @classmethod
        def FromJson(
            cls,
            name: Optional[str],
            parent: Optional["Snapshot.Node"],
            value: Dict[str, Any],
        ) -> "Snapshot.Node":
            hash_value = value["hash_value"]

            if isinstance(hash_value, str):
                file_size = value["file_size"]
            else:
                hash_value = Snapshot.DirHashPlaceholder.FromJson(hash_value)
                file_size = None

            result = cls(name, parent, hash_value, file_size)

            if result.is_dir:
                result.children = {
                    k: cls.FromJson(k, result, v)
                    for k, v in value["children"].items()
                }

            return result

        # ----------------------------------------------------------------------
        def CreateDiffs(
            self,
            other: Optional["Snapshot.Node"],
            file_compare_func: Callable[["Snapshot.Node", "Snapshot.Node"], bool],
        ) -> Tuple[
            List["Snapshot.DiffResult"],
            Optional["Snapshot.DiffOperation"],
        ]:
            diffs: List[Snapshot.DiffResult] = []

            if other is None:
                if self.is_dir and self.children:
                    for value in self.children.values():
                        diffs += value.CreateDiffs(None, file_compare_func)[0]

                else:
                    diffs.append(
                        Snapshot.DiffResult(
                            Snapshot.DiffOperation.add,
                            self.fullpath,
                            self.hash_value,
                            self.file_size,
                            None,
                            None,
                        ),
                    )

                return diffs, Snapshot.DiffOperation.add

            if self.is_file or other.is_file:
                if self.is_file and other.is_file:
                    if file_compare_func(self, other):
                        return [], None

                    diffs.append(
                        Snapshot.DiffResult(
                            Snapshot.DiffOperation.modify,
                            self.fullpath,
                            self.hash_value,
                            self.file_size,
                            other.hash_value,
                            other.file_size,
                        ),
                    )
                else:
                    # The type has changed
                    diffs.append(
                        Snapshot.DiffResult(
                            Snapshot.DiffOperation.remove,
                            other.fullpath,
                            None,
                            None,
                            other.hash_value,
                            other.file_size,
                        ),
                    )

                    diffs += self.CreateDiffs(None, file_compare_func)[0]

                assert diffs
                return diffs, Snapshot.DiffOperation.modify

            # We are looking at directories
            atomic_result: Optional[Snapshot.DiffOperation] = None

            # ----------------------------------------------------------------------
            def UpdateAtomicResult(
                result: Optional[Snapshot.DiffOperation],
            ) -> None:
                nonlocal atomic_result

                if atomic_result is None:
                    atomic_result = result
                elif result != atomic_result:
                    atomic_result = Snapshot.DiffOperation.modify

            # ----------------------------------------------------------------------

            for child_name, other_child in other.children.items():
                if child_name in self.children:
                    continue

                diffs.append(
                    Snapshot.DiffResult(
                        Snapshot.DiffOperation.remove,
                        other.fullpath / child_name,
                        None,
                        None,
                        other_child.hash_value,
                        other_child.file_size,
                    ),
                )

                UpdateAtomicResult(Snapshot.DiffOperation.remove)

            for child_name, this_child in self.children.items():
                child_diffs, child_result = this_child.CreateDiffs(
                    other.children.get(child_name, None),
                    file_compare_func,
                )

                diffs += child_diffs
                UpdateAtomicResult(child_result)

            # If all of the results are consistent, we can replace the diffs with a diff at
            # this level (unless this item has been explicitly added, in which case we should
            # keep it around).
            if atomic_result == Snapshot.DiffOperation.remove:
                assert isinstance(self.hash_value, Snapshot.DirHashPlaceholder)
                assert isinstance(other.hash_value, Snapshot.DirHashPlaceholder)

                if self.hash_value.explicitly_added or other.hash_value.explicitly_added:
                    # We don't want to remove the dir because it has been explicitly added.
                    # Keep the existing diffs and show that the node has been modified.
                    atomic_result = Snapshot.DiffOperation.modify
                else:
                    # Replace the existing diffs with a single diff to remove this dir.
                    diffs = [
                        Snapshot.DiffResult(
                            Snapshot.DiffOperation.remove,
                            other.fullpath,
                            None,
                            None,
                            other.hash_value,
                            other.file_size,
                        ),
                    ]

            assert (
                (atomic_result is None and not diffs)
                or (atomic_result is not None and diffs)
            ), (atomic_result, diffs)

            return diffs, atomic_result

        # ----------------------------------------------------------------------
        # ----------------------------------------------------------------------
        # ----------------------------------------------------------------------
        def _AddImpl(
            self,
            path: Path,
            hash_value: Union[str, "Snapshot.DirHashPlaceholder"],
            file_size: Optional[int],
            *,
            force: bool,
        ) -> "Snapshot.Node":
            node = self

            for part in path.parent.parts:
                new_node = node.children.get(part, None)

                if new_node is None:
                    new_node = self.__class__(part, node, Snapshot.DirHashPlaceholder(explicitly_added=False), None)
                    node.children[part] = new_node

                node = new_node

            assert force or path.name not in node.children, path
            node.children[path.name] = self.__class__(path.name, node, hash_value, file_size)

            return self

    # ----------------------------------------------------------------------
    PERSISTED_FILE_NAME                     = "BackupSnapshot.json"

    # ----------------------------------------------------------------------
    # |
    # |  Public Data
    # |
    # ----------------------------------------------------------------------
    node: Node

    # ----------------------------------------------------------------------
    # |
    # |  Public Methods
    # |
    # ----------------------------------------------------------------------
    @staticmethod
    def GetTaskDisplayName(
        filename: Path,
    ) -> str:
        max_filename_length = 110

        filename_str = str(filename)

        if len(filename_str) <= max_filename_length:
            filename_str = filename_str.ljust(max_filename_length)
        else:
            midpoint = int(math.floor(len(filename_str) / 2))
            chars_to_trim = (len(filename_str) - max_filename_length + 3) / 2

            filename_str = "{}...{}".format(
                filename_str[:(midpoint - math.floor(chars_to_trim))],
                filename_str[(midpoint + math.ceil(chars_to_trim)):],
            )

        assert len(filename_str) == max_filename_length, (len(filename_str), max_filename_length)

        return filename_str

    # ----------------------------------------------------------------------
    @classmethod
    def Calculate(
        cls,
        dm: DoneManager,
        inputs: List[Path],
        *,
        is_ssd: bool,
        quiet: bool=False,
        filter_filename_func: Optional[
            Callable[
                [Path],
                bool,                       # True to include, False to skip
            ]
        ]=None,
        calculate_hashes: bool=True,
    ) -> "Snapshot":
        # Validate that the roots do not overlap
        assert inputs

        for input_item in inputs:
            if not input_item.exists():
                raise Exception("'{}' is not a valid file or directory.".format(input_item))

        sorted_inputs = list(inputs)
        sorted_inputs.sort(key=lambda value: len(value.parts))

        for input_index in range(1, len(sorted_inputs)):
            input_item = sorted_inputs[input_index]

            for query_index in range(0, input_index):
                query_input = sorted_inputs[query_index]

                if PathEx.IsDescendant(input_item, query_input):
                    raise Exception("The input '{}' overlaps with '{}'.".format(input_item, query_input))

        # Continue with the calculation
        filter_filename_func = filter_filename_func or (lambda value: True)

        # ----------------------------------------------------------------------
        @dataclass(frozen=True)
        class InputInfo(object):
            # pylint: disable=missing-class-docstring

            # ----------------------------------------------------------------------
            filenames: List[Path]
            empty_dirs: List[Path]

            # ----------------------------------------------------------------------
            def __bool__(self) -> bool:
                return bool(self.filenames) or bool(self.empty_dirs)

        # ----------------------------------------------------------------------

        # Calculate information about the inputs
        all_input_infos: Dict[Path, InputInfo] = {}

        # ----------------------------------------------------------------------
        # Do not include this method in unit test code coverage, as dm is a mock which means that
        # Nested isn't called, meaning there won't be anything to invoke this functionality on __exit__.
        # Automated testing for this functionality will happen during IntegrationTests.
        def CalculatingFilesExitStatus(): # pragma: no cover
            # We need to do some extra work, because inflect doesn't work we we need a word between the number of items
            # and plural for where the singular form of the plural word ends in 'y'.
            num_empty_dirs = sum(len(input_info.empty_dirs) for input_info in all_input_infos.values())

            return "{} found, {} empty {} found".format(
                inflect.no("file", sum(len(input_info.filenames) for input_info in all_input_infos.values())),
                num_empty_dirs,
                inflect.plural("directory", num_empty_dirs),
            )

        # ----------------------------------------------------------------------

        with dm.Nested("Calculating files...", CalculatingFilesExitStatus) as calculating_dm:
            # ----------------------------------------------------------------------
            def CalculatingFilesStep1(
                context: Path,
                on_simple_status_func: Callable[[str], None],  # pylint: disable=unused-argument
            ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[InputInfo]]:
                input_item = context

                # ----------------------------------------------------------------------
                def Step2(
                    status: ExecuteTasks.Status,
                ) -> Tuple[InputInfo, Optional[str]]:
                    filenames: List[Path] = []
                    empty_dirs: List[Path] = []

                    if input_item.is_file():
                        filenames.append(input_item)
                    else:
                        for root, directories, these_filenames in os.walk(input_item):
                            root = Path(root)

                            if not directories and not these_filenames:
                                empty_dirs.append(root)
                                continue

                            for this_filename in these_filenames:
                                fullpath = root / this_filename

                                if not filter_filename_func(fullpath):
                                    status.OnInfo(
                                        "The file '{}' has been excluded by the filter func.".format(fullpath),
                                        verbose=True,
                                    )

                                    continue

                                filenames.append(fullpath)

                    value = "{} found".format(inflect.no("file", len(filenames)))

                    status.OnInfo(
                        "{} in '{}'.".format(value, input_item),
                        verbose=True,
                    )

                    return InputInfo(filenames, empty_dirs), value

                # ----------------------------------------------------------------------

                return None, Step2

            # ----------------------------------------------------------------------

            all_all_input_infos: List[Optional[InputInfo]] = ExecuteTasks.Transform(
                calculating_dm,
                "Processing",
                [
                    ExecuteTasks.TaskData(str(input_item), input_item)
                    for input_item in inputs
                ],
                CalculatingFilesStep1,
                quiet=quiet,
                max_num_threads=None if is_ssd else 1,
            )

            if calculating_dm.result != 0:
                raise Exception("Errors encountered when calculating files.")

            for root, input_info in zip(inputs, all_all_input_infos):
                assert input_info is not None
                all_input_infos[root] = input_info

        if not any(input_info for input_info in all_input_infos.values()):
            return cls(Snapshot.Node(None, None, Snapshot.DirHashPlaceholder(explicitly_added=False), None))

        with dm.Nested("\nCalculating hashes...") as hashes_dm:
            # ----------------------------------------------------------------------
            def CalculatingHashesStep2Func(
                context: Path,
                on_simple_status_func: Callable[[str], None],  # pylint: disable=unused-argument
            ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[Optional[Tuple[str, int]]]]:
                filename = context

                # ----------------------------------------------------------------------
                def Step2(
                    status: ExecuteTasks.Status,
                ) -> Tuple[Optional[Tuple[str, int]], Optional[str]]:
                    if not filename.is_file():
                        status.OnInfo("'{}' no longer exists.".format(filename))
                        return None, None

                    if not calculate_hashes:
                        hash_value = "ignored"
                    else:
                        hasher = hashlib.sha512()
                        bytes_hashed = 0

                        with filename.open("rb") as f:
                            while True:
                                chunk = f.read(16384)
                                if not chunk:
                                    break

                                hasher.update(chunk)

                                bytes_hashed += len(chunk)
                                status.OnProgress(bytes_hashed, "")

                        hash_value = hasher.hexdigest()

                    return (hash_value, filename.stat().st_size), None

                # ----------------------------------------------------------------------

                if filename.is_file():
                    num_steps = filename.stat().st_size
                else:
                    num_steps = None

                return num_steps, Step2

            # ----------------------------------------------------------------------

            file_infos: List[Optional[Tuple[str, int]]] = []

            tasks: List[ExecuteTasks.TaskData] = [
                ExecuteTasks.TaskData(cls.GetTaskDisplayName(filename), filename)
                for filename in itertools.chain(*(input_info.filenames for input_info in all_input_infos.values()))
            ]

            if tasks:
                file_infos += ExecuteTasks.Transform(
                    hashes_dm,
                    "Processing",
                    tasks,
                    CalculatingHashesStep2Func,
                    quiet=quiet,
                    max_num_threads=None if is_ssd else 1,
                    refresh_per_second=4,
                )

                if hashes_dm.result != 0:
                    raise Exception("Errors encountered when hashing files.")

        with dm.Nested("Organizing results..."):
            root = Snapshot.Node(None, None, Snapshot.DirHashPlaceholder(explicitly_added=False), None)

            filename_offset = 0

            for input_info in all_input_infos.values():
                for filename in input_info.filenames:
                    file_info = file_infos[filename_offset]
                    filename_offset += 1

                    if file_info is None:
                        continue

                    hash_value, file_size = file_info

                    root.AddFile(filename, hash_value, file_size)

                for directory in input_info.empty_dirs:
                    root.AddDir(directory)

        return cls(root)

    # ----------------------------------------------------------------------
    @classmethod
    def IsPersisted(
        cls,
        directory: Path,
    ) -> bool:
        return (directory / cls.PERSISTED_FILE_NAME).is_file()

    # ----------------------------------------------------------------------
    @classmethod
    def LoadPersisted(
        cls,
        dm: DoneManager,
        directory: Path,
    ) -> "Snapshot":
        snapshot_filename = directory / cls.PERSISTED_FILE_NAME

        with dm.Nested("Reading '{}'...".format(snapshot_filename)):
            with snapshot_filename.open() as f:
                content = json.load(f)

            return Snapshot(Snapshot.Node.FromJson(None, None, content))

    # ----------------------------------------------------------------------
    def Persist(
        self,
        dm: DoneManager,
        output_dir: Path,
    ) -> None:
        snapshot_filename = output_dir / self.__class__.PERSISTED_FILE_NAME

        with dm.Nested("Writing '{}'...".format(snapshot_filename)):
            with snapshot_filename.open("w") as f:
                json.dump(self.node.ToJson(), f)

    # ----------------------------------------------------------------------
    def Diff(
        self,
        other: "Snapshot",
        *,
        compare_hashes: bool=True,
    ) -> Generator["Snapshot.DiffResult", None, None]:
        """Enumerates the differences between two `Snapshot`s"""

        if compare_hashes:
            compare_func = lambda a, b: a.hash_value == b.hash_value
        else:
            compare_func = lambda a, b: a.file_size == b.file_size

        yield from self.node.CreateDiffs(other.node, compare_func)[0]
