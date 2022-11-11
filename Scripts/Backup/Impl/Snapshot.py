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

import hashlib
import itertools
import json
import math

from dataclasses import dataclass
from enum import auto, Enum
from pathlib import Path
from typing import cast, Callable, Dict, Generator, List, Optional, Set, Tuple

from Common_Foundation.EnumSource import EnumSource
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
    class DiffOperation(Enum):
        """Defines the cause of the diff"""

        added                               = auto()
        removed                             = auto()
        modified                            = auto()

    # ----------------------------------------------------------------------
    PERSISTED_FILE_NAME                     = "BackupSnapshot.json"

    # ----------------------------------------------------------------------
    # |
    # |  Public Data
    # |
    # ----------------------------------------------------------------------
    hash_values: Dict[
        Path,                               # Root dir
        Dict[
            str,                            # Relative path to file
            str,                            # Hash value
        ],
    ]

    # ----------------------------------------------------------------------
    # |
    # |  Public Methods
    # |
    # ----------------------------------------------------------------------
    @classmethod
    def Calculate(
        cls,
        dm: DoneManager,
        roots: List[Path],
        *,
        is_ssd: bool,
        quiet: bool=False,
        filter_func: Optional[
            Callable[
                [Path],
                bool,                       # True to include, False to skip
            ]
        ]=None,
    ) -> "Snapshot":
        filter_func = filter_func or (lambda value: True)

        # Calculate the filenames
        file_results: Dict[Path, List[Path]] = {}

        with dm.Nested(
            "Calculating files...",
            lambda: "{} found".format(inflect.no("file", sum(len(values) for values in file_results.values()))),
        ) as calculating_dm:
            # ----------------------------------------------------------------------
            def CalculatingFilesStep1(
                context: Path,
                on_simple_status_func: Callable[[str], None],  # pylint: disable=unused-argument
            ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[List[Path]]]:
                root = context

                # ----------------------------------------------------------------------
                def Step2(
                    status: ExecuteTasks.Status,
                ) -> Tuple[List[Path], Optional[str]]:
                    filenames: List[Path] = []

                    for this_root, _, these_filenames in EnumSource(root):
                        for this_filename in these_filenames:
                            fullpath = this_root / this_filename

                            if not filter_func(fullpath):
                                status.OnInfo(
                                    "The file '{}' has been excluded by the filter func.".format(fullpath),
                                    verbose=True,
                                )

                                continue

                            filenames.append(fullpath)

                    value = "{} found".format(inflect.no("file", len(filenames)))

                    status.OnInfo(
                        "{} in '{}'.".format(value, root),
                        verbose=True,
                    )

                    return filenames, value

                # ----------------------------------------------------------------------

                return None, Step2

            # ----------------------------------------------------------------------

            all_filenames: List[Optional[List[Path]]] = ExecuteTasks.Transform(
                calculating_dm,
                "Processing",
                [
                    ExecuteTasks.TaskData(str(root), root)
                    for root in roots
                ],
                CalculatingFilesStep1,
                quiet=quiet,
                max_num_threads=None if is_ssd else 1,
            )

            if calculating_dm.result != 0:
                raise Exception("Errors encountered when calculating files.")

            for root, filenames in zip(roots, all_filenames):
                file_results[root] = cast(List[Path], filenames)

        if not any(filenames for filenames in file_results.values()):
            return cls(
                {
                    root: {}
                    for root in roots
                },
            )

        with dm.Nested("Calculating hashes...") as hashes_dm:
            # ----------------------------------------------------------------------
            def CalculatingHashesStep2Func(
                context: Path,
                on_simple_status_func: Callable[[str], None],  # pylint: disable=unused-argument
            ) -> Tuple[Optional[int], ExecuteTasks.TransformStep2FuncType[Optional[str]]]:
                filename = context

                # ----------------------------------------------------------------------
                def Step2(
                    status: ExecuteTasks.Status,
                ) -> Tuple[Optional[str], Optional[str]]:
                    if not filename.is_file():
                        status.OnInfo("'{}' no longer exists.".format(filename))
                        return None, None

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

                    return hasher.hexdigest(), None

                # ----------------------------------------------------------------------

                if filename.is_file():
                    num_steps = filename.stat().st_size
                else:
                    num_steps = None

                return num_steps, Step2

            # ----------------------------------------------------------------------
            def GetDisplayName(
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

            hash_values: List[Optional[str]] = ExecuteTasks.Transform(
                hashes_dm,
                "Processing",
                [
                    ExecuteTasks.TaskData(GetDisplayName(filename), filename)
                    for filename in itertools.chain(*file_results.values())
                ],
                CalculatingHashesStep2Func,
                quiet=quiet,
                max_num_threads=None if is_ssd else 1,
                refresh_per_second=4,
            )

            if hashes_dm.result != 0:
                raise Exception("Errors encountered when hashing files.")

        with dm.Nested("Organizing results..."):
            results: Dict[Path, Dict[str, str]] = {}

            filename_offset = 0

            for root, filenames in file_results.items():
                these_results: Dict[str, str] = {}

                for filename in filenames:
                    hash_value = hash_values[filename_offset]
                    filename_offset += 1

                    if hash_value is None:
                        continue

                    assert filename.parts[:len(root.parts)] == root.parts

                    relative_path = Path(*filename.parts[len(root.parts):]).as_posix()

                    these_results[relative_path] = hash_value

                results[root] = these_results

        return cls(results)

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

            all_items: Dict[Path, Dict[str, str]] = {}

            for json_path, json_content in content.items():
                all_items[Path(json_path)] = json_content

            return Snapshot(all_items)

    # ----------------------------------------------------------------------
    def Persist(
        self,
        dm: DoneManager,
        output_dir: Path,
    ) -> None:
        snapshot_filename = output_dir / self.__class__.PERSISTED_FILE_NAME

        with dm.Nested("Writing '{}'...".format(snapshot_filename)):
            snapshot_filename.parent.mkdir(parents=True, exist_ok=True)

            with snapshot_filename.open("w") as f:
                json.dump(
                    {
                        root.as_posix(): items
                        for root, items in self.hash_values.items()
                    },
                    f,
                )

    # ----------------------------------------------------------------------
    @dataclass(frozen=True)
    class DiffResult(object):
        operation: "Snapshot.DiffOperation"
        root: Path
        relative_path: Optional[str]        # Can be None when the root is the only thing that has been modified
        this_hash: Optional[str]            # Used when operation is [added, updated]
        other_hash: Optional[str]           # Used when operation is [removed, updated]

    def Diff(
        self,
        other: "Snapshot",
    ) -> Generator["Snapshot.DiffResult", None, None]:
        """Enumerates the differences between two `Snapshot`s"""

        matching_items: List[Tuple[Path, Dict[str, str], Dict[str, str]]] = []
        other_paths: Set[Path] = set(other.hash_values.keys())

        for this_root, this_items in self.hash_values.items():
            other_items = other.hash_values.get(this_root, None)
            if other_items is not None:
                matching_items.append((this_root, this_items, other_items))
                other_paths.remove(this_root)

                continue

            # If here, we are looking at added items
            if not this_items:
                yield Snapshot.DiffResult(
                    Snapshot.DiffOperation.added,
                    this_root,
                    None,
                    None,
                    None,
                )
            else:
                yield from (
                    Snapshot.DiffResult(
                        Snapshot.DiffOperation.added,
                        this_root,
                        relative_path,
                        hash_value,
                        None,
                    )
                    for relative_path, hash_value in this_items.items()
                )

        # Process the matching items
        for root, this_items, other_items in matching_items:
            other_items_lookup: Set[str] = set(other_items.keys())

            for relative_path, this_hash in this_items.items():
                other_hash = other_items.get(relative_path, None)
                if other_hash is not None:
                    if this_hash != other_hash:
                        yield Snapshot.DiffResult(
                            Snapshot.DiffOperation.modified,
                            root,
                            relative_path,
                            this_hash,
                            other_hash,
                        )

                    other_items_lookup.remove(relative_path)

                    continue

                # If here, we are looking at an added item
                yield Snapshot.DiffResult(
                    Snapshot.DiffOperation.added,
                    root,
                    relative_path,
                    this_hash,
                    None,
                )

            # Process the items in the other collection but not available locally
            for other_relative_path in other_items_lookup:
                other_hash_value = other_items[other_relative_path]

                yield Snapshot.DiffResult(
                    Snapshot.DiffOperation.removed,
                    root,
                    other_relative_path,
                    None,
                    other_hash_value,
                )

        # Process the items that are not available locally
        for other_path in other_paths:
            other_items = other.hash_values[other_path]

            if not other_items:
                yield Snapshot.DiffResult(
                    Snapshot.DiffOperation.removed,
                    other_path,
                    None,
                    None,
                    None,
                )
            else:
                yield from (
                    Snapshot.DiffResult(
                        Snapshot.DiffOperation.removed,
                        other_path,
                        relative_path,
                        None,
                        hash_value,
                    )
                    for relative_path, hash_value in other_items.items()
                )
