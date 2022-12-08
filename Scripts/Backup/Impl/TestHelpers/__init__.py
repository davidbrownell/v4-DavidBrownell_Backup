# ----------------------------------------------------------------------
# |
# |  __init__.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-18 11:59:40
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Contains functionality that helps in the automated test process"""

# Note that this file is a module rather than a script so that we aren't forced to write unit tests
# for it (which we would be if it was a file in `../`).

import os
import re

from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import Dict, Generator, List, Match, Optional, Tuple, Union

from Common_Foundation import PathEx
from Common_Foundation.Shell.All import CurrentShell

from ..Mirror import CONTENT_DIR_NAME
from ..Snapshot import Snapshot


# ----------------------------------------------------------------------
# |
# |  Public Types
# |
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class FileInfo(object):
    """Information about a file"""

    path: Path
    file_size: Optional[int]                # None if the instance corresponds to an empty dir


# ----------------------------------------------------------------------
class OutputScrubber(object):
    """\
    Scrub output produced by Backup functionality that is problematic during testing.

    For example:
        - Directory names based on datetime
        - Size outputs based on the destination size

    """

    # ----------------------------------------------------------------------
    def __init__(self):
        self._directory_replacements: Dict[str, str]    = {}
        self._directory_name_regex          = re.compile(
            r"""(?#
            Capture [begin]                 )(?P<value>(?#
                Year                        )\d{4}\.(?#
                Month                       )\d{2}\.(?#
                Day                         )\d{2}\.(?#
                Hour                        )\d{2}\.(?#
                Minute                      )\d{2}\.(?#
                Second                      )\d{2}(?#
                Index                       )-\d+(?#
                Suffix                      )(?:\.delta)?(?#
            Capture [end]                   ))(?#
            )""",
        )

        self._space_regex = re.compile(
            r"""(?#
            Capture [begin]                 )(?P<value>(?#
                Value                       )\d+\s+(?#
                Units                       ).B\s+(?#
                available or required       )(?P<type>required|available)(?#
            Capture [end]                   ))(?#
            )""",
        )

    # ----------------------------------------------------------------------
    def Replace(
        self,
        value: str,
    ) -> str:
        # ----------------------------------------------------------------------
        def ReplaceDirectoryNames(
            match: Match,
        ) -> str:
            folder = match.group("value")

            replacement = self._directory_replacements.get(folder, None)
            if replacement is None:
                replacement = "<Folder{}>".format(len(self._directory_replacements))
                self._directory_replacements[folder] = replacement

            return replacement

        # ----------------------------------------------------------------------
        def ReplaceSpace(
            match: Match,
        ) -> str:
            return "<scrubbed space {}>".format(match.group("type"))

        # ----------------------------------------------------------------------

        value = self._directory_name_regex.sub(ReplaceDirectoryNames, value)
        value = self._space_regex.sub(ReplaceSpace, value)

        return value


# ----------------------------------------------------------------------
# |
# |  Public Functions
# |
# ----------------------------------------------------------------------
def GetOutputPath(
    destination_content_dir: Path,
    working_dir: Path,
) -> Path:
    if CurrentShell.family_name == "Windows":
        result = destination_content_dir / working_dir.parts[0].replace(":", "_") / Path(*working_dir.parts[1:])
    else:
        assert working_dir.parts[0] == '/', working_dir.parts
        result = destination_content_dir / Path(*working_dir.parts[1:])

    assert result.is_dir(), result
    return result


# ----------------------------------------------------------------------
def Enumerate(
    value: Path,
) -> Generator[FileInfo, None, None]:
    if value.is_file():
        yield FileInfo(value, value.stat().st_size)
        return

    for root, directories, filenames in os.walk(value):
        root = Path(root)

        if not directories and not filenames:
            yield FileInfo(root, None)
            continue

        for filename in filenames:
            fullpath = root / filename

            yield FileInfo(fullpath, fullpath.stat().st_size)


# ----------------------------------------------------------------------
def SetComparison(
    this_values: List[FileInfo],
    this_prefix: Path,
    that_values: List[FileInfo],
    that_prefix: Path,
) -> Generator[
    Tuple[
        Optional[FileInfo],                 # Will be None if the file is in that but not this
        Optional[FileInfo],                 # Will be None if the file is in this but not that
    ],
    None,
    None,
]:
    """Returns a tuple for all elements in both sets"""

    that_lookup: Dict[PurePath, FileInfo] = {
        PathEx.CreateRelativePath(that_prefix, that_value.path): that_value
        for that_value in that_values
    }

    for this_value in this_values:
        relative_path = PathEx.CreateRelativePath(this_prefix, this_value.path)

        yield this_value, that_lookup.pop(relative_path, None)

    for that_value in that_lookup.values():
        yield None, that_value


# ----------------------------------------------------------------------
def ValueComparison(
    this_values: List[FileInfo],
    this_prefix: Path,
    that_values: List[FileInfo],
    that_prefix: Path,
    *,
    compare_file_contents: bool,
) -> Generator[
    Tuple[
        Optional[FileInfo],
        Optional[FileInfo],
    ],
    None,
    None,
]:
    """Returns items that do not match"""

    for this_value, that_value in SetComparison(this_values, this_prefix, that_values, that_prefix):
        if this_value is None or that_value is None:
            yield this_value, that_value
            continue

        if this_value.file_size is None or that_value.file_size is None:
            if this_value.file_size != that_value.file_size:
                yield this_value, that_value

            continue

        # We are comparing files
        assert this_value.file_size is not None
        assert that_value.file_size is not None

        if this_value.file_size != that_value.file_size:
            yield this_value, that_value
            continue

        if compare_file_contents:
            with this_value.path.open("rb") as f:
                this_contents = f.read()

            with that_value.path.open("rb") as f:
                that_contents = f.read()

            if this_contents != that_contents:
                yield this_value, that_value


# ----------------------------------------------------------------------
def CompareFileSystemSourceAndDestination(
    source_or_sources: Union[Path, List[Path]],
    destination: Path,
    expected_num_items: Optional[int]=None,
    *,
    compare_file_contents: bool=False,
    is_mirror: bool=True,
) -> None:
    if isinstance(source_or_sources, list):
        sources = source_or_sources
    else:
        sources = [source_or_sources]

    common_source_path = PathEx.GetCommonPath(*sources)
    assert common_source_path is not None

    if is_mirror:
        snapshot_filename = destination / Snapshot.PERSISTED_FILE_NAME
        assert snapshot_filename.is_file(), snapshot_filename

        content_dir = destination / CONTENT_DIR_NAME

        content_prefix_dir = GetOutputPath(content_dir, common_source_path)
        assert content_prefix_dir.is_dir(), content_prefix_dir
    else:
        content_dir = destination
        content_prefix_dir = destination

    assert content_dir.is_dir(), content_dir

    source_files: List[FileInfo] = []

    for source in sources:
        source_files += Enumerate(source)

    content_files = list(Enumerate(content_dir))

    assert source_files
    assert content_files

    if expected_num_items is not None:
        assert len(content_files) == expected_num_items, (len(content_files), expected_num_items)

    mismatches = list(
        ValueComparison(
            source_files,
            common_source_path,
            content_files,
            content_prefix_dir,
            compare_file_contents=compare_file_contents,
        ),
    )

    assert not mismatches, mismatches


# ----------------------------------------------------------------------
def ScrubDurations(
    value: str,
) -> str:
    return re.sub(
        r"\d+\:\d+\:\d+(?:\.\d+)?",
        "<scrubbed duration>",
        value,
    )
