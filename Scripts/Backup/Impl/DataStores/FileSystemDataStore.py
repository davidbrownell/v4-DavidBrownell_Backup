# ----------------------------------------------------------------------
# |
# |  FileSystemDataStore.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-25 10:47:58
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Contains the FileSystemDataStore object"""

import itertools
import os
import shutil

from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List, Optional, Tuple

from Common_Foundation import PathEx
from Common_Foundation.Types import overridemethod

from .FileBasedDataStore import FileBasedDataStore, ItemType


# ----------------------------------------------------------------------
class FileSystemDataStore(FileBasedDataStore):
    """DataStore associated with a standard, local file system"""

    # ----------------------------------------------------------------------
    def __init__(
        self,
        root: Path=Path.cwd(),
        *,
        ssd: bool=False,
        is_local_filesystem_override_value_for_testing: Optional[bool]=None,
    ):
        assert not root.exists() or root.is_dir(), root

        super(FileSystemDataStore, self).__init__(
            is_local_filesystem=is_local_filesystem_override_value_for_testing if is_local_filesystem_override_value_for_testing is not None else root.root == Path.cwd().root,
        )

        self._working_dir: Path             = root
        self._ssd                           = ssd

    # ----------------------------------------------------------------------
    @overridemethod
    def ExecuteInParallel(self) -> bool:
        return self._ssd

    # ----------------------------------------------------------------------
    @overridemethod
    def ValidateBackupInputs(
        self,
        input_filename_or_dirs: List[Path],
    ) -> None:
        for input_filename_or_dir in input_filename_or_dirs:
            if input_filename_or_dir.is_file():
                input_dir = input_filename_or_dir.parent
            elif input_filename_or_dir.is_dir():
                input_dir = input_filename_or_dir
            else:
                raise Exception("'{}' is not a supported item type.".format(input_filename_or_dir))

            if PathEx.IsDescendant(self._working_dir, input_dir):
                raise Exception(
                    "The directory '{}' overlaps with the destination path '{}'.".format(
                        input_filename_or_dir,
                        self._working_dir,
                    ),
                )

    # ----------------------------------------------------------------------
    @overridemethod
    def SnapshotFilenameToDestinationName(
        self,
        path: Path,
    ) -> Path:
        if path.parts[0] == "/":
            path = Path(*path.parts[1:])
        elif path.parts[0]:
            # Probably on Windows
            path = Path(path.parts[0].replace(":", "_").rstrip("\\")) / Path(*path.parts[1:])

        return self.GetWorkingDir() / path

    # ----------------------------------------------------------------------
    @overridemethod
    def GetBytesAvailable(self) -> Optional[int]:
        # Find a directory that exists
        for potential_dir in itertools.chain(
            [self._working_dir, ],
            self._working_dir.parents,
            [Path.cwd(), ],
        ):
            if potential_dir.is_dir():
                return shutil.disk_usage(potential_dir).free

        assert False, self._working_dir

    # ----------------------------------------------------------------------
    @overridemethod
    def GetWorkingDir(self) -> Path:
        return self._working_dir

    # ----------------------------------------------------------------------
    @overridemethod
    def SetWorkingDir(
        self,
        path: Path,
    ) -> None:
        self._working_dir /= path

    # ----------------------------------------------------------------------
    @overridemethod
    def GetItemType(
        self,
        path: Path,
    ) -> Optional[ItemType]:
        path = self._working_dir / path

        if not path.exists():
            return None

        if path.is_symlink():
            return ItemType.SymLink

        if path.is_file():
            return ItemType.File

        if path.is_dir():
            return ItemType.Dir

        raise Exception("'{}' is not a known type".format(path))

    # ----------------------------------------------------------------------
    @overridemethod
    def GetFileSize(
        self,
        path: Path,
    ) -> int:
        return (self._working_dir / path).stat().st_size

    # ----------------------------------------------------------------------
    @overridemethod
    def RemoveDir(
        self,
        path: Path,
    ) -> None:
        PathEx.RemoveTree(self._working_dir / path)

    # ----------------------------------------------------------------------
    @overridemethod
    def RemoveFile(
        self,
        path: Path,
    ) -> None:
        PathEx.RemoveItem(self._working_dir / path)

    # ----------------------------------------------------------------------
    @overridemethod
    def RemoveItem(
        self,
        path: Path,
    ) -> None:
        PathEx.RemoveItem(self._working_dir / path)

    # ----------------------------------------------------------------------
    @overridemethod
    def MakeDirs(
        self,
        path: Path,
    ) -> None:
        (self._working_dir / path).mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------------------
    @overridemethod
    @contextmanager
    def Open(
        self,
        filename: Path,
        *args,
        **kwargs,
    ):
        with (self._working_dir / filename).open(*args, **kwargs) as f:
            yield f

    # ----------------------------------------------------------------------
    @overridemethod
    def Rename(
        self,
        old_path: Path,
        new_path: Path,
    ) -> None:
        old_path = self._working_dir / old_path
        new_path = self._working_dir / new_path

        PathEx.RemoveItem(new_path)
        shutil.move(old_path, new_path)

    # ----------------------------------------------------------------------
    @overridemethod
    def Walk(
        self,
        path: Path=Path(),
    ) -> Generator[
        Tuple[
            Path,                           # root
            List[str],                      # directories
            List[str],                      # filenames
        ],
        None,
        None,
    ]:
        for root, directories, filenames in os.walk(self._working_dir / path):
            yield Path(root), directories, filenames
