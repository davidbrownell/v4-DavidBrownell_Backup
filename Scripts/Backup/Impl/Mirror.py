# ----------------------------------------------------------------------
# |
# |  Mirror.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-13 08:09:07
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Mirror functionality"""

from pathlib import Path
from typing import List, Optional, Pattern

from Common_Foundation import PathEx
from Common_Foundation.Streams.DoneManager import DoneManager

from .Snapshot import Snapshot
from .Destinations.Destination import Destination, ValidateType
from .Destinations.FileSystemDestination import FileSystemDestination


# ----------------------------------------------------------------------
def Backup(
    dm: DoneManager,
    destination: str,
    input_filenames_or_dirs: List[Path],
    *,
    ssd: bool,
    force: bool,
    quiet: bool,
    file_includes: Optional[List[Pattern]],
    file_excludes: Optional[List[Pattern]],
) -> None:
    mirror: Optional[Destination] = None

    if True: # TODO: Hard coded as FileSystemDestination
        destination_path = Path(destination)

        # ----------------------------------------------------------------------
        def ValidateFilesystemDestinationInput(
            input_filename_or_dir: Path,
        ) -> None:
            if input_file_or_dir.is_file():
                input_dir = input_file_or_dir.parent
            else:
                input_dir = input_file_or_dir

            if PathEx.IsDescendant(destination_path, input_dir):
                raise Exception(
                    "The directory '{}' overlaps with the destination path '{}'.".format(
                        input_filename_or_dir,
                        destination_path,
                    ),
                )

        # ----------------------------------------------------------------------

        validate_input_func = ValidateFilesystemDestinationInput

        mirror = FileSystemDestination(
            destination_path,
            force=force,
            is_ssd=ssd,
            quiet=quiet,
        )

    assert mirror is not None

    # Process the inputs
    for input_file_or_dir in input_filenames_or_dirs:
        if not input_file_or_dir.exists():
            raise Exception("'{}' is not a valid filename or directory.".format(input_file_or_dir))

        validate_input_func(input_file_or_dir)

    if file_includes or file_excludes:
        # ----------------------------------------------------------------------
        def SnapshotFilter(
            filename: Path,
        ) -> bool:
            filename_str = filename.as_posix()

            if file_excludes is not None and any(exclude.match(filename_str) for exclude in file_excludes):
                return False

            if file_includes is not None and not any(include.match(filename_str) for include in file_includes):
                return False

            return True

        # ----------------------------------------------------------------------

        filter_filename_func = SnapshotFilter
    else:
        filter_filename_func = None

    # Load the local content
    local_snapshot = Snapshot.Calculate(
        dm,
        input_filenames_or_dirs,
        is_ssd=ssd,
        filter_filename_func=filter_filename_func,
        quiet=quiet,
    )

    if dm.result != 0:
        return

    # Load the destination content
    with dm.Nested(
        "\nCalculating mirrored content...",
        suffix="\n",
    ) as calculate_dm:
        mirrored_snapshot = mirror.GetMirroredSnapshot(calculate_dm)

        if calculate_dm.result != 0:
            return

    with dm.Nested("Applying mirrored content...") as mirror_dm:
        mirror.ProcessMirroredSnapshot(
            mirror_dm,
            local_snapshot,
            mirrored_snapshot,
        )

        if mirror_dm.result != 0:
            return


# ----------------------------------------------------------------------
def Cleanup(
    dm: DoneManager,
    destination: str,
    *,
    ssd: bool,
    quiet: bool,
) -> None:
    # TODO: Hard-coded as FileSystemDestination
    mirror = FileSystemDestination(
        Path(destination),
        force=False,
        is_ssd=ssd,
        quiet=quiet,
    )

    mirror.CleanPreviousRun(dm)


# ----------------------------------------------------------------------
def Validate(
    dm: DoneManager,
    destination: str,
    validate_type: ValidateType,
    *,
    ssd: bool,
    quiet: bool,
) -> None:
    # TODO: Hard-coded as FileSystemDestination
    mirror = FileSystemDestination(
        Path(destination),
        force=False,
        is_ssd=ssd,
        quiet=quiet,
    )

    mirror.Validate(dm, validate_type)
