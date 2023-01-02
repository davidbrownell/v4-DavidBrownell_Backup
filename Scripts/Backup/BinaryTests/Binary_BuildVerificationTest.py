# ----------------------------------------------------------------------
# |
# |  Binary_BuildVerificationTest.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-22 10:42:13
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022-23
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Build verification test for binaries"""

# Note that this file will be invoked outside of an activated environment and cannot take a dependency
# on anything in this repository or Common_Foundation.

import os
import subprocess
import stat
import sys
import textwrap

from pathlib import Path
from typing import List, Optional, Set


# ----------------------------------------------------------------------
def EntryPoint(
    args: List[str],
) -> int:
    if len(args) != 2:
        sys.stdout.write(
            textwrap.dedent(
                """\
                ERROR: Usage:

                    {} <temp_directory>

                """,
            ).format(
                args[0],
            ),
        )

        return -1

    temp_directory = Path(args[1])
    assert temp_directory.is_dir(), temp_directory

    build_output_dir = Path(temp_directory)

    # Get the Backup binary
    for potential_dir in [
        "artifacts",
        "Scripts",
        "Backup",
        "Build",
    ]:
        potential_build_output_dir = build_output_dir / potential_dir
        if not potential_build_output_dir.is_dir():
            break

        build_output_dir = potential_build_output_dir

    backup_filename = build_output_dir / "Backup"

    if not backup_filename.is_file():
        potential_backup_filename = backup_filename.with_suffix(".exe")

        if potential_backup_filename.is_file():
            backup_filename = potential_backup_filename

    if not backup_filename.is_file():
        raise Exception("The filename '{}' does not exist.\n".format(backup_filename))

    # https://github.com/actions/upload-artifact/issues/38
    # Permissions are not currently being saved when uploading artifacts, so they must be set here.
    # This will eventually be fixed, which is why I am placing the work around here rather than in
    # the artifact upload- or download-code.

    backup_filename.chmod(stat.S_IXUSR | stat.S_IWUSR | stat.S_IRUSR)

    # Execute Tests
    result = _ValidateMirror(backup_filename, temp_directory)
    if result != 0:
        return result

    return 0


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
def _ValidateMirror(
    backup_filename: Path,
    temp_directory: Path,
) -> int:
    source_dir = Path(__file__).parent.parent.parent.parent
    destination = source_dir.parent / "destination"

    command_line = '"{}" mirror execute "{}" "{}"'.format(
        backup_filename,
        destination,
        source_dir,
    )

    sys.stdout.write("Command Line: {}\n\n".format(command_line))

    result = subprocess.run(
        command_line,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    content = result.stdout.decode("utf-8")

    sys.stdout.write(content)

    if result.returncode != 0:
        return result.returncode

    # Compare the source and destination files

    # ----------------------------------------------------------------------
    def DecorateFilename(
        filename: Path,
        root: Path,
        prefix_to_strip: Optional[Path]=None,
    ) -> Path:
        filename = Path(*filename.parts[len(root.parts):])

        if prefix_to_strip:
            len_prefix_to_strip_parts = len(prefix_to_strip.parts)

            assert filename.parts[:len_prefix_to_strip_parts] == prefix_to_strip.parts, (filename.parts, prefix_to_strip.parts)
            filename = Path(*filename.parts[len_prefix_to_strip_parts:])

        return filename

    # ----------------------------------------------------------------------
    def GetFiles(
        root: Path,
        prefix_to_strip: Optional[Path]=None,
    ) -> Set[Path]:
        if prefix_to_strip:
            if prefix_to_strip.parts[0].endswith(":") or prefix_to_strip.parts[0].endswith(":\\"):
                prefix_to_strip = Path(prefix_to_strip.parts[0].replace(":", "_").rstrip("\\")) / Path(*prefix_to_strip.parts[1:])
            else:
                assert prefix_to_strip.parts[0] == "/", prefix_to_strip.parts
                prefix_to_strip = Path(*prefix_to_strip.parts[1:])

        results: Set[Path] = set()

        for this_root, directories, filenames in os.walk(root):
            this_root = Path(this_root)

            if not directories and not filenames:
                results.add(DecorateFilename(this_root, root, prefix_to_strip))

            for filename in filenames:
                results.add(DecorateFilename(this_root / filename, root, prefix_to_strip))

        return results

    # ----------------------------------------------------------------------

    source_files = GetFiles(source_dir)
    destination_files = GetFiles(destination / "Content", source_dir)

    if source_files != destination_files:
        sys.stdout.write("Source Files:\n{}\n".format("".join("  - {}) {}\n".format(index, source_file) for index, source_file in enumerate(source_files))))
        sys.stdout.write("Destination Files:\n{}\n".format("".join("  - {}) {}\n".format(index, destination_file) for index, destination_file in enumerate(destination_files))))

        return -1

    return 0


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
if __name__ == "__main__":
    sys.exit(EntryPoint(sys.argv))
