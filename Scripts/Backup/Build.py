# ----------------------------------------------------------------------
# |
# |  Build.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-21 10:01:40
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Builds Backup"""

import os
import re
import shutil

from enum import auto, Enum
from pathlib import Path
from typing import Callable, List, Optional, TextIO, Tuple, Union

from Common_Foundation import PathEx
from Common_Foundation.Streams.DoneManager import DoneManager, DoneManagerFlags
from Common_Foundation import SubprocessEx
from Common_Foundation.Types import overridemethod

from Common_FoundationEx.BuildImpl import BuildInfoBase
from Common_FoundationEx import TyperEx


# ----------------------------------------------------------------------
class BuildInfo(BuildInfoBase):
    # ----------------------------------------------------------------------
    def __init__(self):
        super(BuildInfo, self).__init__(
            name="Backup",
            requires_output_dir=True,
            required_development_configurations=[
                re.compile("dev"),
            ],
            disable_if_dependency_environment=True,
        )

    # ----------------------------------------------------------------------
    @overridemethod
    def Clean(                              # pylint: disable=arguments-differ
        self,
        configuration: Optional[str],       # pylint: disable=unused-argument
        output_dir: Path,
        output_stream: TextIO,
        on_progress_update: Callable[       # pylint: disable=unused-argument
            [
                int,                        # Step ID
                str,                        # Status info
            ],
            bool,                           # True to continue, False to terminate
        ],
        *,
        is_verbose: bool,
        is_debug: bool,
    ) -> Union[
        int,                                # Error code
        Tuple[int, str],                    # Error code and short text that provides info about the result
    ]:
        with DoneManager.Create(
            output_stream,
            "Cleaning '{}'...".format(output_dir),
            output_flags=DoneManagerFlags.Create(verbose=is_verbose, debug=is_debug),
        ) as dm:
            if not output_dir.is_dir():
                dm.WriteInfo("The directory '{}' does not exist.\n".format(output_dir))
            else:
                PathEx.RemoveTree(output_dir)

        return 0

    # ----------------------------------------------------------------------
    @overridemethod
    def GetCustomBuildArgs(self) -> TyperEx.TypeDefinitionsType:
        """Return argument descriptions for any custom args that can be passed to the Build func on the command line"""

        # No custom args by default
        return {}

    # ----------------------------------------------------------------------
    @overridemethod
    def GetNumBuildSteps(
        self,
        configuration: Optional[str],  # pylint: disable=unused-argument
    ) -> int:
        return len(self.__class__._BuildSteps)  # pylint: disable=protected-access

    # ----------------------------------------------------------------------
    @overridemethod
    def Build(                              # pylint: disable=arguments-differ
        self,
        configuration: Optional[str],       # pylint: disable=unused-argument
        output_dir: Path,
        output_stream: TextIO,
        on_progress_update: Callable[       # pylint: disable=unused-argument
            [
                int,                        # Step ID
                str,                        # Status info
            ],
            bool,                           # True to continue, False to terminate
        ],
        *,
        is_verbose: bool,
        is_debug: bool,
        force: bool=False,
    ) -> Union[
        int,                                # Error code
        Tuple[int, str],                    # Error code and short text that provides info about the result
    ]:
        this_dir = Path(__file__).parent

        with DoneManager.Create(
            output_stream,
            "Building '{}'...".format(output_dir),
            output_flags=DoneManagerFlags.Create(verbose=is_verbose, debug=is_debug),
        ) as dm:
            with dm.Nested(
                "Running setup...",
                suffix="\n" if dm.is_verbose else "",
            ) as build_dm:
                on_progress_update(self.__class__._BuildSteps.Build.value, "Building...")  # pylint: disable=protected-access

                command_line = 'python setup.py build_exe'

                build_dm.WriteVerbose("Command Line: {}\n\n".format(command_line))

                result = SubprocessEx.Run(
                    command_line,
                    cwd=this_dir,
                    supports_colors=False,
                )

                build_dm.result = result.returncode

                if build_dm.result != 0:
                    build_dm.WriteError(result.output)
                    return build_dm.result

                with build_dm.YieldVerboseStream() as stream:
                    stream.write(result.output)

            build_dir = PathEx.EnsureDir(this_dir / "build")

            with dm.Nested(
                "Pruning...",
                suffix="\n" if dm.is_verbose else "",
            ) as prune_dm:
                on_progress_update(self.__class__._BuildSteps.Prune.value, "Pruning...")  # pylint: disable=protected-access

                directories: List[Path] = []

                for root, _, _ in os.walk(build_dir):
                    directories.append(Path(root))

                for directory in reversed(directories):
                    if not any(item for item in directory.iterdir()):
                        with prune_dm.VerboseNested("Removing '{}'...".format(directory)):
                            PathEx.RemoveTree(directory)

                if prune_dm.result != 0:
                    return prune_dm.result

            with dm.Nested("Moving content...") as move_dm:
                build_children: List[Path] = list(build_dir.iterdir())

                assert len(build_children) == 1

                content_dir = build_children[0]

                PathEx.RemoveTree(output_dir)
                output_dir.mkdir(parents=True)

                for child in content_dir.iterdir():
                    shutil.move(child, output_dir)

                PathEx.RemoveTree(build_dir)

                if move_dm.result != 0:
                    return move_dm.result

            return dm.result

    # ----------------------------------------------------------------------
    # |
    # |  Private Types
    # |
    # ----------------------------------------------------------------------
    class _BuildSteps(Enum):
        Build                               = 0
        Prune                               = auto()
        Move                                = auto()


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
if __name__ == "__main__":
    BuildInfo().Run()
