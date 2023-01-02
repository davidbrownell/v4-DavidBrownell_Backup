# ----------------------------------------------------------------------
# |
# |  Setup_custom.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-11 08:45:16
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022-23
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
# pylint: disable=missing-module-docstring

import os
import uuid

from pathlib import Path
from typing import Dict, List, Optional, Union

from semantic_version import Version as SemVer

from Common_Foundation.Shell import Commands                                # type: ignore  # pylint: disable=import-error,unused-import
from Common_Foundation.Streams.DoneManager import DoneManager               # type: ignore  # pylint: disable=import-error,unused-import
from Common_Foundation import Types                                         # type: ignore  # pylint: disable=import-error,unused-import

from RepositoryBootstrap import Configuration                               # type: ignore  # pylint: disable=import-error,unused-import
from RepositoryBootstrap import Constants                                   # type: ignore  # pylint: disable=import-error,unused-import


# ----------------------------------------------------------------------
# Uncomment the decorator below to make this repository a mixin repository.
# Mixin repositories can not be activated on their own and cannot have tool
# or version specifications. Mixin repositories are valuable because they
# can provide scripts or tools that augment other repositories when activated.
#
# @Configuration.MixinRepository
def GetConfigurations() -> Union[
    Configuration.Configuration,
    Dict[
        str,                                # configuration name
        Configuration.Configuration,
    ],
]:
    common_python_libraries: List[Configuration.VersionInfo] = [
        Configuration.VersionInfo("paramiko", SemVer("2.12.0")),
    ]

    configurations: Dict[str, Configuration.Configuration] = {
        "standard": Configuration.Configuration(
            "Configuration for using the backup tools and functionality.",
            [
                Configuration.Dependency(
                    uuid.UUID("DD6FCD30-B043-4058-B0D5-A6C8BC0374F4"),
                    "Common_Foundation",
                    "python310",
                    "https://github.com/davidbrownell/v4-Common_Foundation.git",
                ),
            ],
            Configuration.VersionSpecs(
                [],                             # tools
                {
                    "Python": common_python_libraries,
                },                             # libraries
            ),
        ),
        "dev": Configuration.Configuration(
            "Configuration for developing the backup tools and functionality.",
            [
                Configuration.Dependency(
                    uuid.UUID("e4170a9d-70f9-4615-85b6-d514055e62b6"),
                    "Common_PythonDevelopment",
                    "python310",
                    "https://github.com/davidbrownell/v4-Common_PythonDevelopment.git",
                ),
            ],
            Configuration.VersionSpecs(
                [],                             # tools
                {
                    "Python": common_python_libraries + [
                        Configuration.VersionInfo("cx_freeze", SemVer("6.13.1")),
                    ],
                },                             # libraries
            ),
        ),
    }

    return configurations


# ----------------------------------------------------------------------
# Note that it is safe to remove this function if it will never be used.
def GetCustomActions(
    # Note that it is safe to remove any parameters that are not used
    dm: DoneManager,                                    # pylint: disable=unused-argument
    explicit_configurations: Optional[List[str]],       # pylint: disable=unused-argument
    force: bool,                                        # pylint: disable=unused-argument
    interactive: Optional[bool],                        # pylint: disable=unused-argument
) -> List[Commands.Command]:
    """Return custom actions invoked as part of the setup process for this repository"""

    commands: List[Commands.Command] = []

    root_dir = Path(__file__).parent
    assert root_dir.is_dir(), root_dir

    # Create a link to the foundation's .pylintrc file
    foundation_root_file = Path(Types.EnsureValid(os.getenv(Constants.DE_FOUNDATION_ROOT_NAME))) / ".pylintrc"
    assert foundation_root_file.is_file(), foundation_root_file

    commands.append(
        Commands.SymbolicLink(
            root_dir / foundation_root_file.name,
            foundation_root_file,
            remove_existing=True,
            relative_path=True,
        ),
    )

    return commands
