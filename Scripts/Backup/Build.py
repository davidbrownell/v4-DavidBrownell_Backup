# ----------------------------------------------------------------------
# |
# |  Build.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-21 10:01:40
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022-23
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Builds Backup"""

import re

from pathlib import Path

from Common_PythonDevelopment.BuildExeBuildInfo import BuildExeBuildInfo


# ----------------------------------------------------------------------
class BuildInfo(BuildExeBuildInfo):
    # ----------------------------------------------------------------------
    def __init__(self):
        super(BuildInfo, self).__init__(
            build_name="Backup",
            working_dir=Path(__file__).parent,
            required_development_configurations=[
                re.compile("dev"),
            ],
            disable_if_dependency_environment=True,
        )


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
if __name__ == "__main__":
    BuildInfo().Run()
