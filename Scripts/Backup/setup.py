# ----------------------------------------------------------------------
# |
# |  setup.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-22 08:24:27
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Setup for Backup"""

import sys
import textwrap

from pathlib import Path

from cx_Freeze import setup, Executable

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx


# ----------------------------------------------------------------------
_this_dir                                   = Path(__file__).parent
_name                                       = _this_dir.name


# ----------------------------------------------------------------------
sys.path.insert(0, str(PathEx.EnsureDir(_this_dir / "..")))
with ExitStack(lambda: sys.path.pop(0)):
    # We have to import in this way to get the proper doc string from __main__.py
    from Backup import __main__ as Backup


# ----------------------------------------------------------------------
setup(
    name=_name,
    version="1.0.0",
    description=Backup.__doc__,
    executables=[
        Executable(
            PathEx.EnsureFile(Path(__file__).parent / "__main__.py"),
            base=None,
            copyright=textwrap.dedent(
                """\
                Copyright David Brownell 2022
                Distributed under the Boost Software License, Version 1.0. See
                copy at http://www.boost.org/LICENSE_1_0.txt.
                """,
            ),
            # icon=<icon_filename>
            target_name=_name,
            # trademarks="",
        ),
    ],
    options={
        "build_exe": {
            "excludes": [],
            "includes": [],
            "no_compress": False,
            "optimize": 0,
            "packages": [],
        },
    },
)
