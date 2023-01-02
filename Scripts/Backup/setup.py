# ----------------------------------------------------------------------
# |
# |  setup.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-22 08:24:27
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022-23
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Setup for Backup"""

import datetime
import sys
import textwrap

from pathlib import Path

from cx_Freeze import setup, Executable

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx


# ----------------------------------------------------------------------
_this_dir                                   = Path(__file__).parent
_repo_root                                  = _this_dir.parent.parent

_name                                       = _this_dir.name


# ----------------------------------------------------------------------
sys.path.insert(0, str(PathEx.EnsureDir(_this_dir / "..")))
with ExitStack(lambda: sys.path.pop(0)):
    # We have to import in this way to get the proper doc string from __main__.py
    from Backup import __main__ as Backup


# Read version info from VERSION file
with PathEx.EnsureFile(_repo_root / "VERSION").open() as f:
    _version = f.read().strip()
    assert _version

# Create the year suffix
_year = datetime.datetime.now().year

if _year == 2022:
    _year_suffix = ""  # pylint: disable=invalid-name
else:
    if _year < 2100:
        _year = _year % 100

    _year_suffix = "-" + str(_year)  # pylint: disable=invalid-name


# ----------------------------------------------------------------------
setup(
    name=_name,
    version=_version,
    description=Backup.__doc__,
    executables=[
        Executable(
            PathEx.EnsureFile(Path(__file__).parent / "__main__.py"),
            base=None,
            copyright=textwrap.dedent(
                """\
                Copyright David Brownell {year}{year_suffix}
                Distributed under the Boost Software License, Version 1.0. See
                copy at http://www.boost.org/LICENSE_1_0.txt.
                """,
            ).format(
                year="2022", # Formatted in this way as to not participate in updates via the UpdateCopyrights script
                year_suffix=_year_suffix,
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
