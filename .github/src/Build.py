# ----------------------------------------------------------------------
# |
# |  Build.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-23 09:07:25
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022-23
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Builds github content"""

import os
import sys

from pathlib import Path

import typer

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx
from Common_Foundation import Types

foundation_root_dir = PathEx.EnsureDir(Path(Types.EnsureValid(os.getenv("DEVELOPMENT_ENVIRONMENT_FOUNDATION"))))

github_src_root = PathEx.EnsureDir(foundation_root_dir / ".github" / "src")

sys.path.insert(0, str(github_src_root))
with ExitStack(lambda: sys.path.pop(0)):
    from BuildImpl import CreateBuildInfoInstance, CreateTagsImpl  # type: ignore  # pylint: disable=import-error


# ----------------------------------------------------------------------
_build_info = CreateBuildInfoInstance(Path(__file__).parent)


# ----------------------------------------------------------------------
def CreateTags(
    dry_run: bool=typer.Option(False, "--dry-run", help="Prints the command line used to create the tags and push them to GitHub, but does not invoke the functionality."),
    yes: bool=typer.Option(False, "--yes", help="Answer yes to the 'are you sure' prompt."),
    verbose: bool=typer.Option(False, "--verbose", help="Write verbose information to the terminal."),
    debug: bool=typer.Option(False, "--debug", help="Write debug information to the terminal."),
):
    """Creates CI-related tags and pushes them to GitHub."""

    return CreateTagsImpl(
        dry_run=dry_run,
        yes=yes,
        verbose=verbose,
        debug=debug,
    )


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
if __name__ == "__main__":
    _build_info.Run()
