# ----------------------------------------------------------------------
# |
# |  command_line.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-28 14:06:02
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022-23
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Contains functionality used when processing the command line"""

import re

from typing import List, Pattern

import typer


# ----------------------------------------------------------------------
def ToRegex(
    values: List[str],
) -> List[Pattern]:
    expressions: List[Pattern] = []

    for value in values:
        try:
            expressions.append(re.compile("^{}$".format(value)))
        except re.error as ex:
            raise typer.BadParameter("The regular expression '{}' is not valid ({}).".format(value, ex))

    return expressions


# ----------------------------------------------------------------------
input_filename_or_dirs_argument             = typer.Argument(..., exists=True, resolve_path=True, help="Input filename or directory.")

ssd_option                                  = typer.Option(False, "--ssd", help="Processes tasks in parallel to leverage the capabilities of solid-state-drives.")
quiet_option                                = typer.Option(False, "--quiet", help="Reduce the amount of information displayed.")

force_option                                = typer.Option(False, "--force", help="Ignore previous backup information and overwrite all data in the destination data store.")
verbose_option                              = typer.Option(False, "--verbose", help="Write verbose information to the terminal.")
debug_option                                = typer.Option(False, "--debug", help="Write debug information to the terminal.")

file_include_option                         =typer.Option(None, "--file-include", callback=ToRegex, help="Regular expression (based on a posix path) used to include files and/or directories when preserving content.")
file_exclude_option                         =typer.Option(None, "--file-exclude", callback=ToRegex, help="Regular expression (based on a posix path) used to exclude files and/or directories when preserving content.")
