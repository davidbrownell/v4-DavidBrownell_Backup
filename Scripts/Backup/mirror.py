# ----------------------------------------------------------------------
# |
# |  Mirror.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-13 22:22:53
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""\
Mirrors backup content: files created locally will be added to the backup location; files deleted
locally will be removed from the backup location; files modified locally will be modified at the
backup location.
"""

import re
import textwrap

from pathlib import Path
from typing import cast, List, Optional, Pattern

import typer

from typer.core import TyperGroup

from Common_Foundation.Streams.DoneManager import DoneManager, DoneManagerFlags
from Common_Foundation import Types

from Impl import Mirror  # type: ignore  # pylint: disable=import-error


# ----------------------------------------------------------------------
class NaturalOrderGrouper(TyperGroup):
    # ----------------------------------------------------------------------
    def list_commands(self, *args, **kwargs):  # pylint: disable=unused-argument
        return self.commands.keys()


# ----------------------------------------------------------------------
app                                         = typer.Typer(
    cls=NaturalOrderGrouper,
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
    pretty_exceptions_enable=False,
)


# ----------------------------------------------------------------------
_destination_argument                       = typer.Argument(..., help="Destination to mirror content or of previously mirrored content; this value can be a path to a directory.")
_ssd_option                                 = typer.Option(False, "--ssd", help="Processes tasks in parallel to leverage the capabilities of solid-state-drives.")
_quiet_option                               = typer.Option(False, "--quiet", help="Reduce the amount of information displayed.")


# ----------------------------------------------------------------------
def _ToRegex(
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
@app.command("execute", no_args_is_help=True)
def Execute(
    destination: str=_destination_argument,
    input_filename_or_dirs: List[Path]=typer.Argument(..., exists=True, resolve_path=True, help="Input filename or directory."),
    ssd: bool=_ssd_option,
    force: bool=typer.Option(False, "--force", help="Overwrite all data at the destination."),
    verbose: bool=typer.Option(False, "--verbose", help="Write verbose information to the terminal."),
    quiet: bool=_quiet_option,
    debug: bool=typer.Option(False, "--debug", help="Write debug information to the terminal."),
    file_include_params: Optional[List[str]]=typer.Option(None, "--file-include", callback=_ToRegex, help="Regular expression used to include files and/or directories when mirroring content."),
    file_exclude_params: Optional[List[str]]=typer.Option(None, "--file-exclude", callback=_ToRegex, help="Regular expression used to exclude files and/or directories when mirroring content."),
) -> None:
    """Mirrors content to a backup location."""

    file_includes = cast(Optional[List[Pattern]], Types.EnsurePopulatedList(file_include_params))
    file_excludes = cast(Optional[List[Pattern]], Types.EnsurePopulatedList(file_exclude_params))

    with DoneManager.CreateCommandLine(
        output_flags=DoneManagerFlags.Create(verbose=verbose, debug=debug),
    ) as dm:
        Mirror.Backup(
            dm,
            destination,
            input_filename_or_dirs,
            ssd=ssd,
            force=force,
            quiet=quiet,
            file_includes=file_includes,
            file_excludes=file_excludes,
        )


# ----------------------------------------------------------------------
@app.command(
    "validate",
    no_args_is_help=True,
    epilog=textwrap.dedent(
        """\
        Validation Types:

            standard: Validates that files and directories exist and that file sizes match
            complete: Validates that files and directories exist and that file hashes match
        """,
    ).replace("\n", "\n\n"),
)
def Validate(
    destination: str=_destination_argument,
    validate_type: Mirror.ValidateType=typer.Argument(Mirror.ValidateType.standard, case_sensitive=False, help="Specifies the type of validation to use."),
    ssd: bool=_ssd_option,
    verbose: bool=typer.Option(False, "--verbose", help="Write verbose information to the terminal."),
    quiet: bool=_quiet_option,
    debug: bool=typer.Option(False, "--debug", help="Write debug information to the terminal."),
) -> None:
    """Validates previously mirrored content at the backup location."""

    with DoneManager.CreateCommandLine(
        output_flags=DoneManagerFlags.Create(verbose=verbose, debug=debug),
    ) as dm:
        Mirror.Validate(
            dm,
            destination,
            validate_type,
            ssd=ssd,
            quiet=quiet,
        )


# ----------------------------------------------------------------------
@app.command("cleanup", no_args_is_help=True)
def Cleanup(
    destination: str=_destination_argument,
    ssd: bool=_ssd_option,
    verbose: bool=typer.Option(False, "--verbose", help="Write verbose information to the terminal."),
    quiet: bool=_quiet_option,
    debug: bool=typer.Option(False, "--debug", help="Write debug information to the terminal."),
) -> None:
    """Cleans a backup location after a mirror execution that was interrupted or that failed."""

    with DoneManager.CreateCommandLine(
        output_flags=DoneManagerFlags.Create(verbose=verbose, debug=debug),
    ) as dm:
        Mirror.Cleanup(
            dm,
            destination,
            ssd=ssd,
            quiet=quiet,
        )


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
if __name__ == "__main__":
    app()
