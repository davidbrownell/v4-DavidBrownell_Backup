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
Mirrors backup content: files created locally will be added to the backup data store; files deleted
locally will be removed from the backup data store; files modified locally will be modified at the
backup data store.
"""

import datetime
import textwrap

from pathlib import Path
from typing import cast, List, Optional, Pattern

import typer

from typer.core import TyperGroup

from Common_Foundation.Streams.DoneManager import DoneManager, DoneManagerFlags
from Common_Foundation import Types

from Impl import Common                     # type: ignore  # pylint: disable=import-error
from Impl import Mirror                     # type: ignore  # pylint: disable=import-error
import command_line                         # type: ignore  # pylint: disable=import-error


# ----------------------------------------------------------------------
class NaturalOrderGrouper(TyperGroup):
    # pylint: disable=missing-class-docstring
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
_destination_argument                       = typer.Argument(..., help="Destination data store used when mirroring local content; see the comments below for information on the different data store destination formats.")


# ----------------------------------------------------------------------
@app.command(
    "execute",
    epilog=Common.GetDestinationHelp(),
    no_args_is_help=True,
)
def Execute(
    destination: str=_destination_argument,
    input_filename_or_dirs: List[Path]=command_line.input_filename_or_dirs_argument,
    ssd: bool=command_line.ssd_option,
    force: bool=command_line.force_option,
    verbose: bool=command_line.verbose_option,
    quiet: bool=command_line.quiet_option,
    debug: bool=command_line.debug_option,
    file_include_params: Optional[List[str]]=command_line.file_include_option,
    file_exclude_params: Optional[List[str]]=command_line.file_exclude_option,
) -> None:
    """Mirrors content to a backup data store."""

    file_includes = cast(Optional[List[Pattern]], Types.EnsurePopulatedList(file_include_params))
    file_excludes = cast(Optional[List[Pattern]], Types.EnsurePopulatedList(file_exclude_params))

    with DoneManager.CreateCommandLine(
        output_flags=DoneManagerFlags.Create(verbose=verbose, debug=debug),
    ) as dm:
        dm.WriteVerbose(str(datetime.datetime.now()) + "\n\n")

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
        {}
        Validation Types
        ================
            standard: Validates that files and directories at the destination exist and file sizes match the expected values.
            complete: Validates that files and directories at the destination exist and file hashes match the expected values.
        """,
    ).replace("\n", "\n\n").format(Common.GetDestinationHelp()),
)
def Validate(
    destination: str=_destination_argument,
    validate_type: Mirror.ValidateType=typer.Argument(Mirror.ValidateType.standard, case_sensitive=False, help="Specifies the type of validation to use; the the comments below for information on the different validation types."),
    ssd: bool=command_line.ssd_option,
    verbose: bool=command_line.verbose_option,
    quiet: bool=command_line.quiet_option,
    debug: bool=command_line.debug_option,
) -> None:
    """Validates previously mirrored content in the backup data store."""

    with DoneManager.CreateCommandLine(
        output_flags=DoneManagerFlags.Create(verbose=verbose, debug=debug),
    ) as dm:
        dm.WriteVerbose(str(datetime.datetime.now()) + "\n\n")

        Mirror.Validate(
            dm,
            destination,
            validate_type,
            ssd=ssd,
            quiet=quiet,
        )


# ----------------------------------------------------------------------
@app.command(
    "cleanup",
    epilog=Common.GetDestinationHelp(),
    no_args_is_help=True,
)
def Cleanup(
    destination: str=_destination_argument,
    verbose: bool=command_line.verbose_option,
    debug: bool=command_line.debug_option,
) -> None:
    """Cleans a backup data store after a mirror execution that was interrupted or that failed."""

    with DoneManager.CreateCommandLine(
        output_flags=DoneManagerFlags.Create(verbose=verbose, debug=debug),
    ) as dm:
        dm.WriteVerbose(str(datetime.datetime.now()) + "\n\n")

        Mirror.Cleanup(dm, destination)


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
if __name__ == "__main__":
    app()
