# ----------------------------------------------------------------------
# |
# |  offsite.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-28 13:53:29
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
Generates content suitable for offsite backup; files created and modified locally will be added to the backup data store; files
deleted locally will not be removed from the backup data store.
"""

import datetime

from contextlib import contextmanager
from pathlib import Path
from typing import cast, Iterator, List, Optional, Pattern

import typer

from typer.core import TyperGroup

from Common_Foundation import PathEx
from Common_Foundation.Shell.All import CurrentShell
from Common_Foundation.Streams.DoneManager import DoneManager, DoneManagerFlags
from Common_Foundation import Types

from Common_FoundationEx import TyperEx

from Impl import Common                     # type: ignore  # pylint: disable=import-error
from Impl import Offsite                    # type: ignore  # pylint: disable=import-error
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
    help=__doc__,
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
    pretty_exceptions_enable=False,
)


# ----------------------------------------------------------------------
_backup_name_argument                       = typer.Argument(..., help="Unique name of the backup; this value allows for multiple distinct backups on the same machine.")

# ----------------------------------------------------------------------
@app.command(
    "execute",
    epilog=Common.GetDestinationHelp(),
    no_args_is_help=True,
)
def Execute(  # pylint: disable=too-many-arguments;tool-many-locals
    backup_name: str=_backup_name_argument,
    destination: str=typer.Argument(..., help="Destination data store used to backup content. This value can be 'None' if the backup content should be created locally but manually distributed to the data store (this can be helpful when initially creating backups that are hundreds of GB in size)."),
    input_filename_or_dirs: List[Path]=command_line.input_filename_or_dirs_argument,
    encryption_password: Optional[str]=typer.Option(None, "--encryption-password", help="Encrypt the contents for backup prior to transferring them to the destination data store."),
    compress: bool=typer.Option(False, "--compress", help="Compress the contents to backup prior to transferring them to the destination data store."),
    ssd: bool=command_line.ssd_option,
    force: bool=command_line.force_option,
    verbose: bool=command_line.verbose_option,
    quiet: bool=command_line.quiet_option,
    debug: bool=command_line.debug_option,
    working_dir: Optional[Path]=typer.Option(None, file_okay=False, resolve_path=True, help="Local directory used to stage files prior to transferring them to the destination data store."),
    archive_volume_size: int=typer.Option(Offsite.DEFAULT_ARCHIVE_VOLUME_SIZE, min=1024, help="Compressed/encrypted data will be converted to volumes of this size for easier transmission to the data store; value expressed in terms of bytes."),
    ignore_pending_snapshot: bool=typer.Option(False, "--ignore-pending-snapshot", help="Disable the pending warning snapshot and continue."),
    file_include_params: Optional[List[str]]=command_line.file_include_option,
    file_exclude_params: Optional[List[str]]=command_line.file_exclude_option,
) -> None:
    """Prepares local changes for offsite backup."""

    file_includes = cast(Optional[List[Pattern]], Types.EnsurePopulatedList(file_include_params))
    file_excludes = cast(Optional[List[Pattern]], Types.EnsurePopulatedList(file_exclude_params))

    with DoneManager.CreateCommandLine(
        output_flags=DoneManagerFlags.Create(verbose=verbose, debug=debug),
    ) as dm:
        dm.WriteVerbose(str(datetime.datetime.now()) + "\n\n")

        destination_value = None if destination.lower() == "none" else destination

        with _ResolveWorkingDir(
            dm,
            working_dir,
            always_preserve=destination_value is None,
        ) as resolved_working_dir:
            Offsite.Backup(
                dm,
                input_filename_or_dirs,
                backup_name,
                destination_value,
                encryption_password,
                resolved_working_dir,
                compress=compress,
                ssd=ssd,
                force=force,
                quiet=quiet,
                file_includes=file_includes,
                file_excludes=file_excludes,
                archive_volume_size=archive_volume_size,
                ignore_pending_snapshot=ignore_pending_snapshot,
            )


# ----------------------------------------------------------------------
@app.command("commit", no_args_is_help=True)
def Commit(
    backup_name: str=_backup_name_argument,
    verbose: bool=command_line.verbose_option,
    debug: bool=command_line.debug_option,
) -> None:
    """Commits a pending snapshot after the changes have been transferred to an offsite data store."""

    with DoneManager.CreateCommandLine(
        output_flags=DoneManagerFlags.Create(verbose=verbose, debug=debug),
    ) as dm:
        dm.WriteVerbose(str(datetime.datetime.now()) + "\n\n")

        Offsite.Commit(dm, backup_name)


# ----------------------------------------------------------------------
@app.command(
    "restore",
    epilog=Common.GetDestinationHelp(),
    no_args_is_help=True,
)
def Restore(  # pylint: disable=too-many-arguments
    backup_name: str=_backup_name_argument,
    backup_source: str=typer.Argument(..., help="Data store location containing content that has been backed up."),
    working_dir: Optional[Path]=typer.Option(None, "--working-dir", file_okay=False, resolve_path=True, help="Working directory to use when decompressing archives; provide this value during a dry run and subsequent execution to only download and extract the backup content once."),
    encryption_password: Optional[str]=typer.Option(None, "--encryption-password", help="Password used when creating the backups."),
    dir_substitution_key_value_args: Optional[List[str]]=TyperEx.TyperDictOption(None, {}, "--dir-substitution", allow_any__=True, help="A key-value-pair consisting of a string to replace and its replacement value within a posix string; this can be used when restoring to a location that is different from the location used to create the backup. Example: '--dir-substitution \"C\\:/=C\\:/Restore/\" will cause files backed-up as \"C:/Foo/Bar.txt\" to be restored as \"C:/Restore/Foo/Bar.txt\". This value can be provided multiple times on the command line when supporting multiple substitutions."),
    dry_run: bool=typer.Option(False, "--dry-run", help="Show the changes that would be made during the restoration process, but do not modify the local file system."),
    overwrite: bool=typer.Option(False, "--overwrite", help="By default, the restoration process will not overwrite existing files on the local file system; this flag indicates that files should be overwritten as they are restored."),
    ssd: bool=command_line.ssd_option,
    verbose: bool=command_line.verbose_option,
    quiet: bool=command_line.quiet_option,
    debug: bool=command_line.debug_option,
) -> None:
    """Restores content from an offsite backup."""

    dir_substitutions = TyperEx.PostprocessDictArgument(dir_substitution_key_value_args)

    with DoneManager.CreateCommandLine(
        output_flags=DoneManagerFlags.Create(verbose=verbose, debug=debug),
    ) as dm:
        dm.WriteVerbose(str(datetime.datetime.now()) + "\n\n")

        with _ResolveWorkingDir(dm, working_dir) as resolved_working_dir:
            Offsite.Restore(
                dm,
                backup_name,
                backup_source,
                encryption_password,
                resolved_working_dir,
                dir_substitutions,
                ssd=ssd,
                quiet=quiet,
                dry_run=dry_run,
                overwrite=overwrite,
            )


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
@contextmanager
def _ResolveWorkingDir(
    dm: DoneManager,
    working_dir: Optional[Path],
    *,
    always_preserve: bool=False,
) -> Iterator[Path]:
    if working_dir is None:
        delete_dir = not always_preserve
        working_dir = CurrentShell.CreateTempDirectory()

    else:
        delete_dir = False

    was_successful = True

    try:
        assert working_dir is not None
        yield working_dir

    except:
        was_successful = False
        raise

    finally:
        assert working_dir is not None

        if delete_dir:
            was_successful = was_successful and dm.result == 0

            if was_successful:
                PathEx.RemoveTree(working_dir)
            else:
                if dm.result <= 0:
                    # dm.result will be 0 if an exception was encountered
                    type_desc = "errors"
                elif dm.result > 0:
                    type_desc = "warnings"
                else:
                    assert False, dm.result  # pragma: no cover

                dm.WriteInfo(
                    "The temporary directory '{}' was preserved due to {}.".format(
                        working_dir,
                        type_desc,
                    ),
                )


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
if __name__ == "__main__":
    app()
