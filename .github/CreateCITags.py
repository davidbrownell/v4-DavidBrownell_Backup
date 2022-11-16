"""Creates CI tags based on the current commit and pushes them to GitHub."""

import os
import sys

from pathlib import Path

import typer

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation import PathEx
from Common_Foundation import Types


# ----------------------------------------------------------------------
foundation_root_dir = PathEx.EnsureDir(Path(Types.EnsureValid(os.getenv("DEVELOPMENT_ENVIRONMENT_FOUNDATION"))))

github_src_root = PathEx.EnsureDir(foundation_root_dir / ".github" / "src")

sys.path.insert(0, str(github_src_root))
with ExitStack(lambda: sys.path.pop(0)):
    from BuildImpl import CreateTagsImpl  # type: ignore  # pylint: disable=import-error


# ----------------------------------------------------------------------
app                                         = typer.Typer(
    help=__doc__,
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
    pretty_exceptions_enable=False,
)


# ----------------------------------------------------------------------
@app.command("Execute", no_args_is_help=False, help=__doc__)
def Execute(
    dry_run: bool=typer.Option(False, "--dry-run", help="Prints the command line used to create the tags and push them to GitHub, but does not invoke the functionality."),
    yes: bool=typer.Option(False, "--yes", help="Answer yes to the 'are you sure' prompt."),
    verbose: bool=typer.Option(False, "--verbose", help="Write verbose information to the terminal."),
    debug: bool=typer.Option(False, "--debug", help="Write debug information to the terminal."),
) -> None:
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
    app()
