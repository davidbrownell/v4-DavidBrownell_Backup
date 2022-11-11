@echo off

@REM ----------------------------------------------------------------------
@REM |
@REM |  Run as:
@REM |      Setup.cmd [--configuration <config_name>] [--verbose] [--debug] [--name <unique_environment_name>]
@REM |
@REM |      Where:
@REM |          --configuration <config_name>    : Name of the configuration to setup (this value can appear
@REM |                                             multiple times on the command line). All available
@REM |                                             configurations are setup if none are explicitly provided.
@REM |
@REM |          --force                          : Force setup.
@REM |          --verbose                        : Verbose output.
@REM |          --debug                          : Includes debug output (in adddition to verbose output).
@REM |
@REM |          --name <unique_environment_name> : Setup an environment with a unique name. This allows for the
@REM |                                             creation of side-by-side environments that are otherwise identical.
@REM |                                             It is very rare to setup an environment with a unique name.
@REM |
@REM |          --interactive/--no-interactive   : Set the default value for `is_interactive` for those repositories that
@REM |                                             provide those capabilities during setup.
@REM |
@REM |          --search-depth <value>           : Limit searches for other repositories to N levels deep. This value
@REM |                                             can help to decrease the overall search times when a dependency
@REM |                                             repository is not on the system. Coversely, this value can be set
@REM |                                             to a higher value to not artifically limit searches when a dependency
@REM |                                             repsitory is on the system but not found using default values.
@REM |          --max-num-searches <value>       : Limits the maximum number of searches performed when looking for
@REM |                                             dependency repositories.
@REM |          --required-ancestor-dir <value>  : Restrict searches to this directory when searching for dependency
@REM |                                             repositories (this value can appear multiple times on the command
@REM |                                             line).
@REM |
@REM |          --no-hooks                       : Do not install Source Control Management (SCM) hooks for this repository
@REM |                                             (pre-commit, post-commit, etc.).
@REM |
@REM ----------------------------------------------------------------------

if "%DEVELOPMENT_ENVIRONMENT_FOUNDATION%"=="" (
    @echo.
    @echo [31m[1mERROR:[0m Please run this script within an activated environment.
    @echo [31m[1mERROR:[0m
    @echo.

    goto end
)

pushd "%~dp0"
call "%DEVELOPMENT_ENVIRONMENT_FOUNDATION%\RepositoryBootstrap\Impl\Setup.cmd" %*
set _DEVELOPMENT_ENVIRONMENT_SETUP_ERROR=%ERRORLEVEL%
popd

if %_DEVELOPMENT_ENVIRONMENT_SETUP_ERROR% NEQ 0 (exit /B %_DEVELOPMENT_ENVIRONMENT_SETUP_ERROR%)

:end
