#!/bin/bash
# ----------------------------------------------------------------------
# |
# |  Run as:
# |      Setup.cmd [--configuration <config_name>] [--verbose] [--debug] [--name <unique_environment_name>]
# |
# |      Where:
# |          --configuration <config_name>    : Name of the configuration to setup (this value can appear
# |                                             multiple times on the command line). All available
# |                                             configurations are setup if none are explicitly provided.
# |
# |          --force                          : Force setup.
# |          --verbose                        : Verbose output.
# |          --debug                          : Includes debug output (in adddition to verbose output).
# |
# |          --name <unique_environment_name> : Setup an environment with a unique name. This allows for the
# |                                             creation of side-by-side environments that are otherwise identical.
# |                                             It is very rare to setup an environment with a unique name.
# |
# |          --interactive/--no-interactive   : Set the default value for `is_interactive` for those repositories that
# |                                             provide those capabilities during setup.
# |
# |          --search-depth <value>           : Limit searches for other repositories to N levels deep. This value
# |                                             can help to decrease the overall search times when a dependency
# |                                             repository is not on the system. Coversely, this value can be set
# |                                             to a higher value to not artifically limit searches when a dependency
# |                                             repsitory is on the system but not found using default values.
# |          --max-num-searches <value>       : Limits the maximum number of searches performed when looking for
# |                                             dependency repositories.
# |          --required-ancestor-dir <value>  : Restrict searches to this directory when searching for dependency
# |                                             repositories (this value can appear multiple times on the command
# |                                             line).
# |
# |          --no-hooks                       : Do not install Source Control Management (SCM) hooks for this repository
# |                                             (pre-commit, post-commit, etc.).
# |
# ----------------------------------------------------------------------

set -e                                      # Exit on error
set +v                                      # Disable output

if [[ "${DEVELOPMENT_ENVIRONMENT_FOUNDATION}" == "" ]]; then
    echo ""
    echo "[31m[1mERROR:[0m Please run this script within an activated environment."
    echo "[31m[1mERROR:[0m"
    echo ""

    exit -1
fi

pushd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )" > /dev/null
source $DEVELOPMENT_ENVIRONMENT_FOUNDATION/RepositoryBootstrap/Impl/Setup.sh "$@"
popd > /dev/null
