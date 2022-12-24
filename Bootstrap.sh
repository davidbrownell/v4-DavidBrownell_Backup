#!/bin/bash
# ----------------------------------------------------------------------
# |
# |  Enlists and setups a repository and its dependencies.
# |
# |  Run as:
# |      Bootstrap.cmd <common code dir> [--name <unique_environment_name>] [Optional Setup.cmd args]*
# |
# |      Where:
# |          <common code dir>                : Name of the directory in which common dependencies are enlisted.
# |                                             This location can be reused across multiple projects and
# |                                             enlistments.
# |
# |          --name <unique_environment_name> : Setup an environment with a unique name. This allows for the
# |                                             creation of side-by-side environments that are otherwise identical.
# |                                             It is very rare to setup an environment with a unique name.
# |
# |          [Optional Setup.cmd args]        : Any additional args passed to Setup.cmd for the respository
# |                                             and its dependencies. See Setup.cmd for more information on
# |                                             the possible arguments and their use.
# |
# ----------------------------------------------------------------------
set -e                                      # Exit on error
set +v                                      # Disable output

should_continue=1

if [[ ${should_continue} == 1 && "$1" == "" ]]; then
    echo ""
    echo "[31m[1mERROR:[0m This script bootstraps enlistment and setup activities for a repository and its dependencies."
    echo "[31m[1mERROR:[0m"
    echo "[31m[1mERROR:[0m Usage:"
    echo "[31m[1mERROR:[0m     $0 <common code dir> [--name <custom Setup.cmd environment name>] [Optional Setup.cmd args]*"
    echo "[31m[1mERROR:[0m"
    echo ""

    should_continue=0
fi

if [[ ${should_continue} == 1 && ${DEVELOPMENT_ENVIRONMENT_REPOSITORY_ACTIVATED_KEY} ]]; then
    echo ""
    echo "[31m[1mERROR:[0m ERROR: Please run this script for a standard (non-activated) command prompt."
    echo "[31m[1mERROR:[0m"
    echo ""

    should_continue=0
fi

if [[ ${should_continue} == 1 ]]; then
    # Parse the args
    name=""
    next_is_name=0

    no_hooks_arg=""
    force_arg=""
    verbose_arg=""
    debug_arg=""

    ARGS=()

    for var in "${@:2}"; do
        if [[ $next_is_name == 1 ]]; then
            name=$var
            next_is_name=0
        elif [[ $var == --name ]]; then
            next_is_name=1
        else
            ARGS+=("$var")
        fi

        if [[ $var == --no-hooks ]]; then
            no_hooks_arg=$var
        elif [[ $var == --force ]]; then
            force_arg=$var
        elif [[ $var == --verbose ]]; then
            verbose_arg=$var
        elif [[ $var == --debug ]]; then
            debug_arg=$var
        fi
    done

    if [[ ! -z "${name}" ]]; then
        name_arg="--name ${name}"
    else
        name_arg=""
    fi
fi

if [[ ${should_continue} == 1 ]]; then
    # Enlist and setup Common_Foundation
    if [[ ! -e "$1/Common/Foundation" ]]; then
        echo "Enlisting in Common_Foundation..."
        echo ""

        git clone https://github.com/davidbrownell/v4-Common_Foundation.git "$1/Common/Foundation.tmp"

        pushd "$1/Common/Foundation.tmp" > /dev/null
        git checkout tags/main_stable
        popd > /dev/null

        mv "$1/Common/Foundation.tmp" "$1/Common/Foundation"

        echo ""
        echo "DONE!"
        echo ""

    else
        echo "Updating Common_Foundation..."
        echo ""

        pushd "$1/Common/Foundation" > /dev/null
        git fetch origin main_stable
        popd > /dev/null

        echo ""
        echo "DONE!"
        echo ""
    fi

    "$1/Common/Foundation/Setup.sh" ${name_arg} ${no_hooks_arg} ${force_arg} ${verbose_arg} ${debug_arg}

    # Write the environment activation and python execution statements to a temporary file
    # so that this environment remains unactivated. By doing this, the current script can be
    # invoked repeatedly from the same environment.
    if [[ ! -z ${name} ]]; then
        activate_cmd="Activate.${name}.sh"
    else
        activate_cmd="Activate.sh"
    fi

    this_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

    cat >../bootstrap_tmp.sh << EOF
#!/bin/bash
set -e

source "$1/Common/Foundation/${activate_cmd}" python310 ${force_arg} ${verbose_arg} ${debug_arg}
Enlist.sh EnlistAndSetup "${this_dir}" "$1" ${no_hooks_arg} ${force_arg} ${verbose_arg} ${debug_arg} ${ARGS[@]}
EOF

    chmod +x ../bootstrap_tmp.sh
    ../bootstrap_tmp.sh
    error_code=${error_code}

    rm ../bootstrap_tmp.sh

    if [[ ${error_code} -ne 0 ]]; then
        should_continue=0
    fi

    chown -R ${SUDO_UID}:${SUDO_GID} "$1"
fi
