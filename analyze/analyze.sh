#!/usr/bin/env bash
set -u

# The sript requires an installed version of arangodb or at least a way to call
# the arangosh executable from anywhere. We try to do path resolving on best a
# best effort basis for some systems.
# If it does not work for you change the `do_resolve` for your system to false.
# In systems that do not have do_resolve enabled you need to be in the directory
# in which this script is contained to make it work.

do_resolve=false;
uname_output="$(uname -s)"
case "${uname_output}" in
    Linux*)     machine=Linux; do_resolve=true;;
    Darwin*)    machine=Mac;;
    CYGWIN*)    machine=Cygwin;;
    MINGW*)     machine=MinGw;;
    *)          machine="UNKNOWN:${uname_output}"
esac


# the script takes a single argument
if [[ -z "$*" ]]; then
    echo "no file provided"
    exit 1
fi

if $do_resolve; then
    file_name="$(realpath -s "$(readlink -s -f "$1")")"
    if [[ -z "$file_name" ]]; then
        echo "file resolution failed"
        exit 1
    fi
    echo "full path of json: $file_name"
else
    file_name="$1"
fi


# Try to enter the dir that contains this file as we require to be relative to
# the lib directory that is also contained in the same directory.
if $do_resolve; then
    script_dir="$(realpath -s "$(dirname "$(readlink -s -f "${BASH_SOURCE[0]}")")")"
    cd "$script_dir"
    echo "$script_dir"
fi

arangosh \
    --server.endpoint none \
    --javascript.execute lib/analyze.js \
    -- "$file_name"
