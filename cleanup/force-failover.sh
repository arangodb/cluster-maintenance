#!/usr/bin/env bash
set -u

# usage: force-failover.sh AGENCY_ENDPOINT ZOMBIE_FILE

# The script requires an installed version of arangodb or at least a way to call
# the arangosh executable from anywhere. We try to do path resolving on a
# best effort basis for some systems.
#
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

if [[ "$do_resolve" == "false" ]]; then
    echo "detected os-type $machine -- resolve enabled: $do_resolve"
fi

# the script takes a single argument
args=""
server_endpoint="none"

while [[ "$#" -gt 4 ]]; do
    case "$1" in
        --server.endpoint)
            server_endpoint=$2
            shift
            shift
            ;;
        *)
            args="$args $1"
            shift
            ;;
    esac
done

if [[ "$#" -gt 0 ]]; then
    case "$1" in
        http*|ssl*|tcp*)
            server_endpoint="$1"
            shift
            ;;
    esac
fi

if [[ -z "$*" ]]; then
    echo "usage: $0 --server.endpoint LEADER-AGENT INPUT-FILE TARGET-SERVER LEADER-CID SHARD-INDEX"
    exit 1
fi

if $do_resolve; then
    file_name="$(realpath "$(readlink -s -f "$1")")"
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
    script_dir="$(realpath "$(dirname "$(readlink -s -f "${BASH_SOURCE[0]}")")")"
    cd "$script_dir" || { echo "failed to change into binary dir $script_dir"; exit 1; }
    echo "$script_dir"
fi

arangosh \
    --server.endpoint $server_endpoint \
    --javascript.execute "lib/$(basename $0 .sh).js" \
    $args \
    -- "$file_name" "$2" "$3" "$4"
