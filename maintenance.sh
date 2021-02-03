#!/bin/sh

if test -z "$ARANGOSH"; then
    arangosh=`which arangosh`
else
    arangosh="$ARANGOSH"
fi

if test -z "$arangosh"; then
    echo -n "$0: cannot find arangosh."
    if test -z "$ARANGOSH"; then
        echo -n " it is possible to set the environment variable ARANGOSH to the location of the arangosh executable, if it is not in the path."
    fi
    echo
    exit 1
fi
    
if test -d "$arangosh"; then
    echo "$0: '$arangosh' is a directory. it should point to the arangosh executable instead."
    exit 1
fi

if test ! -x "$arangosh"; then
    echo "$0: file '$arangosh' does not exist or is not executable."
    exit 1
fi

nargs=$#
scriptArgs=""
noEndpoint=0
dashDash="--"

for arg do
    case $arg in
        help)
	    scriptArgs="$scriptArgs $arg"
	    noEndpoint=1
	    ;;

	--force|--ignore*)
	    scriptArgs="$scriptArgs $arg"
	    ;;

	--)
	    set "$@" "$arg"
	    dashDash=""
	    ;;

	*)
	    set -- "$@" "$arg"
	    ;;
    esac
done

shift "$nargs"

if test "$noEndpoint" -eq 1; then
    $arangosh --javascript.execute ./lib/index.js --server.endpoint none "$@" $dashDash $scriptArgs
else
    $arangosh --javascript.execute ./lib/index.js "$@" $dashDash $scriptArgs
fi
