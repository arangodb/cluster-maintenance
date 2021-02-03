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

arangoArgs=""
scriptArgs=""
noEndpoint=0
seenDashDash=0

while test $# -gt 0; do
    case $1 in
        help)
	    scriptArgs="$scriptArgs $1"
	    noEndpoint=1
	    ;;

	--force|--ignore*)
	    scriptArgs="$scriptArgs $1"
	    ;;

	--)
	    arangoArgs="$arangoArgs $1"
	    seenDashDash=1
	    ;;

	*)
	    arangoArgs="$arangoArgs $1"
	    ;;
    esac
    shift
done

if test "$noEndpoint" -eq 1; then
    $arangosh --javascript.execute ./lib/index.js $arangoArgs --server.endpoint none -- $scriptArgs
elif test "$seenDashDash" -eq 1; then
    $arangosh --javascript.execute ./lib/index.js $arangoArgs $scriptArgs
else
    $arangosh --javascript.execute ./lib/index.js $arangoArgs -- $scriptArgs
fi
