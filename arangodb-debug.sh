#!/bin/sh

if test -z "$ARANGOSH"; then
    arangosh=`which arangosh`
else
    arangosh="$ARANGOSH"
fi

if test -z "$arangosh"; then
    echo "$0: cannot find arangosh"
    exit 1
fi

if test ! -x "$arangosh"; then
    echo "$0: file '$arangosh' does not exist or not executable"
    exit 1
fi

if test "$1" = "help"; then
    $arangosh --javascript.execute ./debugging/index.js --server.endpoint none "$@"
else
    $arangosh --javascript.execute ./debugging/index.js "$@"
fi
