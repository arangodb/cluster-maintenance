#!/bin/sh
if command -v realpath > /dev/null; then
  true
else
  realpath() {
    OURPWD=$PWD
    cd "$(dirname "$1")"
    LINK=$(readlink "$(basename "$1")")
    while [ "$LINK" ]; do
      cd "$(dirname "$LINK")"
      LINK=$(readlink "$(basename "$1")")
    done
    REALPATH="$PWD/$(basename "$1")"
    cd "$OURPWD"
    echo "$REALPATH"
  }
fi

if command -v /usr/bin/echo > /dev/null; then
    ECHO=/usr/bin/echo
else if command -v /bin/echo > /dev/null; then
    ECHO=/bin/echo
else
    ECHO=echo
fi
fi

if test -z "$ARANGOSH"; then
    arangosh=`which arangosh`
else
    arangosh="$ARANGOSH"
fi

if test -z "$arangosh"; then
    $ECHO -n "$0: cannot find arangosh."
    if test -z "$ARANGOSH"; then
        $ECHO -n " it is possible to set the environment variable ARANGOSH to the location of the arangosh executable, if it is not in the path."
    fi
    $ECHO
    exit 1
fi
    
if test -d "$arangosh"; then
    $ECHO "$0: '$arangosh' is a directory. it should point to the arangosh executable instead."
    exit 1
fi

if test ! -x "$arangosh"; then
    $ECHO "$0: file '$arangosh' does not exist or is not executable."
    exit 1
fi

nargs=$#
scriptArgs=""
noEndpoint=0
dashDash="--"

for arg do
    if test "$noEndpoint" -eq 1; then
      # if we have seen help, we need to move all following arguments
      # to the right of the "--", too.
      scriptArgs="$scriptArgs $arg"
      continue
    fi

    case $arg in
      help)
        scriptArgs="$scriptArgs $arg"
        noEndpoint=1
        ;;

     --force|--ignore*)
        scriptArgs="$scriptArgs $arg"
        ;;

     --)
        set -- "$@" "$arg"
        dashDash=""
        ;;

     *)
        set -- "$@" "$arg"
        ;;
    esac
done

shift "$nargs"

myPath="`realpath $0`"
myPath="`dirname $myPath`"
if [ "x$myPath" = "x" ]; then
    myPath="."
fi

if test "$noEndpoint" -eq 1; then
    $arangosh --javascript.execute "$myPath"/lib/index.js --server.endpoint none "$@" $dashDash $scriptArgs
else
    $arangosh --javascript.execute "$myPath"/lib/index.js "$@" $dashDash $scriptArgs
fi
