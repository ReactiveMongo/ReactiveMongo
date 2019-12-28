#!/usr/bin/env bash
  
set -e

DIR=`dirname $0 | sed -e "s|^./|$PWD/|"`

cd "$DIR/.."

SBT_CMDS="+clean +makePom +packageBin +packageSrc +packageDoc"

sbt $SBT_OPTS $SBT_CMDS
TMP=`mktemp`
tar -cvf "$TMP" `find . -path '*/target/*' -type f \( -name '*.jar' -or -name '*.pom' \) -print`

export REACTIVEMONGO_SHADED=false
sbt $SBT_OPTS $SBT_CMDS

tar -xvf "$TMP"
rm -f "$TMP"
