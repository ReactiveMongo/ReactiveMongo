#!/usr/bin/env bash
  
set -e

DIR=`dirname $0 | sed -e "s|^./|$PWD/|"`

cd "$DIR/.."

SBT_CMDS="+clean +makePom +packageBin +packageSrc +packageDoc"

sbt $SBT_OPTS $SBT_CMDS

export REACTIVEMONGO_SHADED=false
sbt $SBT_OPTS $SBT_CMDS

tar -xvf "$TMP"
rm -f "$TMP"
