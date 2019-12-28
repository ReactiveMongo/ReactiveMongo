#!/usr/bin/env bash
  
set -e

unset REACTIVEMONGO_SHADED

SBT_CMDS="+clean +makePom +packageBin +packageSrc +packageDoc"

sbt $SBT_OPTS $SBT_CMDS

export REACTIVEMONGO_SHADED=false
sbt $SBT_OPTS $SBT_CMDS
