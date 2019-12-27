#!/usr/bin/env bash
  
set -e

SBT_CMDS="+makePom +packageBin +packageSrc +packageDoc"

sbt $SBT_OPTS +clean $SBT_CMDS

REACTIVEMONGO_SHADED=false
sbt $SBT_OPTS $SBT_CMDS
