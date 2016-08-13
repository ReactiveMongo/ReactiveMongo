#! /bin/bash

set -e

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`

cd "$SCRIPT_DIR/.."

if [ "$CI_CATEGORY" = "UNIT_TESTS" ]; then
    source $SCRIPT_DIR/unitTests.sh
else
    source $SCRIPT_DIR/integrationTests.sh
fi
