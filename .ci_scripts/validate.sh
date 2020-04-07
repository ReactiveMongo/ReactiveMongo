#! /usr/bin/env bash

set -e

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`

cd "$SCRIPT_DIR/.."

cat > /dev/stdout <<EOF

[INFO] CI validation:
- CI category: $CI_CATEGORY
- OS name: $OS_NAME

EOF

if [ "$CI_CATEGORY" = "UNIT_TESTS" ]; then
    source $SCRIPT_DIR/unitTests.sh
else
    source $SCRIPT_DIR/integrationTests.sh
fi
