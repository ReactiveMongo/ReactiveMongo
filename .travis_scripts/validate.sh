#! /usr/bin/env sh

set -e

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`

cd "SCRIPT_DIR/.."

sbt ++$TRAVIS_SCALA_VERSION scalariformFormat test:scalariformFormat
git diff --exit-code || (
  echo "ERROR: Scalariform check failed, see differences above."
  echo "To fix, format your sources using ./build scalariformFormat test:scalariformFormat before submitting a pull request."
  echo "Additionally, please squash your commits (eg, use git commit --amend) if you're going to update this pull request."
  false
)

MONGODB_VER="2_6"

# Print version information
MV=`mongod --version | head -n 1`

echo -n "MongoDB ($MV): "
echo "$MV" | sed -e 's/.* v//'

if [ `echo "$MV" | grep v3 | wc -l` -eq 1 ]; then
    MONGODB_VER="3"
    SHELL_OPTS=""

    if [ "$MONGO_SSL" = "true" ]; then
        SHELL_OPTS="--ssl --sslAllowInvalidCertificates"
    fi

    SHELL_OPTS="$SHELL_OPTS --eval"

    echo -n "- security: "
    mongo $SHELL_OPTS 'var s=db.serverStatus();var x=s["security"];(!x)?"_DISABLED_":x["SSLServerSubjectName"];'
    
    echo -n "- storage engine: "
    mongo $SHELL_OPTS 'var s=db.serverStatus();JSON.stringify(s["storageEngine"]);' | grep '"name"' | cut -d '"' -f 4
fi

TEST_OPTS="exclude not_mongo26"
SBT_OPTS="++$TRAVIS_SCALA_VERSION"

if [ "$MONGODB_VER" = "3" ]; then
    TEST_OPTS="exclude mongo2"
    
    if [ "$MONGO_SSL" = "true" ]; then
        SBT_OPTS="$SBT_OPTS -Dtest.enableSSL=true"
    fi
fi

sbt $SBT_OPTS "testOnly -- $TEST_OPTS"
