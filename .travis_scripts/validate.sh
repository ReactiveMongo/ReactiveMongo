#! /usr/bin/env sh

set -e

DIR=`dirname $0`

rm -rf $HOME/.sbt
rm -rf $HOME/.ivy2

cd "$DIR/.."

sbt ++$TRAVIS_SCALA_VERSION scalariformFormat test:scalariformFormat
git diff --exit-code || (
  echo "ERROR: Scalariform check failed, see differences above."
  echo "To fix, format your sources using ./build scalariformFormat test:scalariformFormat before submitting a pull request."
  echo "Additionally, please squash your commits (eg, use git commit --amend) if you're going to update this pull request."
  false
)

MONGODB_VER="2_6"

# Print version information
echo -n "MongoDB: "
mongod --version | head -n 1 | sed -e 's/.* v//'

if [ `mongod --version | head -n 1 | grep v3 | wc -l` -eq 1 ]; then
    MONGODB_VER="3"

    echo -n "- storage engine: "
    mongo --eval 'var s=db.serverStatus();JSON.stringify(s["storageEngine"]);' | grep '"name"' | cut -d '"' -f 4
fi

TEST_OPTS="exclude mongo3"

if [ "$MONGODB_VER" = "3" ]; then
    TEST_OPTS="exclude mongo2"
fi

sbt ++$TRAVIS_SCALA_VERSION "testOnly -- $TEST_OPTS"
