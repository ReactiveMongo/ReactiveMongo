#! /usr/bin/env sh

set -e

DIR=`dirname $0`

cd "$DIR/.."

sbt ++$TRAVIS_SCALA_VERSION scalariformFormat test:scalariformFormat
git diff --exit-code || (
  echo "ERROR: Scalariform check failed, see differences above."
  echo "To fix, format your sources using ./build scalariformFormat test:scalariformFormat before submitting a pull request."
  echo "Additionally, please squash your commits (eg, use git commit --amend) if you're going to update this pull request."
  false
)

sbt ++$TRAVIS_SCALA_VERSION "testOnly -- exclude mongo3"
