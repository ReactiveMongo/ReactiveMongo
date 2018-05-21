#! /bin/sh

SCRIPT_DIR=`dirname $0`
JAVA_COMPAT=`javac -version 2>&1 | grep 1.7 | wc -l`

if [ ! "$CATEGORY" = "UNIT_TESTS" ]; then
  killall -9 mongod
fi

rm -rf "$HOME/.ivy2/cache/org.reactivemongo/"

if [ ! "x$TRAVIS_TAG" = "x" -o "$MONGO_SSL" = "false" -o "x$SONATYPE_USER" = "x" -o "x$SONATYPE_PASS" = "x" -o $JAVA_COMPAT -ne 1 ]; then
    echo -n "\nINFO: Skip the snapshot publication: $JAVA_COMPAT\n"
    exit 0
fi

cd "$SCRIPT_DIR/.."

export PUBLISH_REPO_NAME="Sonatype Nexus Repository Manager"
export PUBLISH_REPO_URL="https://oss.sonatype.org/content/repositories/snapshots"
export PUBLISH_REPO_ID="oss.sonatype.org"
export PUBLISH_USER="$SONATYPE_USER"
export PUBLISH_PASS="$SONATYPE_PASS"

sbt ';+publish ;project ReactiveMongo-JMX ;+publish'
