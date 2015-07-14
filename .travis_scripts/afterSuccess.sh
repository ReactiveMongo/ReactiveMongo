#! /bin/sh

SCRIPT_DIR=`dirname $0`
JAVA_COMPAT=`javac -version 2>&1 | grep 1.6 | wc -l`

# TRAVIS_BRANCH
#  -o "$TRAVIS_BRANCH" != "master"
if [ "$MONGO_SSL" = "false" -o "$SONATYPE_USER" = "" -o "$SONATYPE_PASS" = "" -o $JAVA_COMPAT -ne 1 ]; then
    echo "skip the snapshot publication: $JAVA_COMPAT"
else
    cd "$SCRIPT_DIR/.."

    export PUBLISH_REPO_NAME="Sonatype Nexus Repository Manager"
    export PUBLISH_REPO_ID="oss.sonatype.org"
    export PUBLISH_REPO_URL=https://oss.sonatype.org/content/repositories/snapshots/
    export PUBLISH_USER="$SONATYPE_USER"
    export PUBLISH_PASS="$SONATYPE_PASS"

    sbt '+publish'

    cd -
fi

if [ "$MONGO_SSL" = "true" -a "$TRAVIS_SCALA_VERSION" = "2.11.6" -a `echo "$JAVA_HOME" | grep java-7-oracle | wc -l` -eq 1 ]; then
    ./.travis_scripts/dependents.sh
fi
