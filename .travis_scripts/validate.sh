#! /bin/bash

set -e

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`

source /tmp/validate-env.sh

cd "$SCRIPT_DIR/.."

sbt ++$TRAVIS_SCALA_VERSION scalariformFormat test:scalariformFormat > /dev/null
git diff --exit-code || (
  echo "ERROR: Scalariform check failed, see differences above."
  echo "To fix, format your sources using sbt scalariformFormat test:scalariformFormat before submitting a pull request."
  echo "Additionally, please squash your commits (eg, use git commit --amend) if you're going to update this pull request."
  false
)

MONGODB_VER="2_6"

export LD_LIBRARY_PATH

# Print version information
MV=`mongod --version 2>/dev/null | head -n 1`

echo -n "MongoDB ($MV): "
echo "$MV" | sed -e 's/.* v//'

if [ `echo "$MV" | grep v3 | wc -l` -eq 1 ]; then
    MONGODB_VER="3"
fi

TEST_OPTS="exclude not_mongo26"
SBT_ARGS="-Dtest.primaryHost=$PRIMARY_HOST"
SBT_ARGS="$SBT_ARGS -Dtest.slowPrimaryHost=$PRIMARY_SLOW_PROXY -Dtest.slowProxyDelay=300 -Dtest.slowFailoverRetries=12"

if [ "$MONGODB_VER" = "3" ]; then
    TEST_OPTS="exclude mongo2"
    
    if [ "$MONGO_SSL" = "true" ]; then
        SBT_ARGS="$SBT_ARGS -Dtest.enableSSL=true"
    fi
fi

JVM_OPTS="-Xms2048M -Xmx2048M -Xss64M -XX:+CMSClassUnloadingEnabled -XX:ReservedCodeCacheSize=192m"

if [ `echo "$JAVA_HOME" | grep java-7-oracle | wc -l` -eq 1 ]; then
    JVM_OPTS="$JVM_OPTS -XX:PermSize=512M -XX:MaxPermSize=512M"
else
    JVM_OPTS="$JVM_OPTS -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=512M"
fi

cat > /dev/stdout <<EOF
- JVM options: $JVM_OPTS
- SBT options: $SBT_ARGS
- Test options: $TEST_OPTS
EOF

export JVM_OPTS

sbt ++$TRAVIS_SCALA_VERSION ";error ;mimaReportBinaryIssues ;test:compile" || exit 2
sbt ++$TRAVIS_SCALA_VERSION $SBT_ARGS ";testOnly -- $TEST_OPTS"
SBT_ST="$?"

if [ ! $SBT_ST -eq 0 ]; then
    tail -n 100 /tmp/mongod.log
fi

exit $SBT_ST
