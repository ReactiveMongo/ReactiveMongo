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
    SHELL_OPTS="$PRIMARY_HOST"

    if [ "$MONGO_SSL" = "true" ]; then
        SHELL_OPTS="$SHELL_OPTS --ssl --sslAllowInvalidCertificates"
    fi

    SHELL_OPTS="$SHELL_OPTS --eval"

    echo -n "- security: "
    mongo $SHELL_OPTS 'var s=db.serverStatus();var x=s["security"];(!x)?"_DISABLED_":x["SSLServerSubjectName"];' 2>/dev/null | tail -n 1
    
    echo -n "- storage engine: "
    mongo $SHELL_OPTS 'var s=db.serverStatus();JSON.stringify(s["storageEngine"]);' 2>/dev/null | grep '"name"' | cut -d '"' -f 4
fi

TEST_OPTS="exclude not_mongo26"
SBT_OPTS="++$TRAVIS_SCALA_VERSION -Dtest.primaryHost=$PRIMARY_HOST"

echo "Backend: $PRIMARY_BACKEND"

if [ ! "$PRIMARY_BACKEND" = "no" ]; then
    SBT_OPTS="$SBT_OPTS -Dtest.primaryBackend=$PRIMARY_BACKEND -Dtest.proxyDelay=300"
fi

if [ "$MONGODB_VER" = "3" ]; then
    TEST_OPTS="exclude mongo2"
    
    if [ "$MONGO_SSL" = "true" ]; then
        SBT_OPTS="$SBT_OPTS -Dtest.enableSSL=true"
    fi
fi

if [ `echo "$JAVA_HOME" | grep java-7-oracle | wc -l` -eq 1 -a "$MONGO_SSL" = "false" ]; then
  # Network latency
  SBT_OPTS="$SBT_OPTS -Dtest.failoverRetries=12"
fi

echo "- SBT options: $SBT_OPTS"

#sbt ++$TRAVIS_SCALA_VERSION $SBT_OPTS ";error ;mimaReportBinaryIssues ;test:compile" || exit 2
sbt ++$TRAVIS_SCALA_VERSION $SBT_OPTS ";error ;test:compile" || exit 2
sbt ++$TRAVIS_SCALA_VERSION $SBT_OPTS "testOnly -- $TEST_OPTS"
