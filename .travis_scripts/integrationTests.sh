#! /bin/bash

source /tmp/integration-env.sh

export LD_LIBRARY_PATH

# Print version information
MV=`mongod --version 2>/dev/null | head -n 1`

echo -n "MongoDB ($MV): "
echo "$MV" | sed -e 's/.* v//'

# JVM/SBT setup
TEST_OPTS="exclude not_mongo26"
SBT_ARGS="-Dtest.primaryHost=$PRIMARY_HOST"
SBT_ARGS="$SBT_ARGS -Dtest.slowPrimaryHost=$PRIMARY_SLOW_PROXY -Dtest.slowProxyDelay=300 -Dtest.slowFailoverRetries=12"

if [ "$MONGO_VER" = "3" ]; then
    TEST_OPTS="exclude mongo2"
    
    if [ "$MONGO_SSL" = "true" ]; then
        SBT_ARGS="$SBT_ARGS -Dtest.enableSSL=true"
    fi
fi

source "$SCRIPT_DIR/jvmopts.sh"

cat > /dev/stdout <<EOF
- JVM options: $JVM_OPTS
- SBT options: $SBT_ARGS
- Test options: $TEST_OPTS
EOF

export JVM_OPTS

TEST_ARGS=";project ReactiveMongo ;testOnly -- $TEST_OPTS"
TEST_ARGS="$TEST_ARGS ;project ReactiveMongo-Iteratees ;testOnly -- $TEST_OPTS"
TEST_ARGS="$TEST_ARGS ;project ReactiveMongo-JMX ;testOnly -- $TEST_OPTS"

sbt ++$TRAVIS_SCALA_VERSION $SBT_ARGS "$TEST_ARGS" || (
    PID=`ps -ao pid,comm | grep 'mongod$' | cut -d ' ' -f 1`
    
    if [ ! "x$PID" = "x" ]; then
        pid -p $PID
    fi
    
    tail -n 10000 /tmp/mongod.log | grep -v ' end connection ' | grep -v 'connection accepted' | grep -v 'killcursors: found 0 of 1' | tail -n 100

    false
)
