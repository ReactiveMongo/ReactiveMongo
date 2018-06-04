#! /usr/bin/env bash

# Requires: $PRIMARY_HOST, $PRIMARY_SLOW_PROXY, $MONGO_VER, $MONGO_PROFILE, $SCALA_VERSION

set -e

# JVM/SBT setup
SBT_OPTS="-Dsbt.scala.version=2.10.7 -Dtest.primaryHost=$PRIMARY_HOST"
SBT_OPTS="$SBT_OPTS -Dtest.slowPrimaryHost=$PRIMARY_SLOW_PROXY -Dtest.slowProxyDelay=300 -Dtest.slowFailoverRetries=12"
TEST_OPTS=""

echo "- MongoDB major: $MONGO_VER"

if [ "$MONGO_VER" = "3" ]; then
    TEST_OPTS="exclude mongo2,gt_mongo32,unit"
elif [ "$MONGO_VER" = "3_4" ]; then
    TEST_OPTS="exclude mongo2,unit"
else
    TEST_OPTS="exclude not_mongo26,gt_mongo32,unit"
    SBT_OPTS="$SBT_OPTS -Dtest.authMode=cr"
fi

if [ "$MONGO_PROFILE" = "invalid-ssl" -o "$MONGO_PROFILE" = "mutual-ssl" ]; then
    SBT_OPTS="$SBT_OPTS -Dtest.enableSSL=true"

    if [ "$MONGO_PROFILE" = "mutual-ssl" ]; then
        SBT_OPTS="$SBT_OPTS -Djavax.net.ssl.keyStore=/tmp/keystore.jks"
        SBT_OPTS="$SBT_OPTS -Djavax.net.ssl.keyStorePassword=$SSL_PASS"
        SBT_OPTS="$SBT_OPTS -Djavax.net.ssl.keyStoreType=JKS"
    fi
fi

if [ "$MONGO_PROFILE" = "rs" ]; then
    SBT_OPTS="$SBT_OPTS -Dtest.replicaSet=true"
fi

# Netty
SBT_OPTS="$SBT_OPTS -Dshaded.netty.leakDetection.level=paranoid"
SBT_OPTS="$SBT_OPTS -Dshaded.netty.leakDetection.acquireAndReleaseOnly=true"

source "$SCRIPT_DIR/jvmopts.sh"

cat > /dev/stdout <<EOF
- JVM options: $JVM_OPTS
- SBT options: $SBT_OPTS
- Test options: $TEST_OPTS
EOF

export JVM_OPTS
export SBT_OPTS

if [ "x$TEST_CLASSES" = "x" ]; then
  TEST_ARGS=";project ReactiveMongo ;testQuick -- $TEST_OPTS"
  TEST_ARGS="$TEST_ARGS ;project ReactiveMongo-JMX ;testQuick -- $TEST_OPTS"
else
  TEST_ARGS="testQuick $TEST_CLASSES -- $TEST_OPTS"
fi

sed -e 's/"-deprecation", //' < project/ReactiveMongo.scala > .tmp && mv .tmp project/ReactiveMongo.scala

sbt ++$SCALA_VERSION "$TEST_ARGS" || (
    #tail -n 10000 /tmp/mongod.log | grep -v ' end connection ' | grep -v 'connection accepted' | grep -v 'killcursors: found 0 of 1' | tail -n 100

    false
)
