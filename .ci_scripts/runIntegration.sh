#! /usr/bin/env bash

# Requires: $PRIMARY_HOST, $PRIMARY_SLOW_PROXY, $MONGO_VER, $MONGO_PROFILE, $SCALA_VERSION

set -e

# JVM/SBT setup
SBT_OPTS="-Dtest.primaryHost=$PRIMARY_HOST -Dtest.slowPrimaryHost=$PRIMARY_SLOW_PROXY"
SBT_OPTS="$SBT_OPTS -Dtest.slowProxyDelay=300 -Dtest.slowFailoverRetries=12"

TEST_OPTS=""

# MongoDB
echo "- MongoDB major: $MONGO_VER"

if [ "$MONGO_VER" = "3" ]; then
    TEST_OPTS="exclude mongo2,gt_mongo32,unit"
elif [ "$MONGO_VER" = "4" ]; then
    TEST_OPTS="exclude mongo2,unit"
else
    TEST_OPTS="exclude not_mongo26,gt_mongo32,unit"
    SBT_OPTS="$SBT_OPTS -Dtest.authenticationMechanism=cr"
fi

if [ ! "$MONGO_PROFILE" = "x509" ]; then
    TEST_OPTS="$TEST_OPTS,x509" # exclude x509 tests for all other profiles
else
    TEST_OPTS="$TEST_OPTS,scram_auth,cr_auth" # exclude other auth types for x509 tests

    # See outputs from {full|self}SslCert.sh
    if [ "x${CLIENT_CERT_SUBJECT}" = "x" ]; then
        CLIENT_CERT_SUBJECT=`openssl x509 -in "$SCRIPT_DIR/client-cert.pem" -inform PEM -subject -nameopt RFC2253 | grep subject | awk '{sub("subject= ",""); print}'`
    fi

    SBT_OPTS="$SBT_OPTS -Dtest.authenticationMechanism=x509 -Dtest.clientCertSubject=$CLIENT_CERT_SUBJECT"
fi

if [ "$MONGO_PROFILE" = "invalid-ssl" -o "$MONGO_PROFILE" = "mutual-ssl" -o "$MONGO_PROFILE" = "x509" ]; then
    SBT_OPTS="$SBT_OPTS -Dtest.enableSSL=true"

    if [ "$MONGO_PROFILE" = "mutual-ssl" -o "$MONGO_PROFILE" = "x509" ]; then
        SBT_OPTS="$SBT_OPTS -Dtest.keyStore=file://$SCRIPT_DIR/keystore.p12"
        SBT_OPTS="$SBT_OPTS -Dtest.keyStorePassword=$SSL_PASS"
    fi
fi

if [ "$MONGO_PROFILE" = "rs" ]; then
    SBT_OPTS="$SBT_OPTS -Dtest.replicaSet=true"
fi

if [ "$MONGO_PROFILE" = "default" -a "$MONGO_VER" = "4" ]; then
    SBT_OPTS="$SBT_OPTS -Dtest.nettyNativeArch=linux"
fi

# Netty
SBT_OPTS="$SBT_OPTS -Dreactivemongo.io.netty.leakDetection.level=paranoid"
SBT_OPTS="$SBT_OPTS -Dreactivemongo.io.netty.leakDetection.acquireAndReleaseOnly=true"

source "$SCRIPT_DIR/jvmopts.sh"

cat > /dev/stdout <<EOF
- JVM options: $JVM_OPTS
- SBT options: $SBT_OPTS
- Test options: $TEST_OPTS
EOF

export JVM_OPTS
export SBT_OPTS

MODE="automated"

if [ $# -ge 1 ]; then
    MODE="$1"
fi

sed -e 's/"-deprecation", //' < project/Common.scala > .tmp && mv .tmp project/Common.scala

if [ "x$MODE" = "xinteractive" ]; then
    sbt #++$SCALA_VERSION
else
    TEST_ARGS=";project ReactiveMongo ;testQuick -- $TEST_OPTS"
    TEST_ARGS="$TEST_ARGS ;project ReactiveMongo-JMX ;testQuick -- $TEST_OPTS"

    sbt ++$SCALA_VERSION "$TEST_ARGS"
fi
