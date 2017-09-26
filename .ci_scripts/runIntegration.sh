# JVM/SBT setup
SBT_ARGS="-Dtest.primaryHost=$PRIMARY_HOST"
SBT_ARGS="$SBT_ARGS -Dtest.slowPrimaryHost=$PRIMARY_SLOW_PROXY -Dtest.slowProxyDelay=300 -Dtest.slowFailoverRetries=12"
TEST_OPTS=""

echo "- MongoDB major: $MONGO_VER"

if [ "$MONGO_VER" = "3" ]; then
    TEST_OPTS="exclude mongo2,gt_mongo3,unit"
elif [ "$MONGO_VER" = "3_4" ]; then
    TEST_OPTS="exclude mongo2,unit"
else
    TEST_OPTS="exclude not_mongo26,gt_mongo3,unit"
    SBT_ARGS="$SBT_ARGS -Dtest.authMode=cr"
fi

if [ "$MONGO_PROFILE" = "invalid-ssl" -o "$MONGO_PROFILE" = "mutual-ssl" ]; then
    SBT_ARGS="$SBT_ARGS -Dtest.enableSSL=true"

    if [ "$MONGO_PROFILE" = "mutual-ssl" ]; then
        SBT_ARGS="$SBT_ARGS -Djavax.net.ssl.keyStore=/tmp/keystore.jks"
        SBT_ARGS="$SBT_ARGS -Djavax.net.ssl.keyStorePassword=$SSL_PASS"
        SBT_ARGS="$SBT_ARGS -Djavax.net.ssl.keyStoreType=JKS"
    fi
fi

if [ "$MONGO_PROFILE" = "rs" ]; then
    SBT_ARGS="$SBT_ARGS -Dtest.replicaSet=true"
fi

source "$SCRIPT_DIR/jvmopts.sh"

cat > /dev/stdout <<EOF
- JVM options: $JVM_OPTS
- SBT options: $SBT_ARGS
- Test options: $TEST_OPTS
EOF

export JVM_OPTS
export SBT_OPTS="$SBT_ARGS"

TEST_ARGS=";project ReactiveMongo ;testOnly -- $TEST_OPTS"
TEST_ARGS="$TEST_ARGS ;project ReactiveMongo-JMX ;testOnly -- $TEST_OPTS"

sed -e 's/"-deprecation", //' < project/ReactiveMongo.scala > .tmp && mv .tmp project/ReactiveMongo.scala

sbt ++$TRAVIS_SCALA_VERSION "$TEST_ARGS" || (
    #tail -n 10000 /tmp/mongod.log | grep -v ' end connection ' | grep -v 'connection accepted' | grep -v 'killcursors: found 0 of 1' | tail -n 100

    false
)
