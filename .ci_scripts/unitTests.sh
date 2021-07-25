#! /bin/bash

if [ `git grep localhost | grep 'src/test' | grep -vi 'Common.scala' | wc -l` -ne 0 ]; then
  echo "[ERROR] Test resources must not contains hardcoded localhost mention"
  exit 1
fi

if [ `git grep "$SCRIPT_DIR" $(basename "$SCRIPT_DIR") | grep -v $(basename $0) | wc -l` -ne 0 ]; then
  echo "[ERROR] CI scripts must not contains hardcoded mention to self directory"
  exit 2
fi

echo "[INFO] Checking dependencies for Scala $SCALA_VERSION ..."
sbt ++$SCALA_VERSION update

if [ "$SCALA_VERSION" = "2.11.12" ]; then
    echo "[INFO] Checking the source format ..."
    sbt ++$SCALA_VERSION error scalariformFormat test:scalariformFormat > /dev/null
    git diff --exit-code || (cat >> /dev/stdout <<EOF
[ERROR] Scalariform check failed, see differences above.
To fix, format your sources using sbt scalariformFormat test:scalariformFormat before submitting a pull request.
EOF
        false
    )

    echo "[INFO] Checking the backward compatibility ..."
    sbt ++$SCALA_VERSION ";error ;mimaReportBinaryIssues" || exit 3
fi

# JVM/SBT setup
source "$SCRIPT_DIR/jvmopts.sh"

if [ "x$OS_NAME" = "xosx" ]; then
    export SBT_OPTS="-Dtest.nettyNativeArch=osx"
fi

cat > /dev/stdout <<EOF
- JVM options: $JVM_OPTS
- SBT options: $SBT_OPTS
EOF

export JVM_OPTS

if [ "x$SPECS_TESTS" = "x" ]; then
  SPECS_TESTS="reactivemongo.BsonSpec"
  SPECS_TESTS="$SPECS_TESTS reactivemongo.BulkOpsSpec"
  SPECS_TESTS="$SPECS_TESTS reactivemongo.ChannelFactorySpec"
  SPECS_TESTS="$SPECS_TESTS reactivemongo.QueryBuilderSpec"
  SPECS_TESTS="$SPECS_TESTS AggregationSpec"
  SPECS_TESTS="$SPECS_TESTS DatabaseSpec"
  SPECS_TESTS="$SPECS_TESTS MongoURISpec"
  SPECS_TESTS="$SPECS_TESTS NodeSetSpec"
  SPECS_TESTS="$SPECS_TESTS ProtocolSpec"
  SPECS_TESTS="$SPECS_TESTS ReadPreferenceSpec"
  SPECS_TESTS="$SPECS_TESTS UtilSpec"
  SPECS_TESTS="$SPECS_TESTS WriteResultSpec"
  SPECS_TESTS="$SPECS_TESTS BSONObjectIDSpec"
  SPECS_TESTS="$SPECS_TESTS reactivemongo.InsertCommandSpec"
  SPECS_TESTS="$SPECS_TESTS reactivemongo.UpdateCommandSpec"
fi

TEST_ARGS="$TEST_ARGS testQuick $SPECS_TESTS -- include unit"

sed -e 's/"-deprecation", //' < project/Driver.scala > .tmp && mv .tmp project/Driver.scala

sbt ++$SCALA_VERSION "$TEST_ARGS ;doc"
