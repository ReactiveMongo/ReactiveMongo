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

    sbt ++$SCALA_VERSION ';error ;scalafixAll -check ;scalafmtSbtCheck ;scalafmtCheckAll' > /dev/null || (
        cat >> /dev/stdout <<EOF
[ERROR] Scalafmt check failed, see differences above.
To fix, format your sources using ./build scalafmtAll before submitting a pull request.
Additionally, please squash your commits (eg, use git commit --amend) if you're going to update this pull request.
EOF

        false
    )

    echo "[INFO] Checking the backward compatibility ..."
    sbt ++$SCALA_VERSION ";error ;mimaReportBinaryIssues" || exit 3
fi

# JVM/SBT setup
source "$SCRIPT_DIR/jvmopts.sh"

if [ "x$OS_NAME" = "xosx" ]; then
    SBT_OPTS="-Dtest.nettyNativeArch=osx"
fi

SBT_OPTS="$SBT_OPTS -Dreactivemongo.collectThreadTrace=true"

cat > /dev/stdout <<EOF
- JVM options: $JVM_OPTS
- SBT options: $SBT_OPTS
EOF

export SBT_OPTS
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

TEST_ARGS="testQuick $SPECS_TESTS -- include unit"

sed -e 's/"-deprecation", //' < project/Driver.scala > .tmp && mv .tmp project/Driver.scala

java -version

sbt ++$SCALA_VERSION "$TEST_ARGS ;doc"
