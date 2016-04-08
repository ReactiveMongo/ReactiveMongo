#! /bin/bash

if [ `git grep localhost | grep 'src/test' | grep -vi 'Common.scala' | wc -l` -ne 0 ]; then
  echo "[ERROR] Test resources must not contains hardcoded localhost mention"
  exit 1
fi 

if [ "$SCALA_VERSION" = "2.11.11" -a `javac -version 2>&1 | grep 1.7 | wc -l` -eq 1 ]; then
    echo "[INFO] Check the source format and backward compatibility"

    sbt ++$SCALA_VERSION scalariformFormat test:scalariformFormat > /dev/null
    git diff --exit-code || (cat >> /dev/stdout <<EOF
[ERROR] Scalariform check failed, see differences above.
To fix, format your sources using sbt scalariformFormat test:scalariformFormat before submitting a pull request.
Additionally, please squash your commits (eg, use git commit --amend) if you're going to update this pull request.
EOF
        false
    )

    sbt ++$SCALA_VERSION ";error ;mimaReportBinaryIssues" || exit 2

    sbt ++$SCALA_VERSION ";project ReactiveMongo-BSON ;findbugs ;project ReactiveMongo-BSON-Macros ;findbugs ;project ReactiveMongo ;findbugs ;project ReactiveMongo-JMX ;findbugs" || exit 3
fi

# JVM/SBT setup
source "$SCRIPT_DIR/jvmopts.sh"

cat > /dev/stdout <<EOF
- JVM options: $JVM_OPTS
EOF

export JVM_OPTS

TEST_ARGS=";project ReactiveMongo-BSON ;testQuick"

TEST_ARGS="$TEST_ARGS ;project ReactiveMongo; testQuick"
TEST_ARGS="$TEST_ARGS BSONObjectIDSpec"
TEST_ARGS="$TEST_ARGS MongoURISpec"
TEST_ARGS="$TEST_ARGS NodeSetSpec"
TEST_ARGS="$TEST_ARGS reactivemongo.BsonSpec"
TEST_ARGS="$TEST_ARGS UpdateSpec"
TEST_ARGS="$TEST_ARGS -- include unit"

TEST_ARGS="$TEST_ARGS ;project ReactiveMongo-BSON-Macros ;testQuick"

sed -e 's/"-deprecation", //' < project/ReactiveMongo.scala > .tmp && mv .tmp project/ReactiveMongo.scala

sbt ++$SCALA_VERSION "$TEST_ARGS"
