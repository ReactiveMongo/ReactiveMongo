#! /bin/bash

if [ `git grep localhost | grep 'src/test' | grep -vi 'Common.scala' | wc -l` -ne 0 ]; then
  echo "[ERROR] Test resources must not contains hardcoded localhost mention"
  exit 1
fi 

if [ "$TRAVIS_SCALA_VERSION" = "2.10.5" -a `javac -version 2>&1 | grep 1.7 | wc -l` -eq 1 ]; then
    echo "[INFO] Check the source format and backward compatibility"

    sbt ++$TRAVIS_SCALA_VERSION scalariformFormat test:scalariformFormat > /dev/null
    git diff --exit-code || (cat >> /dev/stdout <<EOF
[ERROR] Scalariform check failed, see differences above.
To fix, format your sources using sbt scalariformFormat test:scalariformFormat before submitting a pull request.
Additionally, please squash your commits (eg, use git commit --amend) if you're going to update this pull request.
EOF
        false
    )

    sbt ++$TRAVIS_SCALA_VERSION ";error ;mimaReportBinaryIssues" || exit 2
fi

# JVM/SBT setup
source "$SCRIPT_DIR/jvmopts.sh"

cat > /dev/stdout <<EOF
- JVM options: $JVM_OPTS
EOF

export JVM_OPTS

TEST_ARGS=";project ReactiveMongo-BSON ;test-only"

TEST_ARGS="$TEST_ARGS ;project ReactiveMongo"
TEST_ARGS="$TEST_ARGS ;test-only BSONObjectID -- include unit"
TEST_ARGS="$TEST_ARGS ;test-only MongoURISpec -- include unit"
TEST_ARGS="$TEST_ARGS ;test-only NodeSetSpec -- include unit"
TEST_ARGS="$TEST_ARGS ;test-only reactivemongo.BsonSpec -- include unit"

TEST_ARGS="$TEST_ARGS ;project ReactiveMongo-BSON-Macros ;test-only"

sed -e 's/"-deprecation", //' < project/ReactiveMongo.scala > .tmp && mv .tmp project/ReactiveMongo.scala

sbt ++$TRAVIS_SCALA_VERSION "$TEST_ARGS"
