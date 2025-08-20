#! /bin/sh

export PUBLISH_REPO_NAME="Sonatype Nexus Repository Manager"
export PUBLISH_REPO_ID="central.sonatype.com"
export PUBLISH_REPO_URL="https://central.sonatype.com/repository/maven-snapshots/"

if [ -z "$PUBLISH_USER" ]; then
  echo "User: "
  read PUBLISH_USER
fi

export PUBLISH_USER

if [ -z "$PUBLISH_PASS" ]; then
  echo "Password: "
  read PUBLISH_PASS
fi

export PUBLISH_PASS

sbt +publish

ACTOR_MODULE=pekko sbt +publish

export REACTIVEMONGO_SHADED=false

sbt +publish

ACTOR_MODULE=pekko sbt +publish
