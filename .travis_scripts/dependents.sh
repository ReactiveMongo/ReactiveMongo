#! /bin/bash

#TRAVIS_TOKEN=`curl -s -X POST 'https://api.travis-ci.org/auth/github' -H 'Content-Type: application/json' -d "{\"github_token\":\"$GITHUB_TOKEN\"}" | sed -e 's/{"access_token":"//;s/"}//'`

#DEPENDENT="Play-ReactiveMongo ReactiveMongo-Extensions"
DEPENDENT="ReactiveMongo-Extensions"

for REPO in $DEPENDENT; do
  echo -n "Check $REPO... "

  # Get last child project build number
  TMP=`mktemp /tmp/curl.XXXXXX`
  curl -s -X GET "https://api.travis-ci.org/repos/ReactiveMongo/$REPO/builds" -d 'pull_request=false' > "$TMP"

  if [ `grep "\"branch\":\"$TRAVIS_BRANCH\"" "$TMP" | wc -l` -eq 0 ]; then
    echo "no branch $TRAVIS_BRANCH"
  else
    BUILD_NUM=`perl -pe "s/.*\"id\":([0-9]*),.*\"branch\":\"$TRAVIS_BRANCH\".*/\1/" < "$TMP"`

    BRANCH_LAST_COMMIT=`curl -s -X GET "https://api.github.com/repos/ReactiveMongo/$REPO/branches/$TRAVIS_BRANCH" | grep '"sha":' | head -n 1 | cut -d '"' -f 4`
    echo "LASTC: $BRANCH_LAST_COMMIT"
    curl -X GET "https://api.travis-ci.org/builds/$BUILD_NUM"

    echo -n "$TRAVIS_BRANCH build #$BUILD_NUM "

    curl -s -X POST "https://api.travis-ci.org/builds/$BUILD_NUM/restart" -H "Authorization: token $TRAVIS_TOKEN" | sed -e 's/.*"notice":"//;s/".*//;s/^The/the/'
  fi

  rm -f "$TMP"
done
