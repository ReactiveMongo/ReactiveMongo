#! /bin/sh

RETRY_COUNT="0"
RETRY_MAX="$1"

shift 1

CMD="$@"
RES="999"

while [ "$RETRY_COUNT" -lt "$RETRY_MAX" ]; do
  $CMD
  RES="$?"
  RETRY_COUNT=`expr $RETRY_COUNT + 1`

  if [ "$RES" -eq 0 ]; then
    grep -r '<error' */target/test-reports/
    grep -r '<failure' */target/test-reports/

    exit 0
  fi
done

exit $RES
