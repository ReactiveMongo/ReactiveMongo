#! /bin/sh

RETRY_COUNT="0"
RETRY_MAX="$1"

shift 1

CMD="$@"
RES="999"

if [ "x$TEST_TIMEOUT" = "x" ]; then
  TEST_TIMEOUT="10m"
fi

cat > /dev/stdout <<EOF
[INFO] Will execute with $RETRY_MAX retries (timeout = $TEST_TIMEOUT)

EOF

while [ "$RETRY_COUNT" -lt "$RETRY_MAX" ]; do
  (sleep "$TEST_TIMEOUT" && \
          echo "Timeout after $TEST_TIMEOUT: $CMD" && killall java) &
  $CMD

  RES="$?"
  RETRY_COUNT=`expr $RETRY_COUNT + 1`

  if [ "$RES" -eq 0 ]; then
    exit 0
  fi
done

exit $RES
