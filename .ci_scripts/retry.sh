#! /usr/bin/env bash

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

set +e

while [ "$RETRY_COUNT" -lt "$RETRY_MAX" ]; do
  echo "Timeout after ${TEST_TIMEOUT}: $CMD"
  timeout --foreground --preserve-status -k "$TEST_TIMEOUT" "$TEST_TIMEOUT" $CMD

  RES="$?"
  RETRY_COUNT=`expr $RETRY_COUNT + 1`

  if [ "$RES" -eq 0 ]; then
    exit 0
  else
    killall -9 java || true # Make sure
  fi
done

if [ $RES -ne 0 ]; then
    echo -n "[WARN] Check OOM kill: "
    grep '^oom_kill ' /sys/fs/cgroup/memory/memory.oom_control
fi

exit $RES
