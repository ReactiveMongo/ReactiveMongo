#! /usr/bin/env bash

ENV_FILE="$1"

if [ -r "$ENV_FILE" ]; then
  source "$ENV_FILE"
fi

export LD_LIBRARY_PATH
export PATH

MONGOD_CMD="mongod -f $MONGO_CONF --fork"

if [ $(which numactl | wc -l) -gt 0 ]; then
    MONGOD_CMD="numactl --interleave=all $MONGOD_CMD"
fi

STARTED=no

set +e

while true; do
    if [ "$STARTED" = "no" ]; then
        $MONGOD_CMD

        if [ "$?" -eq 0 ]; then
            echo "[INFO] MongoDB server started"

            STARTED=yes
        else
            echo "[ERROR] Fails to start MongoDB server"
            (test -r /tmp/mongod.log && tail -n 100 /tmp/mongod.log) || true
        fi
    fi

    sleep 1s

    ps axx | grep mongo | grep -v java | grep -v grep
done
