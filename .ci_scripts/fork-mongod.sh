#! /usr/bin/env bash

ENV_FILE="$1"

if [ -r "$ENV_FILE" ]; then
  source "$ENV_FILE"
fi

export LD_LIBRARY_PATH
export PATH

MONGOD_CMD="mongod -f $MONGO_CONF --fork"

if [ `which numactl | wc -l` -gt 0 ]; then
    MONGOD_CMD="numactl --interleave=all $MONGOD_CMD"
fi

while true; do ($MONGOD_CMD); done
