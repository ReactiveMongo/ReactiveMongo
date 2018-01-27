#! /bin/bash

ENV_FILE="$1"
source "$ENV_FILE"

export LD_LIBRARY_PATH
export PATH

which mongod

MONGOD_CMD="mongod -f $MONGO_CONF --port 27018 --fork"

$MONGOD

if [ `which numactl | wc -l` -gt 0 ]; then
    numactl --interleave=all $MONGOD_CMD
else
    $MONGOD_CMD
fi
