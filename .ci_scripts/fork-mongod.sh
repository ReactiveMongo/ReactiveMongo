#! /bin/bash

source /tmp/integration-env.sh

export LD_LIBRARY_PATH
export PATH

which mongod

MONGOD_CMD="mongod -f /tmp/mongod.conf --port 27018 --fork"

if [ `which numactl | wc -l` -gt 0 ]; then
    numactl --interleave=all $MONGOD_CMD
else
    $MONGOD_CMD
fi
