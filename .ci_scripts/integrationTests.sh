#! /bin/bash

source /tmp/integration-env.sh

export LD_LIBRARY_PATH

MONGOD_PID=`ps -o pid,comm -u $USER | grep 'mongod$' | awk '{ printf("%s\n", $1); }'`

if [ "x$MONGOD_PID" = "x" ]; then
    echo "ERROR: MongoDB process not found" > /dev/stderr
    tail -n 100 /tmp/mongod.log
    exit 1
fi

# Check MongoDB connection
SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`
MONGOSHELL_OPTS=""

if [ "$MONGO_PROFILE" = "invalid-ssl" ]; then
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS --ssl --sslAllowInvalidCertificates"
fi

if [ "$MONGO_PROFILE" = "mutual-ssl" ]; then
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS --ssl --sslCAFile $SCRIPT_DIR/server.pem"
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS --sslPEMKeyFile $SCRIPT_DIR/client.pem"
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS --sslPEMKeyPassword $SSL_PASS"
fi

MONGOSHELL_OPTS="$MONGOSHELL_OPTS --eval"
MONGODB_NAME=`mongo "$PRIMARY_HOST/FOO" $MONGOSHELL_OPTS 'db.getName()' 2>/dev/null | tail -n 1`

if [ ! "x$MONGODB_NAME" = "xFOO" ]; then
    echo -n "\nERROR: Fails to connect using the MongoShell\n"
    mongo "$PRIMARY_HOST/FOO" $MONGOSHELL_OPTS 'db.getName()'
    tail -n 100 /tmp/mongod.log
    exit 2
fi

# Check MongoDB options
echo -n "- server version: "

mongo "$PRIMARY_HOST/FOO" $MONGOSHELL_OPTS 'var s=db.serverStatus();s.version' 2>/dev/null | tail -n 1

echo -n "- security: "
mongo "$PRIMARY_HOST/FOO" $MONGOSHELL_OPTS 'var s=db.serverStatus();var x=s["security"];(!x)?"_DISABLED_":x["SSLServerSubjectName"];' 2>/dev/null | tail -n 1

echo -n "- storage engine: "
mongo "$PRIMARY_HOST/FOO" $MONGOSHELL_OPTS 'var s=db.serverStatus();JSON.stringify(s["storageEngine"]);' 2>/dev/null | grep '"name"' | cut -d '"' -f 4

if [ "$MONGO_PROFILE" = "rs" ]; then
    mongo "$PRIMARY_HOST" $MONGOSHELL_OPTS "rs.initiate({\"_id\":\"testrs0\",\"version\":1,\"members\":[{\"_id\":0,\"host\":\"$PRIMARY_HOST\"}]});" || (
        echo "ERROR: Fails to setup the ReplicaSet" > /dev/stderr
        false
    )
fi

source "$SCRIPT_DIR/runIntegration.sh"
