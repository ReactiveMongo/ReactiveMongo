#! /bin/bash

ENV_FILE="$1"

source "$ENV_FILE"
export LD_LIBRARY_PATH

MONGOD_PID=`ps -o pid,comm -u $USER | grep 'mongod$' | awk '{ printf("%s\n", $1); }'`

if [ "x$MONGOD_PID" = "x" ]; then
    echo "[ERROR] MongoDB process not found" > /dev/stderr
    tail -n 100 /tmp/mongod.log
    exit 1
fi

# Check MongoDB connection
SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`
MONGOSHELL_OPTS=""

# prepare SSL options
if [ "$MONGO_PROFILE" = "invalid-ssl" ]; then
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS --ssl --sslAllowInvalidCertificates"
fi

if [ "$MONGO_PROFILE" = "mutual-ssl" -o "$MONGO_PROFILE" = "x509" ]; then
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS --ssl --sslAllowInvalidCertificates"
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS --sslCAFile $SCRIPT_DIR/server-cert.pem"
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS --sslPEMKeyFile $SCRIPT_DIR/client-cert.pem"
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS --sslPEMKeyPassword $SSL_PASS"
fi

if [ "$MONGO_PROFILE" = "x509" ]; then
    CLIENT_CERT_SUBJECT=`openssl x509 -in "$SCRIPT_DIR/client-cert.pem" -inform PEM -subject -nameopt RFC2253 | grep subject | awk '{sub("subject= ",""); print}'`
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS -u $CLIENT_CERT_SUBJECT"
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS --authenticationMechanism=MONGODB-X509"
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS --authenticationDatabase=\$external"
fi

# prepare common options
MONGOSHELL_OPTS="$MONGOSHELL_OPTS --eval"
MONGODB_NAME=`mongo "$PRIMARY_HOST/FOO" $MONGOSHELL_OPTS 'db.getName()' 2>/dev/null | tail -n 1`

if [ ! "x$MONGODB_NAME" = "xFOO" ]; then
    echo -e -n "\n[ERROR] Fails to connect using the MongoShell: $PRIMARY_HOST ($MONGO_PROFILE); Retrying with $MONGOSHELL_OPTS ...\n"
    mongo "$PRIMARY_HOST/FOO" $MONGOSHELL_OPTS 'db.getName()'
    tail -n 100 /tmp/mongod.log
    exit 2
fi

# Check MongoDB options
echo "[INFO] Checking MongoDB service ..."

echo -n "- Server version: "

mongo "$PRIMARY_HOST/FOO" $MONGOSHELL_OPTS 'var s=db.serverStatus();s.version' 2>/dev/null | tail -n 1

echo -n "- Security: "
mongo "$PRIMARY_HOST/FOO" $MONGOSHELL_OPTS 'var s=db.serverStatus();var x=s["security"];(!x)?"_DISABLED_":x["SSLServerSubjectName"];' 2>/dev/null | tail -n 1

if [ ! "v$MONGO_VER" = "v2_6" ]; then
    echo -n "- Storage engine: "
    mongo "$PRIMARY_HOST/FOO" $MONGOSHELL_OPTS 'var s=db.serverStatus();JSON.stringify(s["storageEngine"]);' 2>/dev/null | grep '"name"' | cut -d '"' -f 4
fi

if [ "$MONGO_PROFILE" = "rs" ]; then
    mongo "$PRIMARY_HOST" $MONGOSHELL_OPTS "rs.initiate({\"_id\":\"testrs0\",\"version\":1,\"members\":[{\"_id\":0,\"host\":\"$PRIMARY_HOST\"}]});" || (
        echo "[ERROR] Fails to setup the ReplicaSet" > /dev/stderr
        false
    )
fi

echo ""

source "$SCRIPT_DIR/runIntegration.sh"
