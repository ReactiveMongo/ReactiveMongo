#!/usr/bin/env bash

set -e -x

echo
echo "adding x509 Certificate subject as a User"
# https://docs.mongodb.com/manual/tutorial/configure-x509-client-authentication/#add-x-509-certificate-subject-as-a-user

DB_PATH="$1"
SCRIPT_DIR="$2"
ENV_FILE="$3"
PRIMARY_HOST="$4"

PORT=$(echo "$PRIMARY_HOST" | cut -d ":" -f 2)

mongod --dbpath "$DB_PATH" --port "$PORT" --fork --logpath /tmp/mongod.log

CLIENT_CERT_SUBJECT=`openssl x509 -in $SCRIPT_DIR/client.pem -inform PEM -subject -nameopt RFC2253 | grep subject | awk '{sub("subject= ",""); print}'`

ADD_SUBJECT_AS_USER=$(mktemp)

cat > "$ADD_SUBJECT_AS_USER" <<EOF
var result = db.getSiblingDB("\$external").runCommand(
  	{
    	createUser: "$CLIENT_CERT_SUBJECT",
    	roles: [
             { role: 'root', db: 'admin' }
           ],
    	writeConcern: { j: true, wtimeout: 5000 }
  	}
);

printjson(result);
EOF

mongo "$PRIMARY_HOST" "$ADD_SUBJECT_AS_USER"

killall -9 mongod

cat >> "$ENV_FILE" <<EOF
CLIENT_CERT_SUBJECT="$CLIENT_CERT_SUBJECT"
EOF
