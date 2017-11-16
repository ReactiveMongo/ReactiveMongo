#!/usr/bin/env bash

set -e -x

echo
echo "adding x509 Certificate subject as a User"
# https://docs.mongodb.com/manual/tutorial/configure-x509-client-authentication/#add-x-509-certificate-subject-as-a-user

PATH="$1"
DB_PATH="$2"
SCRIPT_DIR="$3"
ENV_FILE="$4"

PORT=27018

mongod --dbpath $DB_PATH --port $PORT --fork --logpath /tmp/mongod.log

CLIENT_CERT_SUBJECT=`openssl x509 -in $SCRIPT_DIR/client.pem -inform PEM -subject -nameopt RFC2253 | grep subject | awk '{sub("subject= ",""); print}'`

cat > /tmp/add_subject_as_user.js <<EOF
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

mongo "localhost:$PORT" /tmp/add_subject_as_user.js

killall -9 mongod

cat >> "$ENV_FILE" <<EOF
CLIENT_CERT_SUBJECT="$CLIENT_CERT_SUBJECT"
EOF
