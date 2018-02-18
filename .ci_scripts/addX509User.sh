#! /usr/bin/env bash

set -e

cat > /dev/stdout <<EOF

[INFO] Adding x509 Certificate subject as a User
EOF

# https://docs.mongodb.com/manual/tutorial/configure-x509-client-authentication/#add-x-509-certificate-subject-as-a-user

DB_PATH="$1"
SCRIPT_DIR="$2"
ENV_FILE="$3"
PRIMARY_HOST="$4"

PORT=`echo "$PRIMARY_HOST" | cut -d ":" -f 2`

mongod --bind_ip 0.0.0.0 --port "$PORT" \
       --dbpath "$DB_PATH" --fork --logpath /tmp/mongod.log

CLIENT_CERT_SUBJECT=`openssl x509 -in $SCRIPT_DIR/client-cert.pem -inform PEM -subject -nameopt RFC2253 | grep subject | awk '{sub("subject= ",""); print}'`

ADD_SUBJECT_AS_USER=`mktemp`

echo "[INFO] Resolve client certificate: $CLIENT_CERT_SUBJECT"

cat > "$ADD_SUBJECT_AS_USER" <<EOF
var result = db.getSiblingDB("\$external").runCommand({
  createUser: "$CLIENT_CERT_SUBJECT",
  roles: [
    { role: "userAdminAnyDatabase", db: "admin" },
    { role: "dbAdminAnyDatabase", db: "admin" },
    { role: "readWriteAnyDatabase", db:"admin" },
    { role: "clusterAdmin",  db: "admin" }
  ],
  writeConcern: { j: true, wtimeout: 5000 }
});

printjson(result);
EOF

mongo "$PRIMARY_HOST" "$ADD_SUBJECT_AS_USER"

killall -9 mongod

cat >> "$ENV_FILE" <<EOF
CLIENT_CERT_SUBJECT="$CLIENT_CERT_SUBJECT"
EOF
