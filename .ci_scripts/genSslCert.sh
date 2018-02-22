#! /bin/bash

if [ $# -lt 1 ]; then
    echo "Missing SSL pass phrase"
    exit 1
fi

PASS="$1"

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`

# See http://demarcsek92.blogspot.fr/2014/05/mongodb-ssl-setup.html

# C=FR, ST=Sarthe, L=LeMans, O=ReactiveMongo, OU=test, CN=localhost/emailAddress=test@reactivemongo.org

# Server
openssl req -new -x509 -days 1 -out $SCRIPT_DIR/mongodb-cert.crt -keyout $SCRIPT_DIR/mongodb-cert.key \
   -subj "/C=FR/ST=Sarthe/L=LeMans/O=ReactiveMongo/CN=localhost/OU=cluster-member" -passout pass:$PASS

cat "$SCRIPT_DIR/mongodb-cert.key" "$SCRIPT_DIR/mongodb-cert.crt" > "$SCRIPT_DIR/server.pem"

# Client
openssl req -new -x509 -days 365 -out $SCRIPT_DIR/client-cert.crt -keyout $SCRIPT_DIR/client-cert.key \
   -subj "/C=FR/ST=Sarthe/L=LeMans/O=ReactiveMongo/CN=localhost/OU=client" -passout pass:$PASS

cat "$SCRIPT_DIR/client-cert.key" "$SCRIPT_DIR/client-cert.crt" > "$SCRIPT_DIR/client.pem"

openssl pkcs12 -export -out $SCRIPT_DIR/keystore.p12 -inkey $SCRIPT_DIR/client.pem -in $SCRIPT_DIR/client.pem \
     -passin pass:$PASS -passout pass:$PASS
