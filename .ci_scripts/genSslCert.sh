#! /bin/bash

if [ $# -lt 1 ]; then
    echo "Missing SSL pass phrase"
    exit 1
fi

PASS="$1"

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`

# See http://demarcsek92.blogspot.fr/2014/05/mongodb-ssl-setup.html

if [ `which expect | wc -l` -lt 1 ]; then
  echo "Missing 'expect' command"
  exit 1
fi

# C=FR, ST=Sarthe, L=Le Mans, O=ReactiveMongo, OU=test, CN=localhost/emailAddress=test@reactivemongo.org

# Server
expect << EOF
set timeout 300
spawn openssl req -new -x509 -days 1 -out $SCRIPT_DIR/mongodb-cert.crt -keyout $SCRIPT_DIR/mongodb-cert.key
log_user 0
expect "Enter PEM pass phrase:"
send "$PASS\r"
expect "Enter PEM pass phrase:"
send "$PASS\r"
log_user 1
expect ":"
send "FR\r"
expect ":"
send "Sarthe\r"
expect ":"
send "Le Mans\r"
expect ":"
send "ReactiveMongo\r"
expect ":"
send "test\r"
expect ":"
send "localhost\r"
expect ":"
send "test@reactivemongo\r"
expect eof
EOF

cat "$SCRIPT_DIR/mongodb-cert.key" "$SCRIPT_DIR/mongodb-cert.crt" > "$SCRIPT_DIR/server.pem"

# Client
expect << EOF
set timeout 300
spawn openssl req -new -x509 -days 365 -out $SCRIPT_DIR/client-cert.crt -keyout $SCRIPT_DIR/client-cert.key
log_user 0
expect "Enter PEM pass phrase:"
send "$PASS\r"
expect "Enter PEM pass phrase:"
send "$PASS\r"
log_user 1
expect ":"
send "FR\r"
expect ":"
send "Sarthe\r"
expect ":"
send "Le Mans\r"
expect ":"
send "ReactiveMongo\r"
expect ":"
send "test\r"
expect ":"
send "localhost\r"
expect ":"
send "test@reactivemongo\r"
expect eof
EOF

cat "$SCRIPT_DIR/client-cert.key" "$SCRIPT_DIR/client-cert.crt" > "$SCRIPT_DIR/client.pem"
