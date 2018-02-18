#! /bin/bash

if [ $# -lt 1 ]; then
    echo "Missing SSL pass phrase"
    exit 1
fi

PASS="$1"

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`

if [ "x$SCRIPT_DIR" = "x." ]; then
    SCRIPT_DIR="$PWD"
fi

source "$SCRIPT_DIR/ssl-props.sh"

cat > /dev/stdout <<EOF

[INFO] Will generate self SSL certificate: ${SSL_DN_PREFIX}/OU=${SSL_OU}/CN=${SSL_CN}
EOF

# Server
cat > /dev/stdout <<EOF

[INFO] Generate server certificate
EOF

openssl req -new -x509 -days 1 -out "$SCRIPT_DIR/server-cert.crt" \
        -keyout "$SCRIPT_DIR/server-cert.key" \
        -subj "${SSL_DN_PREFIX}/OU=${SSL_OU}/CN=${SSL_CN}" \
        -passout "pass:$PASS"

cat "$SCRIPT_DIR/server-cert.key" \
    "$SCRIPT_DIR/server-cert.crt" > "$SCRIPT_DIR/server-cert.pem"

# Client
cat > /dev/stdout <<EOF

[INFO] Generate client certificate
EOF

openssl req -new -x509 -days 365 -out "$SCRIPT_DIR/client-cert.crt" \
        -keyout "$SCRIPT_DIR/client-cert.key" \
        -subj "${SSL_DN_PREFIX}/OU=${SSL_OU}/CN=${SSL_CN}" \
        -passout "pass:$PASS"

cat "$SCRIPT_DIR/client-cert.key" \
    "$SCRIPT_DIR/client-cert.crt" > "$SCRIPT_DIR/client-cert.pem"

# Keystore
cat > /dev/stdout <<EOF

[INFO] Generate SSL keystore
EOF

openssl pkcs12 -export -out "$SCRIPT_DIR/keystore.p12" \
        -inkey "$SCRIPT_DIR/client-cert.pem" \
        -in "$SCRIPT_DIR/client-cert.pem" \
        -passin "pass:$PASS" -passout "pass:$PASS"
