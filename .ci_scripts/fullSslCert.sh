#! /usr/bin/env bash

# Full SSL certification, with root CA (for mutual-ssl or x509)

if [ $# -lt 1 ]; then
    echo "Missing SSL pass phrase"
    exit 1
fi

set -e

PASS="$1"

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`

if [ "x$SCRIPT_DIR" = "x." ]; then
    SCRIPT_DIR="$PWD"
fi

source "$SCRIPT_DIR/ssl-props.sh"

cat > /dev/stdout <<EOF

[INFO] Will generate full SSL certificate: ${SSL_DN_PREFIX}/OU=${SSL_OU}/CN=${SSL_CN}
EOF

# See https://raw.githubusercontent.com/tjworks/mongoscripts/master/x509/setup-x509.sh

# Root CA
cat > /dev/stdout <<EOF

[INFO] Generate root CA"
EOF

openssl genrsa -aes256 -passout "pass:$PASS" -out "$SCRIPT_DIR/root-ca.key" 2048

openssl req -new -x509 -days 3650 -passin "pass:$PASS" \
        -key "$SCRIPT_DIR/root-ca.key" -out "$SCRIPT_DIR/root-ca.crt" \
        -subj "${SSL_DN_PREFIX}/CN=ROOTCA"

mkdir -p "$SCRIPT_DIR/root-ca/ca.db.certs"
echo "01" >> "$SCRIPT_DIR/root-ca/ca.db.serial"
touch "$SCRIPT_DIR/root-ca/ca.db.index"
echo "$RANDOM" >> "$SCRIPT_DIR/root-ca/ca.db.rand"

sed "s|#ROOT_CA#|$SCRIPT_DIR/root-ca|;s|#SIGNING_CA#|$SCRIPT_DIR/signing-ca|" < "$SCRIPT_DIR/root-ca.cfg" > /tmp/root-ca.cfg
  cp $SCRIPT_DIR/root-ca.* $SCRIPT_DIR/root-ca/

# Signing CA
cat > /dev/stdout <<EOF

[INFO] Generate signing CA
EOF

openssl genrsa -aes256 -passout "pass:$PASS" \
        -out "$SCRIPT_DIR/signing-ca.key" 2048

openssl req -new -days 1460 -passin "pass:$PASS" \
        -key "$SCRIPT_DIR/signing-ca.key" -out "$SCRIPT_DIR/signing-ca.csr" \
        -subj "${SSL_DN_PREFIX}/CN=CA-SIGNER"

openssl ca -batch -name "RootCA" -config /tmp/root-ca.cfg \
        -extensions v3_ca -out "$SCRIPT_DIR/signing-ca.crt" \
        -passin "pass:$PASS" -infiles "$SCRIPT_DIR/signing-ca.csr" 

mkdir -p "$SCRIPT_DIR/signing-ca/ca.db.certs"
echo "01" >> "$SCRIPT_DIR/signing-ca/ca.db.serial"
touch "$SCRIPT_DIR/signing-ca/ca.db.index"
echo "$RANDOM" >> "$SCRIPT_DIR/signing-ca/ca.db.rand"
cp $SCRIPT_DIR/signing-ca.* $SCRIPT_DIR/signing-ca/

# Create root-ca PEM
cat "$SCRIPT_DIR/root-ca/root-ca.crt" \
    "$SCRIPT_DIR/signing-ca/signing-ca.crt" > "$SCRIPT_DIR/root-ca/root-ca.pem"

function gen_cert {
    BASENAME="$1"

    openssl genrsa -aes256 -passout "pass:$PASS" -out "$BASENAME.key" 2048

    openssl req -new -days 365 -passin "pass:$PASS" \
            -key "$BASENAME.key" -out "$BASENAME.csr" \
            -subj "${SSL_DN_PREFIX}/OU=${SSL_OU}/CN=${SSL_CN}"

    openssl ca -batch -name "SigningCA" -config /tmp/root-ca.cfg \
            -out "$BASENAME.crt" -passin "pass:$PASS" -infiles "$BASENAME.csr" 

    cat "$BASENAME.crt" "$BASENAME.key" > "$BASENAME.pem"
}

# Server
cat > /dev/stdout <<EOF

[INFO] Generate server certificate
EOF

gen_cert "$SCRIPT_DIR/server-cert"

# Client
cat > /dev/stdout <<EOF

[INFO] Generate client certificate
EOF

gen_cert "$SCRIPT_DIR/client-cert"    

# Keystore
cat > /dev/stdout <<EOF

[INFO] Generate SSL keystore
EOF

openssl pkcs12 -export -out "$SCRIPT_DIR/keystore.p12" \
        -inkey "$SCRIPT_DIR/client-cert.pem" \
        -in "$SCRIPT_DIR/client-cert.pem" \
        -passin "pass:$PASS" -passout "pass:$PASS"

# mongo --ssl --sslPEMKeyPassword reactivemongo --sslPEMKeyFile /path/to/client-cert.pem --sslCAFile /path/to/root-ca/root-ca.pem -u `openssl x509 -in $SCRIPT_DIR/client-cert.pem -inform PEM -subject -nameopt RFC2253 | grep subject | awk '{sub("subject= ",""); print}'` --authenticationMechanism=MONGODB-X509 --authenticationDatabase='$external' db:27018
