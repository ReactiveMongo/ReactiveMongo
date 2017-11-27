#! /bin/bash

set -e

MONGO_VER="$1"
MONGO_MINOR="$2"
MONGO_PROFILE="$3"
PRIMARY_HOST="$4"
PRIMARY_SLOW_PROXY="$5"
ENV_FILE="$6"

echo "[INFO] Running $0 '$MONGO_VER' '$MONGO_MINOR' '$MONGO_PROFILE' '$PRIMARY_HOST' '$PRIMARY_SLOW_PROXY' '$ENV_FILE'"

SCRIPT_DIR=`dirname $0`

# Setup MongoDB service
MAX_CON=`ulimit -n`

if [ $MAX_CON -gt 1024 ]; then
    MAX_CON=`expr $MAX_CON - 1024`
fi

MONGO_CONF=`mktemp`

echo "[INFO] Max connection: $MAX_CON"

export PATH="$HOME/mongodb-linux-x86_64-amazon-$MONGO_MINOR/bin:$PATH"

MONGO_DATA=`mktemp -d`
MONGO_CONF_SUFFIX="26"

if [ "$MONGO_VER" = "3" -o "$MONGO_VER" = "3_4" ]; then
    MONGO_CONF_SUFFIX="3"
fi

sed -e "s|MONGO_DATA|$MONGO_DATA|g" < \
    "$SCRIPT_DIR/mongod${MONGO_CONF_SUFFIX}.conf" > "$MONGO_CONF"

echo "  maxIncomingConnections: $MAX_CON" >> "$MONGO_CONF"

SSL_PASS=""

if [ "$MONGO_PROFILE" = "invalid-ssl" -o "$MONGO_PROFILE" = "mutual-ssl" -o "$MONGO_PROFILE" = "x509" ]; then
    if [ `which uuidgen | wc -l` -eq 1 ]; then
      SSL_PASS=`uuidgen`
    else
      SSL_PASS=`cat /proc/sys/kernel/random/uuid`
    fi

    "$SCRIPT_DIR/genSslCert.sh" $SSL_PASS

    cat >> "$MONGO_CONF" << EOF
  ssl:
    mode: requireSSL
    PEMKeyFile: $SCRIPT_DIR/server.pem
    PEMKeyPassword: $SSL_PASS
EOF

    if [ "$MONGO_PROFILE" = "invalid-ssl" ]; then
        cat >> "$MONGO_CONF" << EOF
    allowInvalidCertificates: true
EOF
    fi

    if [ "$MONGO_PROFILE" = "mutual-ssl" -o "$MONGO_PROFILE" = "x509" ]; then
        # mutual-ssl
        cat >> "$MONGO_CONF" << EOF
    CAFile: $SCRIPT_DIR/client.pem
EOF

        keytool -importkeystore  -srcstoretype PKCS12 \
            -srckeystore "$SCRIPT_DIR/keystore.p12" \
            -destkeystore /tmp/keystore.jks \
            -storepass $SSL_PASS -srcstorepass $SSL_PASS
    fi

    if [ "$MONGO_PROFILE" = "x509" ]; then
        "${SCRIPT_DIR}/addX509User.sh" "$MONGO_DATA" "$SCRIPT_DIR" "$ENV_FILE" "$PRIMARY_HOST"
        cat >> "$MONGO_CONF" << EOF
security:
  clusterAuthMode: x509
EOF
    fi
fi

if [ "$MONGO_PROFILE" = "rs" ]; then
    cat >> "$MONGO_CONF" <<EOF
replication:
  replSetName: "testrs0"
EOF
fi

# MongoDB
echo -e "\n  --- MongoDB Configuration ---"
sed -e 's/^/  /' < "$MONGO_CONF"
echo -e "  --- end ---\n"

# Print version information
MV=`mongod --version 2>/dev/null | head -n 1`

echo -n "[INFO] Installed MongoDB ($MV): "
echo "$MV" | sed -e 's/.* v//'

# Export environment for integration tests
cat >> "$ENV_FILE" <<EOF
PRIMARY_HOST="$PRIMARY_HOST"
PRIMARY_SLOW_PROXY="$PRIMARY_SLOW_PROXY"
SSL_PASS="$SSL_PASS"
MONGO_VER=$MONGO_VER
MONGO_CONF=$MONGO_CONF
MONGO_PROFILE=$MONGO_PROFILE
EOF
