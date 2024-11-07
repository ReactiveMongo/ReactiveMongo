#! /usr/bin/env bash

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

MONGO_ARCH="x86_64-amazon"

if [ "v$MONGO_VER" = "v7" ]; then
    ARCH="x86_64-amazon2"
fi

export PATH="$HOME/mongodb/bin:$PATH"

MONGO_DATA=`mktemp -d`
MONGO_CONF_SUFFIX="26"

M3P=(v3 v4 v5 v6 v7)

if [[ ${M3P[@]} =~ "v$MONGO_VER" ]]; then
    MONGO_CONF_SUFFIX="3"
fi

sed -e "s|MONGO_DATA|$MONGO_DATA|g" < \
    "$SCRIPT_DIR/mongod${MONGO_CONF_SUFFIX}.conf" > "$MONGO_CONF"

echo "  maxIncomingConnections: $MAX_CON" >> "$MONGO_CONF"

# So available as empty if not X509 (for java-dockerfile COPY)
if [ ! -d "$SCRIPT_DIR/root-ca" ]; then
    mkdir "$SCRIPT_DIR/root-ca"
fi

touch "$SCRIPT_DIR/client-cert.pem" \
      "$SCRIPT_DIR/root-ca/root-ca.pem" \
      "$SCRIPT_DIR/keystore.p12"

if [ "$MONGO_PROFILE" = "invalid-ssl" -o "$MONGO_PROFILE" = "mutual-ssl" -o "$MONGO_PROFILE" = "x509" ]; then
    if [ "x$SSL_PASS" = "x" ]; then
      if [ `which uuidgen | wc -l` -eq 1 ]; then
        SSL_PASS=`uuidgen`
      else
        SSL_PASS=`cat /proc/sys/kernel/random/uuid`
      fi
    fi


    if [ "$MONGO_PROFILE" = "invalid-ssl" ]; then
        "$SCRIPT_DIR/selfSslCert.sh" "$SSL_PASS"
    else
        "$SCRIPT_DIR/fullSslCert.sh" "$SSL_PASS"
    fi

    ls -al "$SCRIPT_DIR/keystore.p12"

    if [ "$MONGO_VER" -ge 4 ]; then
        cat >> "$MONGO_CONF" << EOF
  tls:
    mode: requireTLS
    certificateKeyFile: $SCRIPT_DIR/server-cert.pem
    certificateKeyFilePassword: $SSL_PASS
EOF
    else
        cat >> "$MONGO_CONF" << EOF
  ssl:
    mode: requireSSL
    PEMKeyFile: $SCRIPT_DIR/server-cert.pem
    PEMKeyPassword: $SSL_PASS
EOF
    fi

    if [ "$MONGO_PROFILE" != "invalid-ssl" ]; then
        cat >> "$MONGO_CONF" << EOF
    CAFile: $SCRIPT_DIR/root-ca/root-ca.pem
EOF
    else
        cat >> "$MONGO_CONF" << EOF
    allowInvalidCertificates: true
EOF
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
echo -e "\n  --- MongoDB Configuration: $MONGO_CONF ---"
sed -e 's/^/  /' < "$MONGO_CONF"
echo -e "  --- end ---\n"

mongod --version

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
MONGO_MINOR=$MONGO_MINOR
MONGO_CONF=$MONGO_CONF
MONGO_PROFILE=$MONGO_PROFILE
EOF
