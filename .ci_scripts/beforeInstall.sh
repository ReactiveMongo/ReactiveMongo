#! /bin/bash

set -e

echo "[INFO] Clean some IVY cache"
rm -rf "$HOME/.ivy2/local/org.reactivemongo"

if [ "$CATEGORY" = "UNIT_TESTS" ]; then
    echo "Skip integration env"
    exit 0
fi

CATEGORY="$1"
MONGO_VER="$2"
MONGO_PROFILE="$3"

# Prepare integration env

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`
PRIMARY_HOST="localhost:27018"
PRIMARY_SLOW_PROXY="localhost:27019"

cat > /dev/stdout <<EOF
MongoDB major version: $MONGO_VER
EOF

MAX_CON=`ulimit -n`

if [ $MAX_CON -gt 1024 ]; then
    MAX_CON=`expr $MAX_CON - 1024`
fi

echo "Max connection: $MAX_CON"

# OpenSSL
if [ ! -L "$HOME/ssl/lib/libssl.so.1.0.0" ]; then
  cd /tmp
  curl -s -o - https://www.openssl.org/source/openssl-1.0.1s.tar.gz | tar -xzf -
  cd openssl-1.0.1s
  rm -rf "$HOME/ssl" && mkdir "$HOME/ssl"
  ./config -shared enable-ssl2 --prefix="$HOME/ssl" > /dev/null
  make depend > /dev/null
  make install > /dev/null
else
  rm -f "$HOME/ssl/lib/libssl.so.1.0.0" "libcrypto.so.1.0.0"
fi

ln -s "$HOME/ssl/lib/libssl.so.1.0.0" "$HOME/ssl/lib/libssl.so.10"
ln -s "$HOME/ssl/lib/libcrypto.so.1.0.0" "$HOME/ssl/lib/libcrypto.so.10"

export LD_LIBRARY_PATH="$HOME/ssl/lib:$LD_LIBRARY_PATH"

# Build MongoDB

if [ "$MONGO_VER" = "3" -o "$MONGO_VER" = "3_4" ]; then
    MONGO_TREE="3.2.10"
    
    if [ "$AKKA_VERSION" = "2.5.3" ]; then
        MONGO_TREE="3.4.5"
        MONGO_VER="3_4"

        echo "Fix MongoDB version to 3.4.5 (due to Akka Stream version)"
    fi

    if [ ! -x "$HOME/mongodb-linux-x86_64-amazon-$MONGO_TREE/bin/mongod" ]; then
        curl -s -o /tmp/mongodb.tgz https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-amazon-$MONGO_TREE.tgz
        cd "$HOME" && rm -rf mongodb-linux-x86_64-amazon-$MONGO_TREE
        tar -xzf /tmp/mongodb.tgz && rm -f /tmp/mongodb.tgz
        chmod u+x mongodb-linux-x86_64-amazon-$MONGO_TREE/bin/mongod
    fi

    #find "$HOME/mongodb-linux-x86_64-amazon-$MONGO_TREE" -ls

    export PATH="$HOME/mongodb-linux-x86_64-amazon-$MONGO_TREE/bin:$PATH"
    cp "$SCRIPT_DIR/mongod3.conf" /tmp/mongod.conf

    echo "  maxIncomingConnections: $MAX_CON" >> /tmp/mongod.conf
else
    if [ ! -x "$HOME/mongodb-linux-x86_64-2.6.12/bin/mongod" ]; then
        curl -s -o /tmp/mongodb.tgz https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-2.6.12.tgz
        cd "$HOME" && rm -rf mongodb-linux-x86_64-2.6.12
        tar -xzf /tmp/mongodb.tgz && rm -f /tmp/mongodb.tgz
        chmod u+x mongodb-linux-x86_64-2.6.12/bin/mongod
    fi

    export PATH="$HOME/mongodb-linux-x86_64-2.6.12/bin:$PATH"
    cp "$SCRIPT_DIR/mongod26.conf" /tmp/mongod.conf

    echo "  maxIncomingConnections: $MAX_CON" >> /tmp/mongod.conf
fi

mkdir /tmp/mongodb

SSL_PASS=""

if [ "$MONGO_PROFILE" = "invalid-ssl" -o "$MONGO_PROFILE" = "mutual-ssl" ]; then
    SSL_PASS=`cat /proc/sys/kernel/random/uuid`

    "$SCRIPT_DIR/genSslCert.sh" $SSL_PASS

    cat >> /tmp/mongod.conf << EOF
  ssl:
    mode: requireSSL
    PEMKeyFile: $SCRIPT_DIR/server.pem
    PEMKeyPassword: $SSL_PASS
EOF

    if [ "$MONGO_PROFILE" = "invalid-ssl" ]; then
        cat >> /tmp/mongod.conf << EOF
    allowInvalidCertificates: true
EOF
    else
        # mutual-ssl
        cat >> /tmp/mongod.conf << EOF
    CAFile: $SCRIPT_DIR/client.pem
EOF

        keytool -importkeystore  -srcstoretype PKCS12 \
            -srckeystore $SCRIPT_DIR/keystore.p12 \
            -destkeystore /tmp/keystore.jks \
            -storepass $SSL_PASS -srcstorepass $SSL_PASS

    fi
fi

if [ "$MONGO_PROFILE" = "rs" ]; then
    cat >> /tmp/mongod.conf <<EOF
replication:
  replSetName: "testrs0"
EOF
fi

# MongoDB
echo "# MongoDB Configuration:"
cat /tmp/mongod.conf

# Print version information
MV=`mongod --version 2>/dev/null | head -n 1`

echo -n "INFO: Installed MongoDB ($MV): "
echo "$MV" | sed -e 's/.* v//'

# Export environment for integration tests
cat > /tmp/integration-env.sh <<EOF
PATH="$PATH"
LD_LIBRARY_PATH="$LD_LIBRARY_PATH"
PRIMARY_HOST="$PRIMARY_HOST"
PRIMARY_SLOW_PROXY="$PRIMARY_SLOW_PROXY"
SSL_PASS=$SSL_PASS
MONGO_VER=$MONGO_VER
EOF
