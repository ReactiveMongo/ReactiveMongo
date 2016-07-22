#! /bin/bash

echo "[INFO] Clean some IVY cache"
rm -rf "$HOME/.ivy2/local/org.reactivemongo"

if [ "$CATEGORY" = "UNIT_TESTS" ]; then
    echo "Skip integration env"
    exit 0
fi

CATEGORY="$1"
MONGO_VER="$2"
MONGO_SSL="$3"

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
if [ "$MONGO_VER" = "3" ]; then
    if [ ! -x "$HOME/mongodb-linux-x86_64-amazon-3.2.8/bin/mongod" ]; then
        curl -s -o /tmp/mongodb.tgz https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-amazon-3.2.8.tgz
        cd "$HOME" && rm -rf mongodb-linux-x86_64-amazon-3.2.8
        tar -xzf /tmp/mongodb.tgz && rm -f /tmp/mongodb.tgz
        chmod u+x mongodb-linux-x86_64-amazon-3.2.8/bin/mongod
    fi

    #find "$HOME/mongodb-linux-x86_64-amazon-3.2.8" -ls

    export PATH="$HOME/mongodb-linux-x86_64-amazon-3.2.8/bin:$PATH"
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

if [ "$MONGO_VER" = "3" -a "$MONGO_SSL" = "true" ]; then
    cat >> /tmp/mongod.conf << EOF
  ssl:
    mode: requireSSL
    PEMKeyFile: $SCRIPT_DIR/server.pem
    PEMKeyPassword: test
    allowInvalidCertificates: true
EOF

fi

# MongoDB
echo "# MongoDB Configuration:"
cat /tmp/mongod.conf

numactl --interleave=all mongod -f /tmp/mongod.conf --port 27018 --fork

MONGOD_PID=`ps -o pid,comm -u $USER | grep 'mongod$' | awk '{ printf("%s\n", $1); }'`

if [ "x$MONGOD_PID" = "x" ]; then
    echo -e "\nERROR: Fails to start the custom 'mongod' instance" > /dev/stderr

    mongod --version
    PID=`ps -o pid,comm -u $USER | grep 'mongod$' | awk '{ printf("%s\n", $1); }'`

    if [ ! "x$PID" = "x" ]; then
        pid -p $PID
    else
        echo "ERROR: MongoDB process not found" > /dev/stderr
    fi

    tail -n 100 /tmp/mongod.log

    exit 1
fi

# Check MongoDB connection
MONGOSHELL_OPTS="$PRIMARY_HOST/FOO"

if [ "$MONGO_SSL" = "true" -a ! "$MONGO_VER" = "2_6" ]; then
    MONGOSHELL_OPTS="$MONGOSHELL_OPTS --ssl --sslAllowInvalidCertificates"
fi

MONGOSHELL_OPTS="$MONGOSHELL_OPTS --eval"
MONGODB_NAME=`mongo $MONGOSHELL_OPTS 'db.getName()' 2>/dev/null | tail -n 1`

if [ ! "x$MONGODB_NAME" = "xFOO" ]; then
    echo -n "\nERROR: Fails to connect using the MongoShell\n"
    mongo $MONGOSHELL_OPTS 'db.getName()'
    tail -n 100 /tmp/mongod.log
    exit 2
fi

# Check MongoDB runtime

echo -n "- security: "
mongo $MONGOSHELL_OPTS 'var s=db.serverStatus();var x=s["security"];(!x)?"_DISABLED_":x["SSLServerSubjectName"];' 2>/dev/null | tail -n 1

echo -n "- storage engine: "
mongo $MONGOSHELL_OPTS 'var s=db.serverStatus();JSON.stringify(s["storageEngine"]);' 2>/dev/null | grep '"name"' | cut -d '"' -f 4

# Export environment for integration tests

cat > /tmp/integration-env.sh <<EOF
PATH="$PATH"
LD_LIBRARY_PATH="$LD_LIBRARY_PATH"
PRIMARY_HOST="$PRIMARY_HOST"
PRIMARY_SLOW_PROXY="$PRIMARY_SLOW_PROXY"
EOF
