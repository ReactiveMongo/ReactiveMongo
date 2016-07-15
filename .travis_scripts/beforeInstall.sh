#! /bin/bash

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`
SCALA_VER="$1"
MONGO_SSL="$2"
MONGODB_VER="2_6"
PRIMARY_HOST="localhost:27018"
PRIMARY_SLOW_PROXY="localhost:27019"

if [ `echo "$JAVA_HOME" | grep java-8-oracle | wc -l` -eq 1 ]; then
    MONGODB_VER="3"
fi

cat > /dev/stdout <<EOF
MongoDB major version: $MONGODB_VER
Scala version: $SCALA_VER
EOF

###
# JAVA_HOME     | SCALA_VER || MONGODB_VER | WiredTiger 
# java-7-oracle | 2.11.7    || 3           | true
# java-7-oracle | -         || 3           | false
# -             | _         || 2.6         | false
##

MAX_CON=`ulimit -n`

if [ $MAX_CON -gt 1024 ]; then
    MAX_CON=`expr $MAX_CON - 1024`
fi

echo "Max connection: $MAX_CON"

# Clean cache
rm -rf "$HOME/.ivy2/local/org.reactivemongo"

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
if [ "$MONGODB_VER" = "3" ]; then
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

if [ "$MONGODB_VER" = "3" -a "$MONGO_SSL" = "true" ]; then
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
MONGOD_ST="$?"

if [ ! $MONGOD_ST -eq 0 ]; then
    echo -e "\nERROR: Fails to start the custom 'mongod' instance" > /dev/stderr
    mongod --version
    PID=`ps -ao pid,comm | grep 'mongod$' | cut -d ' ' -f 1`

    if [ ! "x$PID" = "x" ]; then
        pid -p $PID
    fi

    tail -n 100 /tmp/mongod.log

    exit $MONGOD_ST
fi

# Check Mongo connection
MONGOSHELL_OPTS="$PRIMARY_HOST/FOO"

if [ "$MONGO_SSL" = "true" -a ! "$MONGODB_VER" = "2_6" ]; then
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

##

echo -n "- security: "
mongo $MONGOSHELL_OPTS 'var s=db.serverStatus();var x=s["security"];(!x)?"_DISABLED_":x["SSLServerSubjectName"];' 2>/dev/null | tail -n 1

echo -n "- storage engine: "
mongo $MONGOSHELL_OPTS 'var s=db.serverStatus();JSON.stringify(s["storageEngine"]);' 2>/dev/null | grep '"name"' | cut -d '"' -f 4

##

cat > /tmp/validate-env.sh <<EOF
PATH="$PATH"
LD_LIBRARY_PATH="$LD_LIBRARY_PATH"
PRIMARY_HOST="$PRIMARY_HOST"
PRIMARY_SLOW_PROXY="$PRIMARY_SLOW_PROXY"
EOF
