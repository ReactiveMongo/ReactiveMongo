#! /bin/bash

# Travis OpenJDK workaround
#cat /etc/hosts # optionally check the content *before*
##hostname "$(hostname | cut -c1-63)"
##sed -e "s/^\\(127\\.0\\.0\\.1.*\\)/\\1 $(hostname | cut -c1-63)/" /etc/hosts | tee /etc/hosts
#cat /etc/hosts # optionally check the content *after*

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`
SCALA_VER="$1"
MONGO_SSL="$2"
MONGODB_VER="2_6"
PRIMARY_HOST="localhost:27018"
PRIMARY_BACKEND="no"

# Pip
#export PATH="$HOME/.local/bin:$PATH"
#pip install --upgrade pip > /dev/null
#
#if [ ! -x "$HOME/.local/bin/virtualenv" ]; then
#  pip install --user virtualenv > /dev/null
#fi

# Network latency
if [ `echo "$JAVA_HOME" | grep java-7-oracle | wc -l` -eq 1 -a "$MONGO_SSL" = "false" ]; then
#  if [ ! -x "$HOME/.local/bin/vaurien" ]; then
#    # Vaurien
#    cd "$HOME"
#    git clone https://github.com/lukebakken/vaurien.git --branch fixes/lrb/mysterious-gevent-timeout > /dev/null
#    cd vaurien
#    python setup.py build > /dev/null
#    make > /dev/null
#    python setup.py install --user > /dev/null
#  fi

  echo "Add 300ms to the network latency"
  #vaurien --protocol tcp --behavior 80:delay --behavior-delay-sleep 0.25 \
  #  --proxy localhost:27019 --backend localhost:27018 &>/dev/null &

  PRIMARY_HOST="localhost:27019"
  PRIMARY_BACKEND="localhost:27018"
fi

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

if [ "$MONGODB_VER" = "3" ]; then
    if [ ! -x "$HOME/mongodb-linux-x86_64-amazon-3.2.4/bin/mongod" ]; then
        curl -s -o /tmp/mongodb.tgz https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-amazon-3.2.4.tgz
        cd "$HOME" && rm -rf mongodb-linux-x86_64-amazon-3.2.4
        tar -xzf /tmp/mongodb.tgz && rm -f /tmp/mongodb.tgz
        chmod u+x mongodb-linux-x86_64-amazon-3.2.4/bin/mongod
    fi

    #find "$HOME/mongodb-linux-x86_64-amazon-3.2.4" -ls

    export PATH="$HOME/mongodb-linux-x86_64-amazon-3.2.4/bin:$PATH"
    MONGO_CONF="$SCRIPT_DIR/mongod3.conf"
else
    if [ ! -x "$HOME/mongodb-linux-x86_64-2.6.12/bin/mongod" ]; then
        curl -s -o /tmp/mongodb.tgz https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-2.6.12.tgz
        cd "$HOME" && rm -rf mongodb-linux-x86_64-2.6.12
        tar -xzf /tmp/mongodb.tgz && rm -f /tmp/mongodb.tgz
        chmod u+x mongodb-linux-x86_64-2.6.12/bin/mongod
    fi

    #find "$HOME/mongodb-linux-x86_64-2.6.12" -ls

    export PATH="$HOME/mongodb-linux-x86_64-2.6.12/bin:$PATH"
    MONGO_CONF="$SCRIPT_DIR/mongod26.conf"
fi

mkdir /tmp/mongodb
cp "$MONGO_CONF" /tmp/mongod.conf

if [ "$MONGODB_VER" = "3" -a "$MONGO_SSL" = "true" ]; then
    cat >> /tmp/mongod.conf << EOF
  ssl:
    mode: requireSSL
    PEMKeyFile: $SCRIPT_DIR/server.pem
    allowInvalidCertificates: true
EOF

fi

echo "# Configuration:"
cat /tmp/mongod.conf

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
  #find "$HOME/ssl" -ls
  rm -f "$HOME/ssl/lib/libssl.so.1.0.0" "libcrypto.so.1.0.0"
fi

ln -s "$HOME/ssl/lib/libssl.so.1.0.0" "$HOME/ssl/lib/libssl.so.10"
ln -s "$HOME/ssl/lib/libcrypto.so.1.0.0" "$HOME/ssl/lib/libcrypto.so.10"

export LD_LIBRARY_PATH="$HOME/ssl/lib:$LD_LIBRARY_PATH"

mongod -f /tmp/mongod.conf --port 27018 --fork
#ps axx | grep mongod
#cat /var/log/mongodb/mongod.log

cat > /tmp/validate-env.sh <<EOF
PATH="$PATH"
LD_LIBRARY_PATH="$LD_LIBRARY_PATH"
PRIMARY_HOST="$PRIMARY_HOST"
PRIMARY_BACKEND="$PRIMARY_BACKEND"
EOF
