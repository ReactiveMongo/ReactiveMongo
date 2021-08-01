#! /usr/bin/env bash

set -e

echo "[INFO] Clean some IVY cache"
rm -rf "$HOME/.ivy2/local/org.reactivemongo"

CATEGORY="$1"

if [ "$CATEGORY" = "UNIT_TESTS" ]; then
    echo "Skip integration env"
    $SETUP_CMD
    exit 0
fi

MONGO_VER="$2"
MONGO_PROFILE="$3"
ENV_FILE="$4"

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`

echo "[INFO] MongoDB major version: $MONGO_VER"

MONGO_MINOR="3.2.10"

if [ "v$MONGO_VER" = "v5" ]; then
    MONGO_MINOR="5.0.1"
elif [ "v$MONGO_VER" = "v4" ]; then
    MONGO_MINOR="4.4.4"
fi
    
if [ "$AKKA_VERSION" = "2.5.23" ]; then
    MONGO_MINOR="4.2.1"
    MONGO_VER="4"

    echo "[WARN] Fix MongoDB version to $MONGO_MINOR (due to Akka Stream version)"
fi

# Prepare integration env

PRIMARY_HOST=`hostname`":27018"
PRIMARY_SLOW_PROXY=`hostname`":27019"

# OpenSSL
SSL_MAJOR="1.0.0"
SSL_SUFFIX="10"
SSL_RELEASE="1.0.2"
SSL_FULL_RELEASE="1.0.2u"

if [ ! -L "$HOME/ssl/lib/libssl.so.$SSL_MAJOR" ] && [ ! -f "$HOME/ssl/lib/libcrypto.so.$SSL_MAJOR" ]; then
  echo "[INFO] Building OpenSSL $SSL_MAJOR ..."

  cd /tmp
  curl -s -o - "https://www.openssl.org/source/old/$SSL_RELEASE/openssl-$SSL_FULL_RELEASE.tar.gz" | tar -xzf -
  cd openssl-$SSL_FULL_RELEASE
  rm -rf "$HOME/ssl" && mkdir "$HOME/ssl"
  ./config -shared enable-ssl2 --prefix="$HOME/ssl" > /dev/null
  make depend > /dev/null
  make install > /dev/null

  ln -s "$HOME/ssl/lib/libssl.so.$SSL_MAJOR" "$HOME/ssl/lib/libssl.so.$SSL_SUFFIX"
  ln -s "$HOME/ssl/lib/libcrypto.so.$SSL_MAJOR" "$HOME/ssl/lib/libcrypto.so.$SSL_SUFFIX"
fi

export PATH="$HOME/ssl/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/ssl/lib:$LD_LIBRARY_PATH"

# Build MongoDB
echo "[INFO] Installing MongoDB ${MONGO_MINOR} ..."

cd "$HOME"

MONGO_ARCH="x86_64-amazon"
MONGO_HOME="$HOME/mongodb-linux-$MONGO_ARCH-$MONGO_MINOR"

if [ ! -x "$MONGO_HOME/bin/mongod" ]; then
    if [ -d "$MONGO_HOME" ]; then
      rm -rf "$MONGO_HOME"
    fi

    curl -s -o - "https://fastdl.mongodb.org/linux/mongodb-linux-$MONGO_ARCH-$MONGO_MINOR.tgz" | tar -xzf -
    chmod u+x "$MONGO_HOME/bin/mongod"
fi

echo "[INFO] MongoDB available at $MONGO_HOME"

PATH="$MONGO_HOME/bin:$PATH"

cat > "$ENV_FILE" <<EOF
PATH="$PATH"
LD_LIBRARY_PATH="$LD_LIBRARY_PATH"
EOF

$SCRIPT_DIR/setupEnv.sh $MONGO_VER $MONGO_MINOR $MONGO_PROFILE $PRIMARY_HOST $PRIMARY_SLOW_PROXY "$ENV_FILE"
