#! /usr/bin/env bash

set -e

echo "[INFO] Clean some IVY cache"
rm -rf "$HOME/.ivy2/local/org.reactivemongo"

if [ "$OS_NAME" = "osx" ]; then
  echo "[INFO] Mac OS X setup"

  brew update
  brew install sbt
fi

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

if [ "$MONGO_VER" = "4" ]; then
    MONGO_MINOR="4.0.0"
fi
    
if [ "$AKKA_VERSION" = "2.5.13" ]; then
    MONGO_MINOR="4.0.0"
    MONGO_VER="4"

    echo "[WARN] Fix MongoDB version to $MONGO_MINOR (due to Akka Stream version)"
else
    if [ "$MONGO_VER" = "2_6" ]; then
        MONGO_MINOR="2.6.12"
    fi
fi

# Prepare integration env

PRIMARY_HOST=`hostname`":27018"
PRIMARY_SLOW_PROXY=`hostname`":27019"

# OpenSSL
if [ ! -L "$HOME/ssl/lib/libssl.so.1.0.0" ] && [ ! -f "$HOME/ssl/lib/libssl.so.1.0.0" ]; then
  echo "[INFO] Building OpenSSL"

  cd /tmp
  curl -s -o - https://www.openssl.org/source/openssl-1.0.1s.tar.gz | tar -xzf -
  cd openssl-1.0.1s
  rm -rf "$HOME/ssl" && mkdir "$HOME/ssl"
  ./config -shared enable-ssl2 --prefix="$HOME/ssl" > /dev/null
  make depend > /dev/null
  make install > /dev/null

  ln -s "$HOME/ssl/lib/libssl.so.1.0.0" "$HOME/ssl/lib/libssl.so.10"
  ln -s "$HOME/ssl/lib/libcrypto.so.1.0.0" "$HOME/ssl/lib/libcrypto.so.10"
fi

export PATH="$HOME/ssl/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/ssl/lib:$LD_LIBRARY_PATH"

# Build MongoDB
echo "[INFO] Installing MongoDB ${MONGO_MINOR} ..."

cd "$HOME"

MONGO_ARCH="x86_64-amazon"

if [ "$MONGO_VER" = "2_6" ]; then
  MONGO_ARCH="x86_64"
fi

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
