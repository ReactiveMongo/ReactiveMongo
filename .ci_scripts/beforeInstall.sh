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

SCRIPT_DIR=$(dirname $0 | sed -e "s|^\./|$PWD/|")

echo "[INFO] MongoDB major version: $MONGO_VER"

MONGO_MINOR="3.6.23"

case "v$MONGO_VER" in
    v8)
        MONGO_MINOR="8.0.3"
        ;;

    v7)
        MONGO_MINOR="7.0.0"
        ;;

    v6)
        MONGO_MINOR="6.0.0"
        ;;

    v5)
        MONGO_MINOR="5.0.1"
        ;;

    v4)
        MONGO_MINOR="4.4.4"
        ;;
esac
    
# if [ "$AKKA_VERSION" = "2.5.23" ]; then
#     MONGO_MINOR="4.2.1"
#     MONGO_VER="4"

#     echo "[WARN] Fix MongoDB version to $MONGO_MINOR (due to Akka Stream version)"
# fi

# Prepare integration env

PRIMARY_HOST="$(hostname):27018"
PRIMARY_SLOW_PROXY="$(hostname):27019"

# OpenSSL - TODO: Remove
SSL_MAJOR="1.0.0"
SSL_SUFFIX="10"
SSL_RELEASE="1.0.2"
SSL_FULL_RELEASE="1.0.2u"
SSL_LIB_DIRNAME="lib"

SSL_DL_URL="https://www.openssl.org/source/old/$SSL_RELEASE/openssl-$SSL_FULL_RELEASE.tar.gz"

if [ "v$MONGO_VER" = "v8" ]; then
    SSL_MAJOR="3.0.0"
    SSL_SUFFIX="3"
    SSL_RELEASE="3.0.15"
    SSL_FULL_RELEASE="$SSL_RELEASE"
    SSL_LIB_DIRNAME="lib64"
    SSL_DL_URL="https://github.com/openssl/openssl/releases/download/openssl-${SSL_FULL_RELEASE}/openssl-${SSL_FULL_RELEASE}.tar.gz"
fi

if [ ! -f "$HOME/ssl/${SSL_LIB_DIRNAME}/libssl.so.$SSL_MAJOR" ] && [ ! -f "$HOME/ssl/${SSL_LIB_DIRNAME}/libcrypto.so.$SSL_MAJOR" ]; then
  echo "[INFO] Building OpenSSL $SSL_MAJOR ..."

  cd /tmp
  curl -L -s -o - "$SSL_DL_URL" | tar -xzf -
  cd "openssl-$SSL_FULL_RELEASE"
  rm -rf "$HOME/ssl" && mkdir "$HOME/ssl"
  ./config -shared enable-ssl2 --prefix="$HOME/ssl" > /dev/null
  make depend > /dev/null
  make install > /dev/null
fi

if [ ! -f "$HOME/ssl/${SSL_LIB_DIRNAME}/libssl.so.$SSL_SUFFIX" ]; then
  echo "[INFO] Setting up OpenSSL $SSL_MAJOR (libssl) ..."

  mkdir -p "$HOME/ssl/${SSL_LIB_DIRNAME}"

  ln -s "$HOME/ssl/${SSL_LIB_DIRNAME}/libssl.so.$SSL_MAJOR" "$HOME/ssl/${SSL_LIB_DIRNAME}/libssl.so.$SSL_SUFFIX"
fi

if [ ! -f "$HOME/ssl/${SSL_LIB_DIRNAME}/libcrypto.so.$SSL_SUFFIX" ]; then
  echo "[INFO] Setting up OpenSSL $SSL_MAJOR (libcrypto) ..."

  mkdir -p "$HOME/ssl/${SSL_LIB_DIRNAME}"

  ln -s "$HOME/ssl/${SSL_LIB_DIRNAME}/libcrypto.so.$SSL_MAJOR" "$HOME/ssl/${SSL_LIB_DIRNAME}/libcrypto.so.$SSL_SUFFIX"
fi

export PATH="$HOME/ssl/bin:$PATH"

if [ -z "$LD_LIBRARY_PATH" ]; then
    export LD_LIBRARY_PATH="$HOME/ssl/${SSL_LIB_DIRNAME}"
else
    export LD_LIBRARY_PATH="$HOME/ssl/${SSL_LIB_DIRNAME}:$LD_LIBRARY_PATH"
fi

cd "$HOME"

# Build MongoDB
echo "[INFO] Setting up MongoDB ${MONGO_MINOR} ..."

# MongoShell
if [ "$MONGO_VER" -gt 5 ] && [ ! -x "mongosh/bin/mongosh" ]; then
  MONGOSH="mongosh-1.10.6-linux-x64"

  echo "[INFO] Installing $MONGOSH ..."

  curl -s -o - "https://downloads.mongodb.com/compass/$MONGOSH.tgz" | tar -xzf -

  if [ -d "mongosh" ]; then
    rm -rf "mongosh"
  fi

  mv "$MONGOSH" mongosh # versionless naming to ease the CI cache

  ln -s "$PWD/mongosh/bin/mongosh" "mongosh/bin/mongo"
fi

export PATH="$PATH:$PWD/mongosh/bin"

# MongoDB server
if [ ! -x "mongodb/bin/mongod" ]; then
    MONGO_ARCH="x86_64-amazon"

    echo "[INFO] Installing MongoDB ${MONGO_MINOR} ..."

    if [ "v$MONGO_VER" = "v6" -o "v$MONGO_VER" = "v7" ]; then
        MONGO_ARCH="x86_64-amazon2"
    elif [ "v$MONGO_VER" = "v8" ]; then
        MONGO_ARCH="x86_64-amazon2023"
    fi

    curl -s -o - "https://fastdl.mongodb.org/linux/mongodb-linux-${MONGO_ARCH}-${MONGO_MINOR}.tgz" | tar -xzf -

    if [ -d "mongodb" ]; then
      rm -rf "mongodb"
    fi

    mv "mongodb-linux-${MONGO_ARCH}-${MONGO_MINOR}" mongodb

    chmod u+x "mongodb/bin/mongod"
fi

echo "[INFO] MongoDB is available"

PATH="$PWD/mongodb/bin:$PATH"

cat > "$ENV_FILE" <<EOF
PATH="$PATH"
LD_LIBRARY_PATH="$LD_LIBRARY_PATH"
EOF

"$SCRIPT_DIR/setupEnv.sh" $MONGO_VER $MONGO_MINOR $MONGO_PROFILE $PRIMARY_HOST $PRIMARY_SLOW_PROXY "$ENV_FILE"
