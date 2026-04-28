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

# OpenSSL
SSL_HOME="${SSL_HOME:-$HOME/ssl}"

if [ -z "$SSL_HOME" ]; then
  echo "[ERROR] SSL_HOME is empty"
  exit 1
fi

SSL_MAJOR="1.0.0"
SSL_SUFFIX="10"
SSL_RELEASE="1.0.2"
SSL_FULL_RELEASE="1.0.2u"
SSL_GH_TAG="OpenSSL_1_0_2u"
SSL_LIB_DIRNAME="lib"

SSL_DL_URL="https://github.com/openssl/openssl/releases/download/${SSL_GH_TAG}/openssl-${SSL_FULL_RELEASE}.tar.gz"

if [ "v$MONGO_VER" = "v8" ]; then
    SSL_MAJOR="3.0.0"
    SSL_SUFFIX="3"
    SSL_RELEASE="3.0.19"
    SSL_FULL_RELEASE="$SSL_RELEASE"
    SSL_GH_TAG="openssl-3.0.19"
    SSL_LIB_DIRNAME="lib64"
    SSL_DL_URL="https://github.com/openssl/openssl/releases/download/${SSL_GH_TAG}/openssl-${SSL_FULL_RELEASE}.tar.gz"
fi

SSL_LIB="${SSL_LIB:-$SSL_HOME/$SSL_LIB_DIRNAME}"

resolve_ssl_lib() {
  local cand

  for cand in "$SSL_LIB" "$SSL_HOME/$SSL_LIB_DIRNAME" "$SSL_HOME/lib64" "$SSL_HOME/lib"; do
    if [ -d "$cand" ] && [ -f "$cand/libssl.so.$SSL_MAJOR" ] && [ -f "$cand/libcrypto.so.$SSL_MAJOR" ]; then
      SSL_LIB="$cand"
      return 0
    fi
  done

  return 1
}

if ! resolve_ssl_lib; then
  echo "[INFO] Building OpenSSL $SSL_MAJOR ..."

  cd /tmp

  echo "[INFO] Downloading OpenSSL from $SSL_DL_URL ..."
  curl -fL -s -o - "$SSL_DL_URL" | tar -xzf -

  cd "openssl-${SSL_FULL_RELEASE}"
  rm -rf "$SSL_HOME" && mkdir -p "$SSL_HOME"

  echo "[INFO] Configuring OpenSSL build ..."
  ./config -shared enable-ssl2 --prefix="$SSL_HOME" > /dev/null

  echo "[INFO] Resolving dependencies for OpenSSL build ..."
  make depend > /dev/null

  echo "[INFO] Building and installing OpenSSL ..."
  make install > /dev/null
fi

if ! resolve_ssl_lib; then
  echo "[ERROR] OpenSSL libraries are missing in $SSL_LIB"
  exit 1
fi

ln -sf "$SSL_LIB/libssl.so.$SSL_MAJOR" "$SSL_LIB/libssl.so.$SSL_SUFFIX"
ln -sf "$SSL_LIB/libcrypto.so.$SSL_MAJOR" "$SSL_LIB/libcrypto.so.$SSL_SUFFIX"

export PATH="$SSL_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$SSL_LIB:${LD_LIBRARY_PATH:-}"

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
