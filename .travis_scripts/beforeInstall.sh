#! /bin/sh

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`
SCALA_VER="$1"
MONGO_SSL="$2"
MONGODB_VER="2_6"

if [ `echo "$JAVA_HOME" | grep java-7-oracle | wc -l` -eq 1 ]; then
    MONGODB_VER="3"
fi

###
# JAVA_HOME     | SCALA_VER || MONGODB_VER | WiredTiger 
# java-7-oracle | 2.11.6    || 3           | true
# java-7-oracle | -         || 3           | false
# -             | _         || 2.6         | false
##

apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10

if [ "$MONGODB_VER" = "3" ]; then
    echo "deb http://repo.mongodb.org/apt/ubuntu "$(lsb_release -sc)"/mongodb-org/3.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.0.list
else 
    echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | tee /etc/apt/sources.list.d/mongodb.list
fi

apt-get update
apt-get install mongodb-org-server
apt-get install mongodb-org-shell
service mongod stop

# wiredTiger
if [ "$MONGODB_VER" = "3" -a "$SCALA_VER" = "2.11.6" ]; then
    cat >> /etc/mongod.conf <<EOF
storageEngine = wiredTiger
EOF

    mkdir /tmp/mongo3wt
    chown -R mongodb:mongodb /tmp/mongo3wt
    chmod -R ug+r /tmp/mongo3wt
    chmod -R u+w /tmp/mongo3wt

    sed -e 's|dbpath=/var/lib/mongodb|dbpath=/tmp/mongo3wt|' < /etc/mongod.conf > /tmp/mongod.conf && (cat /tmp/mongod.conf > /etc/mongod.conf)

fi

if [ "$MONGODB_VER" = "3" -a "$MONGO_SSL" = "true" ]; then
    cat >> /etc/mongod.conf <<EOF

sslMode=requireSSL
sslPEMKeyFile=$SCRIPT_DIR/server.pem
sslAllowInvalidCertificates=true
EOF

fi

cat /etc/mongod.conf

service mongod start
