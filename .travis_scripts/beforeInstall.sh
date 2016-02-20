#! /bin/sh

# Travis OpenJDK workaround
#cat /etc/hosts # optionally check the content *before*
hostname "$(hostname | cut -c1-63)"
sed -e "s/^\\(127\\.0\\.0\\.1.*\\)/\\1 $(hostname | cut -c1-63)/" /etc/hosts | tee /etc/hosts
#cat /etc/hosts # optionally check the content *after*

SCRIPT_DIR=`dirname $0 | sed -e "s|^\./|$PWD/|"`
SCALA_VER="$1"
MONGO_SSL="$2"
MONGODB_VER="2_6"

if [ `echo "$JAVA_HOME" | grep java-8-oracle | wc -l` -eq 1 ]; then
    MONGODB_VER="3"
fi

cat > /dev/stdout <<EOF
MongoDB major version: $MONGODB_VER
Scala version: $SCALA_VER
EOF

###
# JAVA_HOME     | SCALA_VER || MONGODB_VER | WiredTiger 
# java-7-oracle | 2.11.6    || 3           | true
# java-7-oracle | -         || 3           | false
# -             | _         || 2.6         | false
##

apt-key adv --keyserver hkp://keyserver.ubuntu.com --recv EA312927

if [ "$MONGODB_VER" = "3" ]; then
    echo "deb http://repo.mongodb.org/apt/debian wheezy/mongodb-org/3.2 main" | tee /etc/apt/sources.list.d/mongodb-org-3.2.list
else 
    echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | tee /etc/apt/sources.list.d/mongodb.list
fi

apt-get update
apt-get install mongodb-org-server
apt-get install mongodb-org-shell
service mongod stop

# wiredTiger
if [ "$MONGODB_VER" = "3" -a "$SCALA_VER" = "2.11.6" ]; then
    echo "Prepare MongoDB 3 configuration"

    mkdir /tmp/mongo3wt
    chown -R mongodb:mongodb /tmp/mongo3wt
    chmod -R ug+r /tmp/mongo3wt
    chmod -R u+w /tmp/mongo3wt

    cat > /etc/mongod.conf <<EOF
storage:
  engine: wiredTiger
  dbPath: /tmp/mongo3wt
  journal:
    enabled: true

systemLog:
  destination: file
  logAppend: true
  path: /tmp/mongo3wt.log

net:
  port: 27017
  bindIp: 127.0.0.1
EOF

fi

if [ "$MONGODB_VER" = "3" -a "$MONGO_SSL" = "true" ]; then
    cat >> /etc/mongod.conf <<EOF
  ssl:
    mode: requireSSL
    PEMKeyFile: $SCRIPT_DIR/server.pem
    allowInvalidCertificates: true
EOF

fi

echo "# Configuration:"
cat /etc/mongod.conf

service mongod start && ps axx | grep mongod
#cat /var/log/mongodb/mongod.log

