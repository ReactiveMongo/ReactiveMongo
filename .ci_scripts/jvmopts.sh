JVM_MAX_MEM="2048M"

# See .jvmopts
JVM_OPTS="-Xms$JVM_MAX_MEM -Xmx$JVM_MAX_MEM"

if [ `echo "$JAVA_HOME" | grep java-7-oracle | wc -l` -eq 1 -o \
     `echo "$JAVA_HOME" | grep 1.7 | wc -l` -eq 1 ]; then
    JVM_OPTS="$JVM_OPTS -XX:PermSize=512M -XX:MaxPermSize=512M"
else
    JVM_OPTS="$JVM_OPTS -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=512M"
fi

export _JAVA_OPTIONS=""
