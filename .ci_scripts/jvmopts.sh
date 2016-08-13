JVM_MAX_MEM="2048M"
JVM_OPTS="-Xms$JVM_MAX_MEM -Xmx$JVM_MAX_MEM -XX:+CMSClassUnloadingEnabled -XX:ReservedCodeCacheSize=192m"

if [ `echo "$JAVA_HOME" | grep java-7-oracle | wc -l` -eq 1 ]; then
    JVM_OPTS="$JVM_OPTS -XX:PermSize=512M -XX:MaxPermSize=512M"
else
    JVM_OPTS="$JVM_OPTS -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=512M"
fi
