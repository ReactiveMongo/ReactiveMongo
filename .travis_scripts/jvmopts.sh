JVM_OPTS="-Xms2048M -Xmx2048M -XX:+CMSClassUnloadingEnabled -XX:ReservedCodeCacheSize=192m"

if [ `echo "$JAVA_HOME" | grep java-7-oracle | wc -l` -eq 1 ]; then
    JVM_OPTS="$JVM_OPTS -XX:PermSize=512M -XX:MaxPermSize=512M"
else
    JVM_OPTS="$JVM_OPTS -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=512M"
fi
