JVM_MAX_MEM="1G"

if [ "x$CI_CATEGORY" = "xINTEGRATION_TESTS" ]; then
  JVM_MAX_MEM="1760M" #1632M"
fi

#JVM_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap"

# See .jvmopts
JVM_OPTS="-Xmx$JVM_MAX_MEM -XX:ReservedCodeCacheSize=192m"
#JVM_OPTS="-Xms$JVM_MAX_MEM -Xmx$JVM_MAX_MEM"
#JVM_OPTS="$JVM_OPTS -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=512M"

export _JAVA_OPTIONS="$JVM_OPTS"
