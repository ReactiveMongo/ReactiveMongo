JVM_MAX_MEM="1G"

JVM_MAX_MEM="1632M" # 1760M"
JVM_OPTS="-Xmx$JVM_MAX_MEM -XX:ReservedCodeCacheSize=192m"

if [ "$JDK_VERSION" != "openjdk17" ]; then
  JVM_OPTS="$JVM_OPTS -XX:+CMSClassUnloadingEnabled"
fi

if [ ! `uname` = "Darwin" ]; then
  export _JAVA_OPTIONS="$JVM_OPTS"
fi
