JVM_MAX_MEM="1G"

JVM_MAX_MEM="1632M" # 1760M"
JVM_OPTS="-Xmx$JVM_MAX_MEM -XX:ReservedCodeCacheSize=192m"

if [ ! `uname` = "Darwin" ]; then
  export _JAVA_OPTIONS="$JVM_OPTS"
fi
