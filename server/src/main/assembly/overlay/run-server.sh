#!/bin/bash

PATH_TO_CODE_BASE=`pwd`

#JAVA_OPTS="-Djava.security.debug=access -Djava.security.manager -Djava.security.policy=/$PATH_TO_CODE_BASE/java.policy -Djava.rmi.server.useCodebaseOnly=false"	

MAIN_CLASS="hazelcast.server.Server"
JAVA_OPTS=""
OTHER_ARGS=()
for arg in "$@"; do
  if [[ $arg == -D* ]]; then
    JAVA_OPTS="$JAVA_OPTS $arg"
  else
    OTHER_ARGS+=("$arg")
  fi
done


java  $JAVA_OPTS -cp 'lib/jars/*' $MAIN_CLASS $*
