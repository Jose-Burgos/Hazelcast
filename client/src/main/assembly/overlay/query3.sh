#!/bin/bash

PATH_TO_CODE_BASE=`pwd`

#JAVA_OPTS="-Djava.rmi.server.codebase=file://$PATH_TO_CODE_BASE/lib/jars/rmi-params-client-1.0-SNAPSHOT.jar"

MAIN_CLASS="hazelcast.client.q3.Query3Client"
JAVA_OPTS=""
OTHER_ARGS=()
for arg in "$@"; do
  if [[ $arg == -D* ]]; then
    JAVA_OPTS="$JAVA_OPTS $arg"
  else
    OTHER_ARGS+=("$arg")
  fi
done
java $JAVA_OPTS -cp 'lib/jars/*'  $MAIN_CLASS $*
