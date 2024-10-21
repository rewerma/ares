#!/bin/bash
set -eu

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ] ; do
  # shellcheck disable=SC2006
  ls=`ls -ld "$PRG"`
  # shellcheck disable=SC2006
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    # shellcheck disable=SC2006
    PRG=`dirname "$PRG"`/"$link"
  fi
done

PRG_DIR=`dirname "$PRG"`
APP_DIR=`cd "$PRG_DIR/.." >/dev/null; pwd`
CONF_DIR=${APP_DIR}/config
APP_MAIN="com.github.ares.spark.embedded.starter.AresStarter"

if [ -f "${CONF_DIR}/ares-env.sh" ]; then
    . "${CONF_DIR}/ares-env.sh"
fi

jvm_args=""
other_args=""

for arg in "$@"; do
  if [[ "$arg" == -[^-]* ]]; then
    jvm_args="$jvm_args $arg"
  else
    other_args="$other_args $arg"
  fi
done

name=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)
      name="$2"
      shift
      ;;
    *)
      ;;
  esac
  shift
done

if [[ -z "$name" ]]; then
  name=$(uuidgen)
  echo "Using generated task name: $name"
fi


set +u
# Log4j2 Config
if [ -e "${CONF_DIR}/log4j2.properties" ]; then
  JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=${CONF_DIR}/log4j2.properties"
  JAVA_OPTS="${JAVA_OPTS} -Dares.logs.path=${APP_DIR}/logs/task"
  JAVA_OPTS="${JAVA_OPTS} -Dares.logs.file_name=${name}"
fi

CLASS_PATH="${APP_DIR}/starter/logging/*:${APP_DIR}/lib/*:${APP_DIR}/connectors/*:${APP_DIR}/thirdparty/hadoop/*:${APP_DIR}/thirdparty/hive/*:${APP_DIR}/starter/ares-spark3-starter.jar:${APP_DIR}/starter/ares-spark-embedded-starter.jar"

echo "java ${jvm_args} ${JAVA_OPTS} -cp ${CLASS_PATH} ${APP_MAIN} ${other_args}"

java ${jvm_args} ${JAVA_OPTS} -cp ${CLASS_PATH} ${APP_MAIN} ${other_args}