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
APP_JAR=${APP_DIR}/starter/ares-spark2-starter.jar
echo ${APP_JAR}
APP_MAIN="com.github.ares.spark.starter.SparkStarter"

if [ -f "${CONF_DIR}/ares-env.sh" ]; then
    . "${CONF_DIR}/ares-env.sh"
fi

if [ $# == 0 ]
then
    args="-h"
else
    args=$@
fi

set +u
# Log4j2 Config
if [ -e "${CONF_DIR}/log4j2.properties" ]; then
  JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=${CONF_DIR}/log4j2.properties"
  JAVA_OPTS="${JAVA_OPTS} -Dares.logs.path=${APP_DIR}/logs"
  JAVA_OPTS="${JAVA_OPTS} -Dares.logs.file_name=ares-spark-starter"
fi

CLASS_PATH=${APP_DIR}/starter/logging/*:${APP_JAR}

CMD=$(java ${JAVA_OPTS} -cp ${CLASS_PATH} ${APP_MAIN} ${args}) && EXIT_CODE=$? || EXIT_CODE=$?
if [ ${EXIT_CODE} -eq 234 ]; then
    # print usage
    echo "${CMD}"
    exit 0
elif [ ${EXIT_CODE} -eq 0 ]; then
    echo "Execute Ares Spark Job: $(echo "${CMD}" | tail -n 1)"
    eval $(echo "${CMD}" | tail -n 1)
else
    echo "${CMD}"
    exit ${EXIT_CODE}
fi
