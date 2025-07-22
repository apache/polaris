#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Run without args to run all tests.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SPARK_ROOT_DIR=$(dirname ${SCRIPT_DIR})
export SPARK_LOCAL_HOSTNAME=localhost # avoid VPN messing up driver local IP address binding

FMT_RED='\033[0;31m'
FMT_GREEN='\033[0;32m'
FMT_NC='\033[0m'

function loginfo() {
  echo "$(date): ${@}"
}
function loggreen() {
  echo -e "${FMT_GREEN}$(date): ${@}${FMT_NC}"
}
function logred() {
  echo -e "${FMT_RED}$(date): ${@}${FMT_NC}"
}

# Allow bearer token to be provided if desired
if [[ -z "$REGTEST_ROOT_BEARER_TOKEN" ]]; then
  if ! output=$(curl -X POST -H "Polaris-Realm: POLARIS" "http://${POLARIS_HOST:-localhost}:8181/api/catalog/v1/oauth/tokens" \
    -d "grant_type=client_credentials" \
    -d "client_id=root" \
    -d "client_secret=s3cr3t" \
    -d "scope=PRINCIPAL_ROLE:ALL"); then
    logred "Error: Failed to retrieve bearer token"
    exit 1
  fi

  token=$(echo "$output" | awk -F\" '{print $4}')

  if [ "$token" == "unauthorized_client" ]; then
    logred "Error: Failed to retrieve bearer token"
    exit 1
  fi

  export REGTEST_ROOT_BEARER_TOKEN=$token
fi

echo "Root bearer token: ${REGTEST_ROOT_BEARER_TOKEN}"

NUM_FAILURES=0

SCALA_VERSIONS=("2.12" "2.13")
if [[ -n "$CURRENT_SCALA_VERSION" ]]; then
  SCALA_VERSIONS=("${CURRENT_SCALA_VERSION}")
fi
SPARK_MAJOR_VERSION="3.5"
SPARK_VERSION="3.5.6"

SPARK_SHELL_OPTIONS=("PACKAGE" "JAR")

for SCALA_VERSION in "${SCALA_VERSIONS[@]}"; do
  echo "RUN REGRESSION TEST FOR SPARK_MAJOR_VERSION=${SPARK_MAJOR_VERSION}, SPARK_VERSION=${SPARK_VERSION}, SCALA_VERSION=${SCALA_VERSION}"
  # find the project jar
  SPARK_DIR=${SPARK_ROOT_DIR}/spark
  JAR_PATH=$(find ${SPARK_DIR} -name "polaris-spark-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-*.*-bundle.jar" -print -quit)
  echo "find jar ${JAR_PATH}"

  # extract the polaris
  JAR_NAME=$(basename "$JAR_PATH")
  echo "JAR_NAME=${JAR_NAME}"
  POLARIS_VERSION=$(echo "$JAR_NAME" | sed -n 's/.*-\([0-9][^-]*.*\)-bundle\.jar/\1/p')
  echo "$POLARIS_VERSION"

  SPARK_EXISTS="TRUE"
  if [ -z "${SPARK_HOME}" ]; then
    SPARK_EXISTS="FALSE"
  fi

  for SPARK_SHELL_OPTION in "${SPARK_SHELL_OPTIONS[@]}"; do
    # clean up the default configuration if exists
    if [ -f "${SPARK_HOME}" ]; then
      SPARK_CONF="${SPARK_HOME}/conf/spark-defaults.conf"
          if [ -f ${SPARK_CONF} ]; then
            rm ${SPARK_CONF}
          fi
    fi

    if [ "${SPARK_SHELL_OPTION}" == "PACKAGE" ]; then
      # run the setup without jar configuration
      source ${SCRIPT_DIR}/setup.sh --sparkVersion ${SPARK_VERSION} --scalaVersion ${SCALA_VERSION} --polarisVersion ${POLARIS_VERSION}
    else
      source ${SCRIPT_DIR}/setup.sh --sparkVersion ${SPARK_VERSION} --scalaVersion ${SCALA_VERSION} --polarisVersion ${POLARIS_VERSION} --jar ${JAR_PATH}
    fi

    # run the spark_sql test
    loginfo "Starting test spark_sql.sh"

    TEST_FILE="spark_sql.sh"
    TEST_SHORTNAME="spark_sql"
    TEST_TMPDIR="/tmp/polaris-spark-regtests/${TEST_SHORTNAME}_${SPARK_MAJOR_VERSION}_${SCALA_VERSION}"
    TEST_STDERR="${TEST_TMPDIR}/${TEST_SHORTNAME}.stderr"
    TEST_STDOUT="${TEST_TMPDIR}/${TEST_SHORTNAME}.stdout"

    mkdir -p ${TEST_TMPDIR}
    if (( ${VERBOSE} )); then
      ${SCRIPT_DIR}/${TEST_FILE} 2>${TEST_STDERR} | grep -v 'loading settings' | tee ${TEST_STDOUT}
    else
      ${SCRIPT_DIR}/${TEST_FILE} 2>${TEST_STDERR} | grep -v 'loading settings' > ${TEST_STDOUT}
    fi
    loginfo "Test run concluded for ${TEST_SUITE}:${TEST_SHORTNAME}"

    TEST_REF="$(realpath ${SCRIPT_DIR})/${TEST_SHORTNAME}.ref"
    if cmp --silent ${TEST_STDOUT} ${TEST_REF}; then
      loggreen "Test SUCCEEDED: ${TEST_SUITE}:${TEST_SHORTNAME}"
    else
      logred "Test FAILED: ${TEST_SUITE}:${TEST_SHORTNAME}"
      echo '#!/bin/bash' > ${TEST_TMPDIR}/${TEST_SHORTNAME}.fixdiffs.sh
      echo "meld ${TEST_STDOUT} ${TEST_REF}" >> ${TEST_TMPDIR}/${TEST_SHORTNAME}.fixdiffs.sh
      chmod 750 ${TEST_TMPDIR}/${TEST_SHORTNAME}.fixdiffs.sh
      logred "To compare and fix diffs (if 'meld' installed): ${TEST_TMPDIR}/${TEST_SHORTNAME}.fixdiffs.sh"
      logred "Or manually diff: diff ${TEST_STDOUT} ${TEST_REF}"
      logred "See stderr from test run for additional diagnostics: ${TEST_STDERR}"
      diff ${TEST_STDOUT} ${TEST_REF}
      NUM_FAILURES=$(( NUM_FAILURES + 1 ))
    fi
  done

  # clean up
  if [ "${SPARK_EXISTS}" = "FALSE" ]; then
    rm -rf ${SPARK_HOME}
    export SPARK_HOME=""
  fi
done

# clean the output dir
rm -rf ${SCRIPT_DIR}/output

loginfo "Tests completed with ${NUM_FAILURES} failures"
if (( ${NUM_FAILURES} > 0 )); then
  exit 1
else
  exit 0
fi
