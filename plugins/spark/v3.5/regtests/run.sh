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

# Define test suites to run
# Each suite specifies: test_file:table_format:test_shortname
declare -a TEST_SUITES=(
  "spark_sql.sh:delta:spark_sql"
  "spark_hudi.sh:hudi:spark_hudi"
)

# Allow running specific test via environment variable
echo "REGTEST_SUITE=${REGTEST_SUITE}"
if [[ -n "$REGTEST_SUITE" ]]; then
  echo "Overriding TEST_SUITES to run only: ${REGTEST_SUITE}"
  TEST_SUITES=("${REGTEST_SUITE}")
fi
echo "Will run test suites: ${TEST_SUITES[@]}"

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
    # Loop through each test suite
    for TEST_SUITE_CONFIG in "${TEST_SUITES[@]}"; do
      # Parse test suite configuration (format: test_file:table_format:test_shortname)
      IFS=':' read -r TEST_FILE TABLE_FORMAT TEST_SHORTNAME <<< "$TEST_SUITE_CONFIG"

      # Skip this suite if REGTEST_SUITE is set and doesn't match
      if [[ -n "$REGTEST_SUITE" ]] && [[ "$TEST_SUITE_CONFIG" != "$REGTEST_SUITE" ]]; then
        echo "Skipping test suite ${TEST_SHORTNAME} (REGTEST_SUITE=${REGTEST_SUITE})"
        continue
      fi

      loginfo "Setting up for test suite: ${TEST_SHORTNAME} with table format: ${TABLE_FORMAT}"

      # clean up the default configuration if exists
      if [ -f "${SPARK_HOME}" ]; then
        SPARK_CONF="${SPARK_HOME}/conf/spark-defaults.conf"
        if [ -f ${SPARK_CONF} ]; then
          rm ${SPARK_CONF}
        fi
      fi

      # Run setup with appropriate table format
      if [ "${SPARK_SHELL_OPTION}" == "PACKAGE" ]; then
        # run the setup without jar configuration
        source ${SCRIPT_DIR}/setup.sh --sparkVersion ${SPARK_VERSION} --scalaVersion ${SCALA_VERSION} --polarisVersion ${POLARIS_VERSION} --tableFormat ${TABLE_FORMAT}
      else
        source ${SCRIPT_DIR}/setup.sh --sparkVersion ${SPARK_VERSION} --scalaVersion ${SCALA_VERSION} --polarisVersion ${POLARIS_VERSION} --jar ${JAR_PATH} --tableFormat ${TABLE_FORMAT}
      fi

      # run the test
      loginfo "Starting test ${TEST_FILE}"

      TEST_TMPDIR="/tmp/polaris-spark-regtests/${TEST_SHORTNAME}_${SPARK_MAJOR_VERSION}_${SCALA_VERSION}_${SPARK_SHELL_OPTION}"
      TEST_STDERR="${TEST_TMPDIR}/${TEST_SHORTNAME}.stderr"
      TEST_STDOUT="${TEST_TMPDIR}/${TEST_SHORTNAME}.stdout"

      mkdir -p ${TEST_TMPDIR}
      if (( ${VERBOSE} )); then
        ${SCRIPT_DIR}/${TEST_FILE} 2>${TEST_STDERR} | grep -v 'loading settings' | tee ${TEST_STDOUT}
      else
        ${SCRIPT_DIR}/${TEST_FILE} 2>${TEST_STDERR} | grep -v 'loading settings' > ${TEST_STDOUT}
      fi
      loginfo "Test run concluded for ${TEST_SHORTNAME}"

      # Compare output with reference
      TEST_REF="$(realpath ${SCRIPT_DIR})/${TEST_SHORTNAME}.ref"
      if cmp --silent ${TEST_STDOUT} ${TEST_REF}; then
        loggreen "Test SUCCEEDED: ${TEST_SHORTNAME}"
      else
        logred "Test FAILED: ${TEST_SHORTNAME}"
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
  done

   clean up
   Commented out for faster development/testing - uncomment for CI or final runs
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
