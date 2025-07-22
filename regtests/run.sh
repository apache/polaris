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
# Run without args to run all tests, or single arg for single test.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

export SPARK_VERSION=spark-3.5.6
export SPARK_DISTRIBUTION=${SPARK_VERSION}-bin-hadoop3

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME=$(realpath ~/${SPARK_DISTRIBUTION})
fi
export PYTHONPATH="${SCRIPT_DIR}/../client/python:${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
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

REGTEST_HOME=$(dirname $(realpath $0))

cd "${SCRIPT_DIR}/.." && ./gradlew regeneratePythonClient
cd ${REGTEST_HOME}

./setup.sh

# start the python venv
. ~/polaris/polaris-venv/bin/activate

if [ -z "${1}" ]; then
  loginfo 'Running all tests'
  TEST_LIST="../client/python/test $(find t_* -wholename '*t_*/src/*')"
else
  loginfo "Running single test ${1}"
  TEST_LIST=${1}
fi

export PYTHONDONTWRITEBYTECODE=1

NUM_FAILURES=0
NUM_SUCCESSES=0

export AWS_ACCESS_KEY_ID=''
export AWS_SECRET_ACCESS_KEY=''

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

for TEST_FILE in ${TEST_LIST}; do
  # Special-case running all client pytests
  if [ "${TEST_FILE}" == '../client/python/test' ]; then
    loginfo "Starting pytest for entire client suite"
    SCRIPT_DIR="$SCRIPT_DIR" python3 -m pytest ${TEST_FILE}
    CODE=$?
    if [[ $CODE -ne 0 ]]; then
      logred "Test FAILED: ${TEST_FILE}"
      NUM_FAILURES=$(( NUM_FAILURES + 1 ))
    else
      loggreen "Test SUCCEEDED: ${TEST_FILE}"
      NUM_SUCCESSES=$(( NUM_SUCCESSES + 1 ))
    fi
    continue
  fi

  # Handle individually-specified pytests
  TEST_SUITE=$(dirname $(dirname ${TEST_FILE}))
  TEST_SHORTNAME=$(basename ${TEST_FILE})
  if [[ "${TEST_SHORTNAME}" =~ .*.py ]]; then
    # skip non-test python files
    if [[ ! "${TEST_SHORTNAME}" =~ ^test_.*.py ]]; then
      continue
    fi
    loginfo "Starting pytest ${TEST_SUITE}:${TEST_SHORTNAME}"
    SCRIPT_DIR="$SCRIPT_DIR" python3 -m pytest $TEST_FILE
    CODE=$?
    if [[ $CODE -ne 0 ]]; then
      logred "Test FAILED: ${TEST_SUITE}:${TEST_SHORTNAME}"
      NUM_FAILURES=$(( NUM_FAILURES + 1 ))
    else
      loggreen "Test SUCCEEDED: ${TEST_SUITE}:${TEST_SHORTNAME}"
      NUM_SUCCESSES=$(( NUM_SUCCESSES + 1 ))
    fi
    continue
  fi

  # Assume anything else is open-ended executable-script based test
  if [[ "${TEST_SHORTNAME}" =~ .*.azure.*.sh ]]; then
      if  [ -z "${AZURE_CLIENT_ID}" ] || [ -z "${AZURE_CLIENT_SECRET}" ] || [ -z "${AZURE_TENANT_ID}" ] ; then
          loginfo "Azure tests not enabled, skip running test ${TEST_FILE}"
          continue
      fi
  fi
  if [[ "${TEST_SHORTNAME}" =~ .*.s3_cross_region.*.sh ]]; then
      if  [ -z "$AWS_CROSS_REGION_TEST_ENABLED" ] || [ "$AWS_CROSS_REGION_TEST_ENABLED" != "true" ] ; then
          loginfo "AWS cross region tests not enabled, skip running test ${TEST_FILE}"
          continue
      fi
  fi
  if [[ "${TEST_SHORTNAME}" =~ .*.s3.*.sh ]]; then
      if  [ -z "$AWS_TEST_ENABLED" ] || [ "$AWS_TEST_ENABLED" != "true" ] || [ -z "$AWS_TEST_BASE" ] ; then
          loginfo "AWS tests not enabled, skip running test ${TEST_FILE}"
          continue
      fi
  fi
  if [[ "${TEST_SHORTNAME}" =~ .*.gcp.sh ]]; then
      # this variable should be the location of your gcp service account key in json
      # it is required by running polaris against local + gcp
      # example: export GOOGLE_APPLICATION_CREDENTIALS="/home/schen/google_account/google_service_account.json"
      if [ -z "$GCS_TEST_ENABLED" ] || [ "$GCS_TEST_ENABLED" != "true" ] || [ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ] ; then
          loginfo "GCS tests not enabled, skip running test ${TEST_FILE}"
          continue
      fi
  fi
  loginfo "Starting test ${TEST_SUITE}:${TEST_SHORTNAME}"

  TEST_TMPDIR="/tmp/polaris-regtests/${TEST_SUITE}"
  TEST_STDERR="${TEST_TMPDIR}/${TEST_SHORTNAME}.stderr"
  TEST_STDOUT="${TEST_TMPDIR}/${TEST_SHORTNAME}.stdout"

  mkdir -p ${TEST_TMPDIR}
  if (( ${VERBOSE} )); then
    ./${TEST_FILE} 2>${TEST_STDERR} | grep -v 'loading settings' | tee ${TEST_STDOUT}
  else
    ./${TEST_FILE} 2>${TEST_STDERR} | grep -v 'loading settings' > ${TEST_STDOUT}
  fi
  loginfo "Test run concluded for ${TEST_SUITE}:${TEST_SHORTNAME}"

  TEST_REF="$(realpath ${TEST_SUITE})/ref/${TEST_SHORTNAME}.ref"
  touch ${TEST_REF}
  if cmp --silent ${TEST_STDOUT}  ${TEST_REF}; then
    loggreen "Test SUCCEEDED: ${TEST_SUITE}:${TEST_SHORTNAME}"
    NUM_SUCCESSES=$(( NUM_SUCCESSES + 1 ))
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

loginfo "Tests completed with ${NUM_SUCCESSES} successes and ${NUM_FAILURES} failures"
if (( ${NUM_FAILURES} > 0 )); then
  exit 1
else
  exit 0
fi
