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
cd ${REGTEST_HOME}

# create python venv
if [ ! -d ~/polaris-venv ]; then
  python3 -m venv ~/polaris-venv
fi

# start and setup the python venv
. ~/polaris-venv/bin/activate
pip install poetry==1.5.0
python3 -m poetry install --directory client/python

if [ -z "${1}" ]; then
  loginfo 'Running all tests'
  TEST_LIST="$(find t_* -wholename '*t_*/src/*')"
else
  loginfo "Running single test ${1}"
  TEST_LIST=${1}
fi

export PYTHONDONTWRITEBYTECODE=1

NUM_FAILURES=0
NUM_SUCCESSES=0

export AWS_ACCESS_KEY_ID=''
export AWS_SECRET_ACCESS_KEY=''

for TEST_FILE in ${TEST_LIST}; do
  TEST_SUITE=$(dirname $(dirname ${TEST_FILE}))
  TEST_SHORTNAME=$(basename ${TEST_FILE})
  if [[ "${TEST_SHORTNAME}" =~ .*.py ]]; then
    # skip non-test python files
    if [[ ! "${TEST_SHORTNAME}" =~ ^test_.*.py ]]; then
      continue
    fi
    loginfo "Starting pytest ${TEST_SUITE}:${TEST_SHORTNAME}"
    python3 -m pytest $TEST_FILE
    CODE=$?
    if [[ $CODE -ne 0 ]]; then
      logred "Test FAILED: ${TEST_SUITE}:${TEST_SHORTNAME}"
      NUM_FAILURES=$(( NUM_FAILURES + 1 ))
    else
      loggreen "Test SUCCEEDED: ${TEST_SUITE}:${TEST_SHORTNAME}"
    fi
    continue
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
done

loginfo "Tests completed with ${NUM_SUCCESSES} successes and ${NUM_FAILURES} failures"
if (( ${NUM_FAILURES} > 0 )); then
  exit 1
else
  exit 0
fi
