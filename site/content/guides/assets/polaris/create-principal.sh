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

set -e

apk add --no-cache jq

realm=${1:-"POLARIS"}

principal_name=${2:?"principal name is required"}

role_name=${3:-"service_admin"}

TOKEN=${4:-""}

BASEDIR=$(dirname $0)

if [ -z "$TOKEN" ]; then
  source $BASEDIR/obtain-token.sh
fi

echo
echo "Obtained access token: ${TOKEN}"

echo
echo Creating principal $principal_name in realm $realm...

curl --fail-with-body \
   -s \
   -H "Authorization: Bearer ${TOKEN}" \
   -H 'Accept: application/json' \
   -H 'Content-Type: application/json' \
   -H "Polaris-Realm: $realm" \
   http://polaris:8181/api/management/v1/principals \
   -d "{\"principal\": {\"name\": \"$principal_name\"}}" -v

echo
echo Granting principal role $role_name to $principal_name in realm $realm...

curl --fail-with-body \
   -s \
   -X PUT \
   -H "Authorization: Bearer ${TOKEN}" \
   -H 'Accept: application/json' \
   -H 'Content-Type: application/json' \
   -H "Polaris-Realm: $realm" \
   http://polaris:8181/api/management/v1/principals/$principal_name/principal-roles \
   -d "{\"principalRole\": {\"name\": \"$role_name\"}}" -v

echo
echo Done.
