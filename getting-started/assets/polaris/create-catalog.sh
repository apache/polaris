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

token=$(curl -s http://polaris:8181/api/catalog/v1/oauth/tokens \
  --user root:s3cr3t \
  -d grant_type=client_credentials \
  -d scope=PRINCIPAL_ROLE:ALL | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

if [ -z "${token}" ]; then
  echo "Failed to obtain access token."
  exit 1
fi

echo
echo "Obtained access token: ${token}"

echo
echo Creating a catalog named polaris_demo...

curl -s -H "Authorization: Bearer ${token}" \
   -H 'Accept: application/json' \
   -H 'Content-Type: application/json' \
   http://polaris:8181/api/management/v1/catalogs \
   -d '{
     "catalog": {
       "name": "quickstart_catalog",
       "type": "INTERNAL",
       "readOnly": false,
       "properties": {
         "default-base-location": "file:///tmp/quickstart_catalog/"
       },
       "storageConfigInfo": {
         "storageType": "FILE",
         "allowedLocations": [
           "file:///tmp"
         ]
       }
     }
   }'

echo
echo Done.