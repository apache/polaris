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

token=$(curl -s http://polaris:8181/api/catalog/v1/oauth/tokens \
  --user ${CLIENT_ID}:${CLIENT_SECRET} \
  -d grant_type=client_credentials \
  -d scope=PRINCIPAL_ROLE:ALL | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

if [ -z "${token}" ]; then
  echo "Failed to obtain access token."
  exit 1
fi

echo
echo "Obtained access token: ${token}"

STORAGE_TYPE="FILE"
if [ -z "${STORAGE_LOCATION}" ]; then
    echo "STORAGE_LOCATION is not set, using FILE storage type"
    STORAGE_LOCATION="file:///var/tmp/quickstart_catalog/"
else
    echo "STORAGE_LOCATION is set to '$STORAGE_LOCATION'"
    if [[ "$STORAGE_LOCATION" == s3* ]]; then
        STORAGE_TYPE="S3"
    elif [[ "$STORAGE_LOCATION" == gs* ]]; then
        STORAGE_TYPE="GCS"
    else
        STORAGE_TYPE="AZURE"
    fi
    echo "Using StorageType: $STORAGE_TYPE"
fi

STORAGE_CONFIG_INFO="{\"storageType\": \"$STORAGE_TYPE\", \"allowedLocations\": [\"$STORAGE_LOCATION\"]}"

if [[ "$STORAGE_TYPE" == "S3" ]]; then
    STORAGE_CONFIG_INFO=$(echo "$STORAGE_CONFIG_INFO" | jq --arg roleArn "$AWS_ROLE_ARN" '. + {roleArn: $roleArn}')
elif [[ "$STORAGE_TYPE" == "AZURE" ]]; then
    STORAGE_CONFIG_INFO=$(echo "$STORAGE_CONFIG_INFO" | jq --arg tenantId "$AZURE_TENANT_ID" '. + {tenantId: $tenantId}')
fi

echo
echo Creating a catalog named quickstart_catalog...

PAYLOAD='{
   "catalog": {
     "name": "quickstart_catalog",
     "type": "INTERNAL",
     "readOnly": false,
     "properties": {
       "default-base-location": "'$STORAGE_LOCATION'"
     },
     "storageConfigInfo": '$STORAGE_CONFIG_INFO'
   }
 }'

echo $PAYLOAD

curl -s -H "Authorization: Bearer ${token}" \
   -H 'Accept: application/json' \
   -H 'Content-Type: application/json' \
   http://polaris:8181/api/management/v1/catalogs \
   -d "$PAYLOAD" -v

echo
echo Done.