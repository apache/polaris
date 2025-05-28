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

DESCRIBE_INSTANCE=$(curl -H Metadata:true "http://169.254.169.254/metadata/instance?api-version=2021-02-01")
CURRENT_RESOURCE_GROUP=$(echo $DESCRIBE_INSTANCE | jq -r '.compute.resourceGroupName')
CURRENT_REGION=$(echo $DESCRIBE_INSTANCE | jq -r '.compute.location')
CURRENT_VM_NAME=$(echo $DESCRIBE_INSTANCE | jq -r '.compute.name')
RANDOM_SUFFIX=$(head /dev/urandom | tr -dc 'a-z0-9' | head -c 8)
INSTANCE_NAME="polaris-backend-test-$RANDOM_SUFFIX"

CREATE_DB_RESPONSE=$(az postgres flexible-server create -l $CURRENT_REGION -g $CURRENT_RESOURCE_GROUP -n $INSTANCE_NAME -u postgres -p postgres -y)

az postgres flexible-server db create -g $CURRENT_RESOURCE_GROUP -s $INSTANCE_NAME -d POLARIS

POSTGRES_ADDR=$(echo $CREATE_DB_RESPONSE | jq -r '.host')
export QUARKUS_DATASOURCE_JDBC_URL=$(printf '%s' "jdbc:postgresql://$POSTGRES_ADDR/POLARIS")
export QUARKUS_DATASOURCE_USERNAME=postgres
export QUARKUS_DATASOURCE_PASSWORD=postgres
echo $QUARKUS_DATASOURCE_JDBC_URL

STORAGE_ACCOUNT_NAME="polaristest$RANDOM_SUFFIX"
STORAGE_CONTAINER_NAME="polaris-test-container-$RANDOM_SUFFIX"

az storage account create \
  --name "$STORAGE_ACCOUNT_NAME" \
  --resource-group "$CURRENT_RESOURCE_GROUP" \
  --location "$CURRENT_REGION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace false

az storage container create \
  --account-name "$STORAGE_ACCOUNT_NAME" \
  --name "$STORAGE_CONTAINER_NAME" \
  --auth-mode login

ASSIGNEE_PRINCIPAL_ID=$(az vm show --name $CURRENT_VM_NAME --resource-group $CURRENT_RESOURCE_GROUP --query identity.principalId -o tsv)
SCOPE=$(az storage account show --name $STORAGE_ACCOUNT_NAME --resource-group $CURRENT_RESOURCE_GROUP --query id -o tsv)
ROLE="Storage Blob Data Contributor"
az role assignment create \
  --assignee $ASSIGNEE_PRINCIPAL_ID \
  --role "$ROLE" \
  --scope "$SCOPE"

export AZURE_TENANT_ID=$(az account show --query tenantId -o tsv)
export STORAGE_LOCATION="abfss://$STORAGE_CONTAINER_NAME@$STORAGE_ACCOUNT_NAME.dfs.core.windows.net/quickstart_catalog"

cat >> getting-started/eclipselink/trino-config/catalog/iceberg.properties << EOF
fs.native-azure.enabled=true
azure.auth-type=DEFAULT
EOF

./gradlew clean :polaris-quarkus-server:assemble :polaris-quarkus-admin:assemble \
       -Dquarkus.container-image.tag=postgres-latest \
       -Dquarkus.container-image.build=true \
       --no-build-cache

docker compose -p polaris -f getting-started/jdbc/docker-compose-bootstrap-db.yml -f getting-started/jdbc/docker-compose.yml up -d
