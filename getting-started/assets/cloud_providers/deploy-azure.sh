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

CURRENT_REGION=$(curl -H Metadata:true "http://169.254.169.254/metadata/instance?api-version=2021-02-01" | jq -r '.compute.location')
CURRENT_RESOURCE_GROUP=$(curl -H Metadata:true "http://169.254.169.254/metadata/instance?api-version=2021-02-01" | jq -r '.compute.resourceGroupName')
RANDOM_SUFFIX=$(head /dev/urandom | tr -dc 'a-z0-9' | head -c 8)
INSTANCE_NAME="polaris-backend-test-$RANDOM_SUFFIX"

CREATE_DB_RESPONSE=$(az postgres flexible-server create -l $CURRENT_REGION -g $CURRENT_RESOURCE_GROUP -n $INSTANCE_NAME -u postgres -p postgres -y)

az postgres flexible-server db create -g $CURRENT_RESOURCE_GROUP -s $INSTANCE_NAME -d POLARIS

POSTGRES_ADDR=$(echo $CREATE_DB_RESPONSE | jq -r '.host')

FULL_POSTGRES_ADDR=$(printf '%s\n' "jdbc:postgresql://$POSTGRES_ADDR:5432/{realm}" | sed 's/[&/\]/\\&/g')
sed -i "/jakarta.persistence.jdbc.url/ s|value=\"[^\"]*\"|value=\"$FULL_POSTGRES_ADDR\"|" "getting-started/assets/eclipselink/persistence.xml"

./gradlew clean :polaris-quarkus-server:assemble :polaris-quarkus-admin:assemble \
       -Dquarkus.container-image.tag=postgres-latest \
       -Dquarkus.container-image.build=true \
       --no-build-cache

docker compose -f getting-started/eclipselink/docker-compose-bootstrap-db.yml -f getting-started/eclipselink/docker-compose.yml up -d