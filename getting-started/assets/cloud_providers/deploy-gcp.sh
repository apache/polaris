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

CURRENT_ZONE=$(curl -H "Metadata-Flavor: Google" "http://169.254.169.254/computeMetadata/v1/instance/zone" | awk -F/ '{print $NF}')
CURRENT_REGION=$(echo $CURRENT_ZONE | sed 's/-[a-z]$//')
VM_INSTANCE_NAME=$(curl -H "Metadata-Flavor: Google" "http://169.254.169.254/computeMetadata/v1/instance/name")
RANDOM_SUFFIX=$(head /dev/urandom | tr -dc 'a-z0-9' | head -c 8)
DB_INSTANCE_NAME="polaris-backend-test-$RANDOM_SUFFIX"

INSTANCE_IP=$(gcloud compute instances describe $VM_INSTANCE_NAME --zone=$CURRENT_ZONE --format="get(networkInterfaces[0].accessConfigs[0].natIP)")


gcloud sql instances create $DB_INSTANCE_NAME \
  --database-version=POSTGRES_17 \
  --region=$CURRENT_REGION \
  --tier=db-perf-optimized-N-4 \
  --edition=ENTERPRISE_PLUS \
  --root-password=postgres \
  --authorized-networks="$INSTANCE_IP/32"

gcloud sql databases create POLARIS --instance=$DB_INSTANCE_NAME

export QUARKUS_DATASOURCE_JDBC_URL=$(printf '%s' "jdbc:postgresql://$POSTGRES_ADDR/POLARIS")
export QUARKUS_DATASOURCE_USERNAME=postgres
export QUARKUS_DATASOURCE_PASSWORD=postgres
echo $QUARKUS_DATASOURCE_JDBC_URL

GCS_BUCKET_NAME="polaris-test-gcs-$RANDOM_SUFFIX"
echo "GCS Bucket Name: $GCS_BUCKET_NAME"

gcloud storage buckets create "gs://$GCS_BUCKET_NAME" --location=$CURRENT_REGION
export STORAGE_LOCATION="gs://$GCS_BUCKET_NAME/quickstart_catalog/"

./gradlew clean :polaris-quarkus-server:assemble :polaris-quarkus-admin:assemble \
       -Dquarkus.container-image.tag=postgres-latest \
       -Dquarkus.container-image.build=true \
       --no-build-cache

docker compose -p polaris -f getting-started/jdbc/docker-compose-bootstrap-db.yml -f getting-started/jdbc/docker-compose.yml up -d
