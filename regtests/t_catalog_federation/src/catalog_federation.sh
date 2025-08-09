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

# This test creates an INTERNAL catalog and an EXTERNAL catalog with passthrough facade
# to demonstrate true catalog federation.

set -e


SPARK_BEARER_TOKEN="${REGTEST_ROOT_BEARER_TOKEN}"

echo "=== Setting up Catalog Federation Test ==="

# Step 1: Create a new principal
echo "Creating new principal..."
PRINCIPAL_RESPONSE=$(curl -s -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principals \
  -d '{
    "principal": {
      "name": "new-user"
    }
  }')

NEW_CLIENT_ID=$(echo "$PRINCIPAL_RESPONSE" | jq -r '.credentials.clientId')
NEW_CLIENT_SECRET=$(echo "$PRINCIPAL_RESPONSE" | jq -r '.credentials.clientSecret')

# Step 2: Create local catalog
echo "Creating local catalog..."
RESPONSE_CODE=$(curl -s -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
  -d '{
    "type": "INTERNAL",
    "name": "test-catalog-local",
    "properties": {
      "default-base-location": "file:///tmp/warehouse"
    },
    "storageConfigInfo": {
      "storageType": "FILE",
      "allowedLocations": ["file:///tmp/warehouse"]
    }
  }' \
  --write-out "%{http_code}")
echo "Create local catalog response code: $RESPONSE_CODE"



# Step 3: Grant permissions
echo "Setting up permissions..."

# Grant TABLE_WRITE_DATA privilege to catalog_admin for local catalog
RESPONSE_CODE=$(curl -s -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/test-catalog-local/catalog-roles/catalog_admin/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}' \
  --write-out "%{http_code}")
echo "Grant TABLE_WRITE_DATA to catalog_admin response code: $RESPONSE_CODE"

# Assign catalog_admin to service_admin
RESPONSE_CODE=$(curl -s -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principal-roles/service_admin/catalog-roles/test-catalog-local \
  -d '{"name": "catalog_admin"}' \
  --write-out "%{http_code}")
echo "Assign catalog_admin to service_admin response code: $RESPONSE_CODE"

# Assign service_admin to new-user
RESPONSE_CODE=$(curl -s -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principals/new-user/principal-roles \
  -d '{"name": "service_admin"}' \
  --write-out "%{http_code}")
echo "Assign service_admin to new-user response code: $RESPONSE_CODE"

# Step 4: Create external catalog
echo "Creating external catalog (passthrough facade)..."
RESPONSE_CODE=$(curl -s -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
  -d "{
    \"type\": \"EXTERNAL\",
    \"name\": \"test-catalog-external\",
    \"connectionConfigInfo\": {
      \"connectionType\": \"ICEBERG_REST\",
      \"uri\": \"http://${POLARIS_HOST:-localhost}:8181/api/catalog\",
      \"remoteCatalogName\": \"test-catalog-local\",
      \"authenticationParameters\": {
        \"authenticationType\": \"OAUTH\",
        \"tokenUri\": \"http://${POLARIS_HOST:-localhost}:8181/api/catalog/v1/oauth/tokens\",
        \"clientId\": \"${NEW_CLIENT_ID}\",
        \"clientSecret\": \"${NEW_CLIENT_SECRET}\",
        \"scopes\": [\"PRINCIPAL_ROLE:ALL\"]
      }
    },
    \"properties\": {
      \"default-base-location\": \"file:///tmp/warehouse\"
    },
    \"storageConfigInfo\": {
      \"storageType\": \"FILE\",
      \"allowedLocations\": [\"file:///tmp/warehouse\"]
    }
  }" \
  --write-out "%{http_code}")
echo "Create external catalog response code: $RESPONSE_CODE"

# Step 5: Grant permissions for external catalog
echo "Setting up permissions for external catalog..."

# Grant TABLE_WRITE_DATA privilege to catalog_admin role for test-catalog-external
RESPONSE_CODE=$(curl -s -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/test-catalog-external/catalog-roles/catalog_admin/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}' \
  --write-out "%{http_code}")
echo "Grant TABLE_WRITE_DATA to external catalog_admin response code: $RESPONSE_CODE"

# Assign catalog_admin role to service_admin principal-role for test-catalog-external
RESPONSE_CODE=$(curl -s -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principal-roles/service_admin/catalog-roles/test-catalog-external \
  -d '{"name": "catalog_admin"}' \
  --write-out "%{http_code}")
echo "Assign catalog_admin to service_admin for external catalog response code: $RESPONSE_CODE"

echo "Catalogs created successfully"

echo ""
echo "=== Starting federation test ==="

# Test data operations via local catalog
echo "=== Creating data via LOCAL catalog ==="
cat << EOF | ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" --conf spark.sql.catalog.polaris.warehouse=test-catalog-local --conf spark.sql.defaultCatalog=polaris --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
use polaris;
create namespace if not exists ns1;
create table if not exists ns1.test_table (id int, name string);
insert into ns1.test_table values (1, 'Alice');
insert into ns1.test_table values (2, 'Bob');
create namespace if not exists ns2;
create table if not exists ns2.test_table (id int, name string);
insert into ns2.test_table values (1, 'Apache Spark');
insert into ns2.test_table values (2, 'Apache Iceberg');
EOF

echo ""
echo "=== Accessing data via EXTERNAL catalog ==="
cat << EOF | ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" --conf spark.sql.catalog.polaris.warehouse=test-catalog-external --conf spark.sql.defaultCatalog=polaris --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
use polaris;
show namespaces;
select * from ns1.test_table order by id;
insert into ns1.test_table values (3, 'Charlie');
select * from ns2.test_table order by id;
insert into ns2.test_table values (3, 'Apache Polaris');
EOF

echo ""
echo "=== Verifying federation via LOCAL catalog ==="
cat << EOF | ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" --conf spark.sql.catalog.polaris.warehouse=test-catalog-local --conf spark.sql.defaultCatalog=polaris --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
use polaris;
select * from ns1.test_table order by id;
select * from ns2.test_table order by id;
drop table ns1.test_table;
drop table ns2.test_table;
drop namespace ns1;
drop namespace ns2;
EOF

echo ""
echo "=== Cleaning up catalogs and principal ==="
# Clean up catalogs
RESPONSE_CODE=$(curl -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/test-catalog-external \
  --write-out "%{http_code}")
echo "Delete external catalog response code: $RESPONSE_CODE"

RESPONSE_CODE=$(curl -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/test-catalog-local \
  --write-out "%{http_code}")
echo "Delete local catalog response code: $RESPONSE_CODE"

# Clean up principal
RESPONSE_CODE=$(curl -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principals/new-user \
  --write-out "%{http_code}")
echo "Delete principal response code: $RESPONSE_CODE"

echo "Catalog federation test completed successfully!"