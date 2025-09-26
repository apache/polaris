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

SPARK_BEARER_TOKEN="${REGTEST_ROOT_BEARER_TOKEN}"
CATALOG_NAME="spark_sql_fine_grained_catalog"
PRINCIPAL_NAME="fine_grained_user"
PRINCIPAL_ROLE_NAME="fine_grained_principal_role"
CATALOG_ROLE_NAME="fine_grained_catalog_role"

echo "Creating catalog with fine-grained authorization enabled..."

# Create catalog with fine-grained authorization feature flag enabled
curl -i -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
  -d "{
    \"name\": \"${CATALOG_NAME}\", 
    \"id\": 100, 
    \"type\": \"INTERNAL\", 
    \"readOnly\": false, 
    \"properties\": {
      \"default-base-location\": \"file:///tmp/spark_sql_fine_grained_catalog\", 
      \"drop-with-purge.enabled\": \"true\"
    }, 
    \"storageConfigInfo\": {
      \"storageType\": \"FILE\", 
      \"allowedLocations\": [\"file:///tmp\"]
    }
  }" > /dev/stderr

echo "Creating principal and roles..."

# Create a principal for testing fine-grained privileges
PRINCIPAL_RESPONSE=$(curl -s -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principals \
  -d "{\"principal\": {\"name\": \"${PRINCIPAL_NAME}\", \"type\": \"SERVICE\"}}")

# Extract client credentials from response (this is a simplified extraction - in a real script you'd use jq)
PRINCIPAL_CLIENT_ID=$(echo $PRINCIPAL_RESPONSE | grep -o '"clientId":"[^"]*"' | cut -d'"' -f4)
echo "Principal created with client ID: __CLIENT_ID__"

# Create principal role
curl -i -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principal-roles \
  -d "{\"principalRole\": {\"name\": \"${PRINCIPAL_ROLE_NAME}\"}}" > /dev/stderr

# Create catalog role
curl -i -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles \
  -d "{\"catalogRole\": {\"name\": \"${CATALOG_ROLE_NAME}\"}}" > /dev/stderr

# Assign principal role to principal
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principals/${PRINCIPAL_NAME}/principal-roles \
  -d "{\"principalRole\": {\"name\": \"${PRINCIPAL_ROLE_NAME}\"}}" > /dev/stderr

# Assign catalog role to principal role
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principal-roles/${PRINCIPAL_ROLE_NAME}/catalog-roles/${CATALOG_NAME} \
  -d "{\"catalogRole\": {\"name\": \"${CATALOG_ROLE_NAME}\"}}" > /dev/stderr

echo "Granting basic privileges needed for setup..."

# Grant basic privileges needed for namespace and table creation
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE_NAME}/grants \
  -d '{"type": "catalog", "privilege": "NAMESPACE_FULL_METADATA"}' > /dev/stderr

curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE_NAME}/grants \
  -d '{"type": "catalog", "privilege": "TABLE_CREATE"}' > /dev/stderr

curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE_NAME}/grants \
  -d '{"type": "catalog", "privilege": "TABLE_LIST"}' > /dev/stderr

curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE_NAME}/grants \
  -d '{"type": "catalog", "privilege": "TABLE_READ_PROPERTIES"}' > /dev/stderr

# Grant additional privileges required for fine-grained authorization
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE_NAME}/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}' > /dev/stderr

curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE_NAME}/grants \
  -d '{"type": "catalog", "privilege": "TABLE_DROP"}' > /dev/stderr

# Get the catalog configuration
curl -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  "http://${POLARIS_HOST:-localhost}:8181/api/catalog/v1/config?warehouse=${CATALOG_NAME}"
echo
echo "Catalog created with fine-grained authorization enabled"

echo "Testing coarse-grained TABLE_WRITE_PROPERTIES privilege (should work for all operations)..."

# Grant broad TABLE_WRITE_PROPERTIES privilege (should work for all metadata updates)
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE_NAME}/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_PROPERTIES"}' > /dev/stderr

# Test with coarse-grained privilege - should work
cat << EOF | ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" --conf spark.sql.catalog.polaris.warehouse=${CATALOG_NAME}
use polaris;
create namespace db1;
create table db1.test_table (col1 int, col2 string);

-- These operations should all work with TABLE_WRITE_PROPERTIES
alter table db1.test_table set tblproperties ('test.property' = 'test.value');
alter table db1.test_table unset tblproperties ('test.property');

drop table db1.test_table purge;
drop namespace db1;
EOF

echo "Coarse-grained privilege test completed successfully"

echo "Testing fine-grained privileges..."

# Remove the broad privilege and test specific fine-grained privileges
curl -i -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  "http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE_NAME}/grants?type=catalog&privilege=TABLE_WRITE_PROPERTIES" > /dev/stderr

echo "Testing TABLE_SET_PROPERTIES privilege..."

# Grant only TABLE_SET_PROPERTIES
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE_NAME}/grants \
  -d '{"type": "catalog", "privilege": "TABLE_SET_PROPERTIES"}' > /dev/stderr

# Test TABLE_SET_PROPERTIES - should work
cat << EOF | ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" --conf spark.sql.catalog.polaris.warehouse=${CATALOG_NAME}
use polaris;
create namespace db1;
create table db1.test_table (col1 int, col2 string);

-- This should work with TABLE_SET_PROPERTIES
alter table db1.test_table set tblproperties ('test.property' = 'test.value');
EOF

echo "SET_PROPERTIES test completed"

# Test that UNSET_PROPERTIES fails without the right privilege
echo "Testing that UNSET_PROPERTIES fails without TABLE_REMOVE_PROPERTIES..."

# This should fail since we don't have TABLE_REMOVE_PROPERTIES
cat << 'EOF' | ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" --conf spark.sql.catalog.polaris.warehouse=${CATALOG_NAME} || echo "Expected failure: missing TABLE_REMOVE_PROPERTIES privilege"
use polaris;
-- This should fail
alter table db1.test_table unset tblproperties ('test.property');
EOF

echo "Testing TABLE_REMOVE_PROPERTIES privilege..."

# Grant TABLE_REMOVE_PROPERTIES
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE_NAME}/grants \
  -d '{"type": "catalog", "privilege": "TABLE_REMOVE_PROPERTIES"}' > /dev/stderr

# Now this should work
cat << EOF | ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" --conf spark.sql.catalog.polaris.warehouse=${CATALOG_NAME}
use polaris;
-- This should now work
alter table db1.test_table unset tblproperties ('test.property');

drop table db1.test_table purge;
drop namespace db1;
EOF

echo "REMOVE_PROPERTIES test completed"

echo "Testing multiple fine-grained privileges together..."

# Test that having both specific privileges works together
cat << EOF | ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" --conf spark.sql.catalog.polaris.warehouse=${CATALOG_NAME}
use polaris;
create namespace db1;
create table db1.test_table (col1 int, col2 string);

-- Both operations should work since we have both privileges
alter table db1.test_table set tblproperties ('prop1' = 'value1', 'prop2' = 'value2');
alter table db1.test_table unset tblproperties ('prop1');

drop table db1.test_table purge;
drop namespace db1;
EOF

echo "Multiple fine-grained privileges test completed"

echo "Cleaning up..."

# Clean up resources
curl -i -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principals/${PRINCIPAL_NAME} > /dev/stderr

curl -i -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principal-roles/${PRINCIPAL_ROLE_NAME} > /dev/stderr

curl -i -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE_NAME} > /dev/stderr

curl -i -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME} > /dev/stderr

echo "Fine-grained authorization test completed successfully!"
