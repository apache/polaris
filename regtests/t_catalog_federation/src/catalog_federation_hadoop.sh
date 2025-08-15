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

# This test creates a HADOOP catalog via Spark and an EXTERNAL catalog in Polaris 
# that federates to the Hadoop catalog to demonstrate hadoop catalog federation.
# The test relies on the underlying filesystem to create and use the hadoop catalog.

set -e

SPARK_BEARER_TOKEN="${REGTEST_ROOT_BEARER_TOKEN}"

echo "=== Setting up Hadoop Catalog Federation Test ==="

# Verify Hadoop configuration is available
echo "Verifying Hadoop configuration..."
if [ -z "${HADOOP_CONF_DIR}" ]; then
    echo "Error: HADOOP_CONF_DIR is not set"
    exit 1
fi  
if [ ! -f "${HADOOP_CONF_DIR}/core-site.xml" ]; then
    echo "Error: Hadoop configuration not found at: ${HADOOP_CONF_DIR}/core-site.xml"
    exit 1
fi

# Create and verify shared Hadoop warehouse directory is accessible
echo "Creating and verifying shared Hadoop warehouse directory..."
mkdir -p /tmp/hadoop_warehouse
chmod 777 /tmp/hadoop_warehouse 2>/dev/null || true
if [ ! -d "/tmp/hadoop_warehouse" ]; then
    echo "Error: Hadoop warehouse directory not accessible"
    exit 1
fi

# Test file creation permissions
echo "Testing file creation in warehouse..."
touch /tmp/hadoop_warehouse/test_file && rm /tmp/hadoop_warehouse/test_file
echo "File creation test successful"

# Create Hadoop catalog via Spark (not managed by Polaris)
echo "Creating Hadoop catalog via Spark..."
cat << EOF | HADOOP_CONF_DIR=${HADOOP_CONF_DIR} ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.hadoop_catalog.type=hadoop --conf spark.sql.catalog.hadoop_catalog.warehouse=file:///tmp/hadoop_warehouse --conf spark.sql.defaultCatalog=hadoop_catalog
use hadoop_catalog;
create namespace if not exists hadoop_ns1;
create table if not exists hadoop_ns1.hadoop_test_table (id int, name string, source string);
insert into hadoop_ns1.hadoop_test_table values (1, 'Alice', 'Hadoop Direct');
insert into hadoop_ns1.hadoop_test_table values (2, 'Bob', 'Hadoop Direct');
create namespace if not exists hadoop_ns2;
create table if not exists hadoop_ns2.hadoop_test_table (id int, name string, source string);
insert into hadoop_ns2.hadoop_test_table values (1, 'Apache Spark', 'Hadoop Direct');
insert into hadoop_ns2.hadoop_test_table values (2, 'Apache Iceberg', 'Hadoop Direct');
EOF

# Create external catalog in Polaris that federates to the Hadoop catalog
echo ""
echo "Creating external catalog in Polaris (Hadoop federation)..."
CATALOG_RESPONSE=$(curl -s -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
  -d '{
    "type": "EXTERNAL",
    "name": "hadoop-federated-catalog",
    "connectionConfigInfo": {
      "connectionType": "HADOOP",
      "uri": "file:///tmp",
      "warehouse": "file:///tmp/hadoop_warehouse",
      "authenticationParameters": {
        "authenticationType": "IMPLICIT"
      }
    },
    "properties": {
      "default-base-location": "file:///tmp/hadoop_warehouse"
    },
    "storageConfigInfo": {
      "storageType": "FILE",
      "allowedLocations": ["file:///tmp/hadoop_warehouse"]
    }
  }' \
  --write-out "\n%{http_code}")

echo "Create Hadoop federated catalog response: $CATALOG_RESPONSE"

# Grant permissions for external catalog
echo "Setting up permissions for Hadoop federated catalog..."

# Grant TABLE_WRITE_DATA privilege to catalog_admin role for hadoop-federated-catalog
RESPONSE_CODE=$(curl -s -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/hadoop-federated-catalog/catalog-roles/catalog_admin/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}' \
  --write-out "%{http_code}")
echo "Grant TABLE_WRITE_DATA to hadoop catalog_admin response code: $RESPONSE_CODE"

# Assign catalog_admin role to service_admin principal-role for hadoop-federated-catalog
RESPONSE_CODE=$(curl -s -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principal-roles/service_admin/catalog-roles/hadoop-federated-catalog \
  -d '{"name": "catalog_admin"}' \
  --write-out "%{http_code}")
echo "Assign catalog_admin to service_admin for hadoop catalog response code: $RESPONSE_CODE"

echo "Hadoop federated catalog created successfully"

# Verify the catalog was created and is accessible
echo "Verifying catalog creation..."
CATALOG_CHECK=$(curl -s -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/hadoop-federated-catalog \
  --write-out "\n%{http_code}")

CHECK_RESPONSE_CODE=$(echo "$CATALOG_CHECK" | tail -n1)
if [ "$CHECK_RESPONSE_CODE" != "200" ]; then
    echo "Error: Cannot access created catalog. Response code: $CHECK_RESPONSE_CODE"
    echo "Response body: $(echo "$CATALOG_CHECK" | head -n -1)"
    exit 1
fi
echo "Catalog verification successful"

echo ""
echo "=== Accessing Hadoop data via Polaris federated catalog ==="
cat << EOF | ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.polaris.type=rest --conf spark.sql.catalog.polaris.uri=http://${POLARIS_HOST:-localhost}:8181/api/catalog --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" --conf spark.sql.catalog.polaris.warehouse=hadoop-federated-catalog --conf spark.sql.defaultCatalog=polaris --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
use polaris;
select * from hadoop_ns1.hadoop_test_table order by id;
insert into hadoop_ns1.hadoop_test_table values (3, 'Charlie', 'Via Polaris Federation');
select * from hadoop_ns2.hadoop_test_table order by id;
insert into hadoop_ns2.hadoop_test_table values (3, 'Apache Polaris', 'Via Polaris Federation');
EOF

echo ""
echo "=== Verifying federation changes via Hadoop catalog directly ==="
cat << EOF | HADOOP_CONF_DIR=${HADOOP_CONF_DIR} ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.hadoop_catalog.type=hadoop --conf spark.sql.catalog.hadoop_catalog.warehouse=file:///tmp/hadoop_warehouse --conf spark.sql.defaultCatalog=hadoop_catalog
use hadoop_catalog;
select * from hadoop_ns1.hadoop_test_table order by id;
select * from hadoop_ns2.hadoop_test_table order by id;
EOF

echo ""
echo "=== Cleaning up via Hadoop catalog ==="
cat << EOF | HADOOP_CONF_DIR=${HADOOP_CONF_DIR} ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.hadoop_catalog.type=hadoop --conf spark.sql.catalog.hadoop_catalog.warehouse=file:///tmp/hadoop_warehouse --conf spark.sql.defaultCatalog=hadoop_catalog
use hadoop_catalog;
drop table hadoop_ns1.hadoop_test_table;
drop table hadoop_ns2.hadoop_test_table;
drop namespace hadoop_ns1;
drop namespace hadoop_ns2;
EOF

echo ""
echo "=== Cleaning up Polaris federated catalog ==="
# Clean up federated catalog
RESPONSE_CODE=$(curl -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/hadoop-federated-catalog \
  --write-out "%{http_code}")
echo "Delete Hadoop federated catalog response code: $RESPONSE_CODE"

rm -rf /tmp/hadoop_warehouse/*
echo "Hadoop catalog federation test completed successfully!"