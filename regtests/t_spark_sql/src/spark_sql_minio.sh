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

#if [ -z "$S3COMPATIBLE_TEST_ENABLED" ] || [ "$S3COMPATIBLE_TEST_ENABLED" != "true" ]; then
#  echo "S3COMPATIBLE_TEST_ENABLED is not set to 'true'. Skipping test."
#  exit 0
#fi
echo "S3COMPATIBLE Starting test. (shell script called minio to avoid to be trapped by AWS test pattern based on s3 word)"

SPARK_BEARER_TOKEN="${REGTEST_ROOT_BEARER_TOKEN}"

curl -i -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
  -d "{\"name\": \"spark_sql_s3compatible_catalog\", \"id\": 100, \"type\": \"INTERNAL\", \"readOnly\": false, \"properties\": {\"default-base-location\": \"s3://warehouse/polaris_test/spark_sql_s3compatible_catalog\"}, \"storageConfigInfo\": {\"storageType\": \"S3_COMPATIBLE\", \"allowedLocations\": [\"s3://warehouse/polaris_test/\"], \"s3.endpoint\": \"http://minio-without-tls:9000\", \"s3.pathStyleAccess\": true, \"s3.credentials.catalog.accessKeyEnvVar\": \"MINIO_S3_CATALOG_1_ID\", \"s3.credentials.catalog.secretAccessKeyEnvVar\": \"MINIO_S3_CATALOG_1_SECRET\" }}" > /dev/stderr


# Add TABLE_WRITE_DATA to the catalog's catalog_admin role since by default it can only manage access and metadata
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/spark_sql_s3compatible_catalog/catalog-roles/catalog_admin/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}' > /dev/stderr

# For now, also explicitly assign the catalog_admin to the service_admin. Remove once GS fully rolled out for auto-assign.
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principal-roles/service_admin/catalog-roles/spark_sql_s3compatible_catalog \
  -d '{"name": "catalog_admin"}' > /dev/stderr

curl -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  "http://${POLARIS_HOST:-localhost}:8181/api/catalog/v1/config?warehouse=spark_sql_s3compatible_catalog"
echo
echo "Catalog created"
cat << EOF | ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" --conf spark.sql.catalog.polaris.warehouse=spark_sql_s3compatible_catalog
use polaris;
show namespaces;
create namespace db1;
create namespace db2;
show namespaces;

create namespace db1.schema1;
show namespaces;
show namespaces in db1;

create table db1.schema1.tbl1 (col1 int);
show tables in db1;
use db1.schema1;

insert into tbl1 values (123), (234);
select * from tbl1;

drop table tbl1 purge;
show tables;
drop namespace db1.schema1;
drop namespace db1;
show namespaces;
drop namespace db2;
show namespaces;
EOF

curl -i -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/spark_sql_s3compatible_catalog > /dev/stderr
