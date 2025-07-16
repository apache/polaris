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

# -----------------------------------------------------------------------------
# Purpose: Launch the Spark SQL shell to interact with Polaris.
# -----------------------------------------------------------------------------
#
# Usage:
#   ./run_spark_sql.sh [S3-location AWS-IAM-role]
#
# Description:
#   - Without arguments: Runs against a catalog backed by the local filesystem.
#   - With two arguments: Runs against a catalog backed by AWS S3.
#       - [S3-location]  - The S3 path to use as the default base location for the catalog.
#       - [AWS-IAM-role] - The AWS IAM role for catalog to assume when accessing the S3 location.
#
# Examples:
#   - Run against local filesystem:
#     ./run_spark_sql.sh
#
#   - Run against AWS S3:
#     ./run_spark_sql.sh s3://my-bucket/path arn:aws:iam::123456789001:role/my-role

if [ $# -ne 0 ] && [ $# -ne 2 ]; then
  echo "run_spark_sql.sh only accepts 0 or 2 arguments"
  echo "Usage: ./run_spark_sql.sh [S3-location AWS-IAM-role]"
  exit 1
fi

REGTEST_HOME=$(dirname $(realpath $0))
cd ${REGTEST_HOME}

export SPARK_VERSION=spark-3.5.6
export SPARK_DISTRIBUTION=${SPARK_VERSION}-bin-hadoop3
export SPARK_LOCAL_HOSTNAME=localhost # avoid VPN messing up driver local IP address binding

./setup.sh

if [ -z "${SPARK_HOME}"]; then
  export SPARK_HOME=$(realpath ~/${SPARK_DISTRIBUTION})
fi

if ! output=$(curl -X POST -H "Polaris-Realm: POLARIS" "http://${POLARIS_HOST:-localhost}:8181/api/catalog/v1/oauth/tokens" \
  -d "grant_type=client_credentials" \
  -d "client_id=root" \
  -d "client_secret=s3cr3t" \
  -d "scope=PRINCIPAL_ROLE:ALL"); then
  echo "Error: Failed to retrieve bearer token"
  exit 1
fi

SPARK_BEARER_TOKEN=$(echo "$output" | awk -F\" '{print $4}')

if [ "SPARK_BEARER_TOKEN" == "unauthorized_client" ]; then
  echo "Error: Failed to retrieve bearer token"
  exit 1
fi

if [ $# -eq 0 ]; then
  # create a catalog backed by the local filesystem
  curl -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" \
       -H 'Accept: application/json' \
       -H 'Content-Type: application/json' \
       http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
       -d '{
             "catalog": {
               "name": "manual_spark",
               "type": "INTERNAL",
               "readOnly": false,
               "properties": {
                 "default-base-location": "file:///tmp/polaris/"
               },
               "storageConfigInfo": {
                 "storageType": "FILE",
                 "allowedLocations": [
                   "file:///tmp"
                 ]
               }
             }
           }'

elif [ $# -eq 2 ]; then
  # create a catalog backed by S3
  S3_LOCATION=$1
  AWS_IAM_ROLE=$2

  curl -i -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" \
       -H 'Accept: application/json' \
       -H 'Content-Type: application/json' \
       http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
       -d "{
             \"name\": \"manual_spark\",
             \"id\": 100,
             \"type\": \"INTERNAL\",
             \"readOnly\": false,
             \"properties\": {
               \"default-base-location\": \"${S3_LOCATION}\"
             },
             \"storageConfigInfo\": {
               \"storageType\": \"S3\",
               \"allowedLocations\": [\"${S3_LOCATION}/\"],
               \"roleArn\": \"${AWS_IAM_ROLE}\"
             }
           }"
fi

# Add TABLE_WRITE_DATA to the catalog's catalog_admin role since by default it can only manage access and metadata
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/manual_spark/catalog-roles/catalog_admin/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}' > /dev/stderr

# Assign the catalog_admin to the service_admin.
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principal-roles/service_admin/catalog-roles/manual_spark \
  -d '{"name": "catalog_admin"}' > /dev/stderr

curl -X GET -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/manual_spark

${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" \
  --conf spark.sql.catalog.polaris.warehouse=manual_spark \
  --conf spark.sql.defaultCatalog=polaris \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
