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
# Purpose: Launch the Spark SQL shell to interact with Polaris and do NRT.
# -----------------------------------------------------------------------------
#
# Prequisite:
# This script use a MinIO with TLS.
# Please follow instructions in regtests/minio/Readme.md and update your 
# java cacerts with self-signed certificate
#
# Usage:
#   ./run_spark_sql_s3compatible.sh [S3-location]
#
# Description:
#   - Without arguments: Runs against default minio bucket s3://warehouse/polaris
#   - With one arguments: Runs against a catalog backed by minio S3.
#       - [S3-location]  - The S3 path to use as the default base location for the catalog.
#
# Examples:
#   - Run against AWS S3_COMPATIBLE:
#     ./run_spark_sql_s3compatible.sh s3://warehouse/polaris


clear
if [ $# -ne 0 ] && [ $# -ne 1 ]; then
  echo "run_spark_sql_s3compatible.sh only accepts 1 or 0 argument, argument is the the bucket, by default it will be s3://warehouse/polaris"
  echo "Usage: ./run_spark_sql.sh [S3-location]"
  exit 1
fi

# Init
REGTEST_HOME=$(dirname $(realpath $0))
cd ${REGTEST_HOME}


if [ $# -eq 0 ]; then
 echo "creating a catalog backed by S3, default bucket is s3://warehouse/polaris"
 S3_LOCATION="s3://warehouse/polaris"
fi

if [ $# -eq 1 ]; then
  echo "creating a catalog backed by S3 from first arg of this script respecting pattern 's3://mybucket/path'"
  S3_LOCATION=$1
fi
# Second location for testing catalog update
S3_LOCATION_2="s3://warehouse2/polaris/"


# SPARK_BEARER_TOKEN
if ! output=$(curl -s -X POST -H "Polaris-Realm: POLARIS" "http://${POLARIS_HOST:-localhost}:8181/api/catalog/v1/oauth/tokens" \
  -d "grant_type=client_credentials" \
  -d "client_id=root" \
  -d "client_secret=secret" \
  -d "scope=PRINCIPAL_ROLE:ALL"); then
  echo "Error: Failed to retrieve bearer token"
  exit 1
fi
SPARK_BEARER_TOKEN=$(echo "$output" | awk -F\" '{print $4}')
if [ "SPARK_BEARER_TOKEN" == "unauthorized_client" ]; then
  echo "Error: Failed to retrieve bearer token"
  exit 1
fi

# check if Polaris is running
polaris_http_code=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs --output /dev/null)
if [ $polaris_http_code -eq 000 ] && [ $polaris_http_code -ne 200 ]; then
  echo "Polaris is not running on ${POLARIS_HOST:-localhost}:8181. End of script"
  exit 1
fi


# check if cacerts contain MinIO certificate
cert_response=$(keytool -list -cacerts -alias minio -storepass changeit | grep trustedCertEntry)
echo $cert_response
if [ -z "$cert_response" ]; then
  echo "There is no MinIO certificate in your cacerts, please read regtests/minio/Readme.md"
  echo "End of script :-("
 exit 1
fi

# start minio with buckets and users
echo -e "\n\n-------\n\n"
echo "Start a minio with secured self-signed buckets s3://warehouse and users, wait a moment please..."
docker-compose --progress tty --project-name polaris-minio --project-directory minio/ -f minio/docker-compose.yml up -d minio-configured

echo "minio brower is availaible during this test in https://localhost:9001 admin/password (please accept the self signed certificate)"
echo -e "\n\n-------\n\n"

# spark setup
export SPARK_VERSION=spark-3.5.2
export SPARK_DISTRIBUTION=${SPARK_VERSION}-bin-hadoop3

echo "Doing spark setup... wait a moment"
./setup.sh > /dev/null 2>&1

if [ -z "${SPARK_HOME}"]; then
  export SPARK_HOME=$(realpath ~/${SPARK_DISTRIBUTION})
fi




# start of tests

# creation of catalog

echo """
These environnement variables have to be available to Polaris service :
CATALOG_S3_KEY_ID     = minio-user-catalog
CATALOG_S3_KEY_SECRET = 12345678-minio-catalog
export CATALOG_S3_KEY_ID=minio-user-catalog
export CATALOG_S3_KEY_SECRET=12345678-minio-catalog
"""

echo -e "\n----\nCREATE Catalog with few parameters \n"
response_catalog=$(curl  --output /dev/null -w "%{http_code}" -s -i -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" \
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
              \"storageType\": \"S3_COMPATIBLE\",
              \"allowedLocations\": [\"${S3_LOCATION}/\"],
              \"s3.endpoint\": \"https://localhost:9000\"
            }
          }"
)

echo -e "Catalog creation - response API http code : $response_catalog \n"
if [ $response_catalog -ne 201 ] && [ $response_catalog -ne 409 ]; then
  echo "Problem during catalog creation"
  exit 1
fi


echo -e "Get the catalog created : \n"
curl -s -i -X GET -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" \
      -H 'Accept: application/json' \
      -H 'Content-Type: application/json' \
      http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/manual_spark

# Update the catalog
echo -e "\n----\nUPDATE the catalog v1, - adding a second bucket in the alllowed locations\n"
curl -s -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" \
      -H 'Accept: application/json' \
      -H 'Content-Type: application/json' \
      http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/manual_spark \
      -d "{
            \"currentEntityVersion\":1,
            \"properties\": {
              \"default-base-location\": \"${S3_LOCATION}\"
            },
            \"storageConfigInfo\": {
              \"storageType\": \"S3_COMPATIBLE\",
              \"allowedLocations\": [\"${S3_LOCATION}/\",\"${S3_LOCATION_2}/\"],
              \"s3.endpoint\": \"https://localhost:9000\",
              \"s3.region\": \"region-1\",
              \"s3.pathStyleAccess\": true,
              \"s3.credentials.catalog.accessKeyEnvVar\": \"CATALOG_S3_KEY_ID\",
              \"s3.credentials.catalog.secretAccessKeyEnvVar\": \"CATALOG_S3_KEY_SECRET\",
              \"s3.roleArn\": \"arn:xxx:xxx:xxx:xxxx\"
            }
          }"


echo -e "\n----\nAdd TABLE_WRITE_DATA to the catalog's catalog_admin role since by default it can only manage access and metadata\n"
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/manual_spark/catalog-roles/catalog_admin/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}' > /dev/stderr


echo -e "\n----\nAssign the catalog_admin to the service_admin.\n"
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principal-roles/service_admin/catalog-roles/manual_spark \
  -d '{"name": "catalog_admin"}' > /dev/stderr


echo -e "\n----\nStart Spark-sql to test Polaris catalog with queries\n"
${SPARK_HOME}/bin/spark-sql --verbose \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" \
  --conf spark.sql.catalog.polaris.warehouse=manual_spark \
  --conf spark.sql.defaultCatalog=polaris \
  --conf spark.hadoop.hive.cli.print.header=true \
  -f "minio/queries-for-spark.sql"



echo -e "\n\n\nEnd of tests, a table and a view data with displayed should be visible in log above"
echo "Minio stopping, bucket browser will be shutdown, volume data of the bucket remains in 'regtests/minio/miniodata'"
echo ":-)"
echo ""
docker-compose --progress quiet --project-name polaris-minio --project-directory minio/ -f minio/docker-compose.yml down
