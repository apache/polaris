#!/bin/bash

SPARK_BEARER_TOKEN="${REGTEST_ROOT_BEARER_TOKEN:-principal:root;realm:realm1}"

curl -i -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accepts: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
  -d "{\"name\": \"spark_sql_azure_blob_catalog\", \"id\": 101, \"type\": \"INTERNAL\", \"readOnly\": false, \"properties\": {\"default-base-location\": \"${AZURE_BLOB_TEST_BASE}/polaris-test/spark_sql_blob_catalog/\"}, \"storageConfigInfo\": {\"storageType\": \"AZURE\", \"allowedLocations\": [\"${AZURE_BLOB_TEST_BASE}/polaris-test/spark_sql_blob_catalog2/\"], \"tenantId\": \"${AZURE_TENANT_ID}\"}}" > /dev/stderr

# Add TABLE_WRITE_DATA to the catalog's catalog_admin role since by default it can only manage access and metadata
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accepts: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/spark_sql_azure_blob_catalog/catalog-roles/catalog_admin/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}' > /dev/stderr

# For now, also explicitly assign the catalog_admin to the service_admin. Remove once GS fully rolled out for auto-assign.
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accepts: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principal-roles/service_admin/catalog-roles/spark_sql_azure_blob_catalog \
  -d '{"name": "catalog_admin"}' > /dev/stderr

curl -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accepts: application/json' -H 'Content-Type: application/json' \
  "http://${POLARIS_HOST:-localhost}:8181/api/catalog/v1/config?warehouse=spark_sql_azure_blob_catalog"
echo
echo "Catalog created"
cat << EOF | ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" --conf spark.sql.catalog.polaris.warehouse=spark_sql_azure_blob_catalog;
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

curl -i -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accepts: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/spark_sql_azure_blob_catalog > /dev/stderr
