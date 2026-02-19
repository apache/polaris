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

CATALOG_NAME="spark_sql_catalog"
curl -i -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
  -d '{"name": "spark_sql_catalog", "id": 100, "type": "INTERNAL", "readOnly": false, "properties": {"default-base-location": "file:///tmp/spark_catalog"}, "storageConfigInfo": {"storageType": "FILE", "allowedLocations": ["file:///tmp"]}}' > /dev/stderr

# Add TABLE_WRITE_DATA to the catalog's catalog_admin role since by default it can only manage access and metadata
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/catalog_admin/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}' > /dev/stderr

curl -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  "http://${POLARIS_HOST:-localhost}:8181/api/catalog/v1/config?warehouse=${CATALOG_NAME}"
echo
echo "Catalog created"
cat << EOF | ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" --conf spark.sql.catalog.polaris.warehouse=${CATALOG_NAME}
use polaris;
create namespace db1;
create namespace db2;
show namespaces;

create namespace db1.schema1;
show namespaces in db1;

create table db1.schema1.iceberg_tb (col1 int);
show tables in db1;
show tables in db1.schema1;

use db1.schema1;
insert into iceberg_tb values (123), (234), (111);
select * from iceberg_tb order by col1;

create table delta_tb1(col1 string) using delta location 'file:///tmp/spark_catalog/delta_tb1';
insert into delta_tb1 values ('ab'), ('bb'), ('dd');
select * from delta_tb1 order by col1;

show tables;

use db1;
create table delta_tb2(col1 int) using delta location 'file:///tmp/spark_catalog/delta_tb2';
insert into delta_tb2 values (1), (2), (3) order by col1;
select * from delta_tb2;

show tables;
show tables in db1.schema1;

drop table db1.schema1.iceberg_tb;
drop table db1.schema1.delta_tb1;
drop namespace db1.schema1;
drop table delta_tb2;
drop namespace db1;
drop namespace db2;
EOF

# clean up the spark_catalog dir
rm -rf /tmp/spark_catalog/

curl -i -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME} > /dev/stderr
