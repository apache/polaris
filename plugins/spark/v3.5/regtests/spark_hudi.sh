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

# Determine Scala version (default to 2.12 if not set)
SCALA_VERSION="${SCALA_VERSION:-2.12}"

CATALOG_NAME="spark_hudi_catalog"
curl -i -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
  -d '{"name": "spark_hudi_catalog", "id": 200, "type": "INTERNAL", "readOnly": false, "properties": {"default-base-location": "file:///tmp/spark_hudi_catalog"}, "storageConfigInfo": {"storageType": "FILE", "allowedLocations": ["file:///tmp"]}}' > /dev/stderr

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
create namespace hudi_db1;
create namespace hudi_db2;
show namespaces;

create namespace hudi_db1.schema1;
show namespaces in hudi_db1;

create table hudi_db1.schema1.hudi_tb1 (id int, name string) using hudi location 'file:///tmp/spark_hudi_catalog/hudi_tb1';
show tables in hudi_db1;
show tables in hudi_db1.schema1;

use hudi_db1.schema1;
insert into hudi_tb1 values (1, 'alice'), (2, 'bob');
select * from hudi_tb1 order by id;

create table hudi_tb2 (name string, age int, country string) using hudi partitioned by (country) location 'file:///tmp/spark_hudi_catalog/hudi_tb2';
insert into hudi_tb2 values ('anna', 10, 'US'), ('james', 32, 'US'), ('yan', 16, 'CHINA');
select name, country from hudi_tb2 order by age;

show tables;

use hudi_db1;
create table iceberg_tb (col1 int);
insert into iceberg_tb values (100), (200);
select * from iceberg_tb order by col1;

show tables;
show tables in hudi_db1.schema1;

drop table hudi_db1.schema1.hudi_tb1;
drop table hudi_db1.schema1.hudi_tb2;
drop namespace hudi_db1.schema1;
drop table iceberg_tb;
drop namespace hudi_db1;
drop namespace hudi_db2;
EOF

# clean up the spark_hudi_catalog dir
rm -rf /tmp/spark_hudi_catalog/

curl -i -X DELETE -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/${CATALOG_NAME} > /dev/stderr