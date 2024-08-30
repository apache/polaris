<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
   http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Getting Started with Trino and Apache Polaris

This getting started guide provides a docker-compose file to set up [Trino](https://trino.io/) with Apache Polaris. Apache Polaris is configured as an Iceberg REST Catalog in Trino. 

## Run the docker-compose file
To start the docker-compose file, run this command from the repo's root directory:
```
docker-compose -f getting-started/trino/docker-compose-trino.yml up 
```

## Run Trino queries via Trino ClI
To access the Trino CLI, run this command
```
docker exec -it trino-trino-1 trino
```
Note, `trino-trino-1` is the name docker container.

Example Trino queries:
```
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.information_schema;
DESCRIBE iceberg.information_schema.tables;

CREATE SCHEMA iceberg.tpch;
CREATE TABLE iceberg.tpch.test_polaris AS SELECT 1 x;
SELECT * FROM iceberg.tpch.test_polaris;
```

## Note
The polaris catalog setup script use the credential `principal:root;realm:default-realm`. This credential is used so users do not need to fetch credentials from Apache Polaris' console output.

An example catalog is created in Apache Polaris using the `curl` command. See `create-polaris-catalog.sh` for details.
