---
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
Title: Using Polaris
type: docs
weight: 400
---

## Setup

Ensure your `CLIENT_ID` & `CLIENT_SECRET` variables are already defined, as they were required for starting the Polaris server earlier.

```shell
export CLIENT_ID=YOUR_CLIENT_ID
export CLIENT_SECRET=YOUR_CLIENT_SECRET
```

## Defining a Catalog

In Polaris, the [catalog]({{% relref "../entities#catalog" %}}) is the top-level entity that objects like [tables]({{% relref "../entities#table" %}}) and [views]({{% relref "../entities#view" %}}) are organized under. With a Polaris service running, you can create a catalog like so:

```shell
cd ~/polaris

./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalogs \
  create \
  --storage-type s3 \
  --default-base-location ${DEFAULT_BASE_LOCATION} \
  --role-arn ${ROLE_ARN} \
  quickstart_catalog
```

This will create a new catalog called **quickstart_catalog**. If you are using one of the Getting Started locally-built Docker images, we have already created a catalog named `quickstart_catalog` for you.

The `DEFAULT_BASE_LOCATION` you provide will be the default location that objects in this catalog should be stored in, and the `ROLE_ARN` you provide should be a [Role ARN](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html) with access to read and write data in that location. These credentials will be provided to engines reading data from the catalog once they have authenticated with Polaris using credentials that have access to those resources.

If you’re using a storage type other than S3, such as Azure, you’ll provide a different type of credential than a Role ARN. For more details on supported storage types, see the [docs]({{% relref "../entities#storage-type" %}}).

Additionally, if Polaris is running somewhere other than `localhost:8181`, you can specify the correct hostname and port by providing `--host` and `--port` flags. For the full set of options supported by the CLI, please refer to the [docs]({{% relref "../command-line-interface" %}}).


### Creating a Principal and Assigning it Privileges

With a catalog created, we can create a [principal]({{% relref "../entities#principal" %}}) that has access to manage that catalog. For details on how to configure the Polaris CLI, see [the section above](#defining-a-catalog) or refer to the [docs]({{% relref "../command-line-interface" %}}).

```shell
./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  principals \
  create \
  quickstart_user

./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  principal-roles \
  create \
  quickstart_user_role

./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalog-roles \
  create \
  --catalog quickstart_catalog \
  quickstart_catalog_role
```

Be sure to provide the necessary credentials, hostname, and port as before.

When the `principals create` command completes successfully, it will return the credentials for this new principal. Export them for future use. For example:

```shell
./polaris ... principals create example
{"clientId": "XXXX", "clientSecret": "YYYY"}
export USER_CLIENT_ID=XXXX
export USER_CLIENT_SECRET=YYYY
```

Now, we grant the principal the [principal role]({{% relref "../entities#principal-role" %}}) we created, and grant the [catalog role]({{% relref "../entities#catalog-role" %}}) the principal role we created. For more information on these entities, please refer to the linked documentation.

```shell
./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  principal-roles \
  grant \
  --principal quickstart_user \
  quickstart_user_role

./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalog-roles \
  grant \
  --catalog quickstart_catalog \
  --principal-role quickstart_user_role \
  quickstart_catalog_role
```

Now, we’ve linked our principal to the catalog via roles like so:

![Principal to Catalog](/img/quickstart/privilege-illustration-1.png "Principal to Catalog")

In order to give this principal the ability to interact with the catalog, we must assign some [privileges]({{% relref "../entities#privilege" %}}). For the time being, we will give this principal the ability to fully manage content in our new catalog. We can do this with the CLI like so:

```shell
./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  privileges \
  catalog \
  grant \
  --catalog quickstart_catalog \
  --catalog-role quickstart_catalog_role \
  CATALOG_MANAGE_CONTENT
```

This grants the [catalog privileges]({{% relref "../entities#privilege" %}}) `CATALOG_MANAGE_CONTENT` to our catalog role, linking everything together like so:

![Principal to Catalog with Catalog Role](/img/quickstart/privilege-illustration-2.png "Principal to Catalog with Catalog Role")

`CATALOG_MANAGE_CONTENT` has create/list/read/write privileges on all entities within the catalog. The same privilege could be granted to a namespace, in which case the principal could create/list/read/write any entity under that namespace.

## Using Iceberg & Polaris

At this point, we’ve created a principal and granted it the ability to manage a catalog. We can now use an external engine to assume that principal, access our catalog, and store data in that catalog using [Apache Iceberg](https://iceberg.apache.org/). Polaris is compatible with any [Apache Iceberg](https://iceberg.apache.org/) client that supports the REST API. Depending on the client you plan to use, refer to the respective examples below.

### Connecting with Spark

#### Using a Local Build of Spark

To use a Polaris-managed catalog in [Apache Spark](https://spark.apache.org/), we can configure Spark to use the Iceberg catalog REST API.

This guide uses [Apache Spark 3.5](https://spark.apache.org/releases/spark-release-3-5-0.html), but be sure to find [the appropriate iceberg-spark package for your Spark version](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark). From a local Spark clone on the `branch-3.5` branch we can run the following:

_Note: the credentials provided here are those for our principal, not the root credentials._

```shell
bin/spark-sql \
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,org.apache.iceberg:iceberg-aws-bundle:1.9.1 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.quickstart_catalog.warehouse=quickstart_catalog \
--conf spark.sql.catalog.quickstart_catalog.header.X-Iceberg-Access-Delegation=vended-credentials \
--conf spark.sql.catalog.quickstart_catalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.quickstart_catalog.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
--conf spark.sql.catalog.quickstart_catalog.uri=http://localhost:8181/api/catalog \
--conf spark.sql.catalog.quickstart_catalog.credential='${USER_CLIENT_ID}:${USER_CLIENT_SECRET}' \
--conf spark.sql.catalog.quickstart_catalog.scope='PRINCIPAL_ROLE:ALL' \
--conf spark.sql.catalog.quickstart_catalog.token-refresh-enabled=true \
--conf spark.sql.catalog.quickstart_catalog.client.region=us-west-2
```

Similar to the CLI commands above, this configures Spark to use the Polaris running at `localhost:8181`. If your Polaris server is running elsewhere, but sure to update the configuration appropriately.

Finally, note that we include the `iceberg-aws-bundle` package here. If your table is using a different filesystem, be sure to include the appropriate dependency.

#### Using Spark SQL from a Docker container

Refresh the Docker container with the user's credentials:
```shell
docker compose -p polaris -f getting-started/eclipselink/docker-compose.yml stop spark-sql
docker compose -p polaris -f getting-started/eclipselink/docker-compose.yml rm -f spark-sql
docker compose -p polaris -f getting-started/eclipselink/docker-compose.yml up -d --no-deps spark-sql
```

Attach to the running spark-sql container:

```shell
docker attach $(docker ps -q --filter name=spark-sql)
```

#### Sample Commands

Once the Spark session starts, we can create a namespace and table within the catalog:

```sql
USE quickstart_catalog;
CREATE NAMESPACE IF NOT EXISTS quickstart_namespace;
CREATE NAMESPACE IF NOT EXISTS quickstart_namespace.schema;
USE NAMESPACE quickstart_namespace.schema;
CREATE TABLE IF NOT EXISTS quickstart_table (id BIGINT, data STRING) USING ICEBERG;
```

We can now use this table like any other:

```
INSERT INTO quickstart_table VALUES (1, 'some data');
SELECT * FROM quickstart_table;
. . .
+---+---------+
|id |data     |
+---+---------+
|1  |some data|
+---+---------+
```

If at any time access is revoked...

```shell
./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  privileges \
  catalog \
  revoke \
  --catalog quickstart_catalog \
  --catalog-role quickstart_catalog_role \
  CATALOG_MANAGE_CONTENT
```

Spark will lose access to the table:

```
INSERT INTO quickstart_table VALUES (1, 'some data');

org.apache.iceberg.exceptions.ForbiddenException: Forbidden: Principal 'quickstart_user' with activated PrincipalRoles '[]' and activated grants via '[quickstart_catalog_role, quickstart_user_role]' is not authorized for op LOAD_TABLE_WITH_READ_DELEGATION
```

### Connecting with Trino

Refresh the Docker container with the user's credentials:

```shell
docker compose -p polaris -f getting-started/eclipselink/docker-compose.yml stop trino
docker compose -p polaris -f getting-started/eclipselink/docker-compose.yml rm -f trino
docker compose -p polaris -f getting-started/eclipselink/docker-compose.yml up -d --no-deps trino
```

Attach to the running Trino container:

```shell
docker exec -it $(docker ps -q --filter name=trino) trino
```

You may not see Trino's prompt immediately, type ENTER to see it. A few commands that you can try:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
CREATE SCHEMA iceberg.quickstart_schema;
CREATE TABLE iceberg.quickstart_schema.quickstart_table AS SELECT 1 x;
SELECT * FROM iceberg.quickstart_schema.quickstart_table;
```

If at any time access is revoked...

```shell
./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  privileges \
  catalog \
  revoke \
  --catalog quickstart_catalog \
  --catalog-role quickstart_catalog_role \
  CATALOG_MANAGE_CONTENT
```

Trino will lose access to the table:

```sql
SELECT * FROM iceberg.quickstart_schema.quickstart_table;

org.apache.iceberg.exceptions.ForbiddenException: Forbidden: Principal 'quickstart_user' with activated PrincipalRoles '[]' and activated grants via '[quickstart_catalog_role, quickstart_user_role]' is not authorized for op LOAD_TABLE_WITH_READ_DELEGATION
```

### Connecting Using REST APIs

To access Polaris from the host machine, first request an access token:

```shell
export POLARIS_TOKEN=$(curl -s http://polaris:8181/api/catalog/v1/oauth/tokens \
   --resolve polaris:8181:127.0.0.1 \
   --user ${CLIENT_ID}:${CLIENT_SECRET} \
   -d 'grant_type=client_credentials' \
   -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r .access_token)
```

Then, use the access token in the Authorization header when accessing Polaris:

```shell
curl -v http://127.0.0.1:8181/api/management/v1/principal-roles -H "Authorization: Bearer $POLARIS_TOKEN"
curl -v http://127.0.0.1:8181/api/management/v1/catalogs/quickstart_catalog -H "Authorization: Bearer $POLARIS_TOKEN"
```

## Next Steps
* Visit [Configuring Polaris for Production]({{% relref "../configuring-polaris-for-production" %}}).
* A Getting Started experience for using Spark with Jupyter Notebooks is documented [here](https://github.com/apache/polaris/blob/main/getting-started/spark/README.md).
* To shut down a locally-deployed Polaris server and clean up all related Docker containers, run the command listed below. Cloud Deployments have their respective termination commands on their Deployment page, while Polaris running on Gradle will terminate when the Gradle process terminates.
```shell
docker compose -p polaris \
  -f getting-started/assets/postgres/docker-compose-postgres.yml \
  -f getting-started/jdbc/docker-compose-bootstrap-db.yml \
  -f getting-started/jdbc/docker-compose.yml \
  down
```
