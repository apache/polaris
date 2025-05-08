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
Title: Quickstart
type: docs
weight: 200
---

Polaris can be deployed via a docker image or as a standalone process. Before starting, be sure that you've satisfied the relevant prerequisites detailed in the previous page.

## Docker Image

To start using Polaris in Docker, build and launch Polaris, which is packaged with a Postgres instance, Apache Spark, and Trino. 

```shell
cd ~/polaris
./gradlew \
  :polaris-quarkus-server:assemble \
  :polaris-quarkus-server:quarkusAppPartsBuild \
  :polaris-quarkus-admin:assemble --rerun \
  -Dquarkus.container-image.tag=postgres-latest \
  -Dquarkus.container-image.build=true
docker compose -f getting-started/eclipselink/docker-compose-postgres.yml -f getting-started/eclipselink/docker-compose-bootstrap-db.yml -f getting-started/eclipselink/docker-compose.yml up
```

You should see output for some time as Polaris, Spark, and Trino build and start up. Eventually, you won’t see any more logs and see some logs relating to Spark, resembling the following:

```
spark-sql-1          | Spark Web UI available at http://8bc4de8ed854:4040
spark-sql-1          | Spark master: local[*], Application Id: local-1743745174604
spark-sql-1          | 25/04/04 05:39:38 WARN SparkSQLCLIDriver: WARNING: Directory for Hive history file: /home/spark does not exist.   History will not be available during this session.
spark-sql-1          | 25/04/04 05:39:39 WARN RESTSessionCatalog: Iceberg REST client is missing the OAuth2 server URI configuration and defaults to http://polaris:8181/api/catalogv1/oauth/tokens. This automatic fallback will be removed in a future Iceberg release.It is recommended to configure the OAuth2 endpoint using the 'oauth2-server-uri' property to be prepared. This warning will disappear if the OAuth2 endpoint is explicitly configured. See https://github.com/apache/iceberg/issues/10537
```

Finally, set the following static credentials for interacting with the Polaris server in the following exercises:

```shell
export CLIENT_ID=root
export CLIENT_SECRET=s3cr3t
```

The Docker image pre-configures a sample catalog called `quickstart_catalog` that uses a local file system.

## Running Polaris as a Standalone Process

You can also start Polaris through Gradle (packaged within the Polaris repository):

```shell
cd ~/polaris
# Build the server
./gradlew clean :polaris-quarkus-server:assemble :polaris-quarkus-server:quarkusAppPartsBuild --rerun
# Start the server
./gradlew run
```

You should see output for some time as Polaris builds and starts up. Eventually, you won’t see any more logs and should see messages that resemble the following:

```
INFO  [io.quarkus] [,] [,,,] (Quarkus Main Thread) polaris-quarkus-service <version> on JVM (powered by Quarkus <version>) started in 2.656s. Listening on: http://localhost:8181. Management interface listening on http://0.0.0.0:8182.
INFO  [io.quarkus] [,] [,,,] (Quarkus Main Thread) Profile prod activated. Live Coding activated.
INFO  [io.quarkus] [,] [,,,] (Quarkus Main Thread) Installed features: [...]
```

At this point, Polaris is running.

When using a Gradle-launched Polaris instance in this tutorial, we'll launch an instance of Polaris that stores entities only in-memory. This means that any entities that you define will be destroyed when Polaris is shut down. 
For more information on how to configure Polaris for production usage, see the [docs]({{% relref "../configuring-polaris-for-production" %}}).

When Polaris is run using the `./gradlew run` command, the root principal credentials are `root` and `secret` for the `CLIENT_ID` and `CLIENT_SECRET`, respectively.
You can also set these credentials as environment variables for use with the Polaris CLI:
```shell
export CLIENT_ID=root
export CLIENT_SECRET=secret
```

### Installing Apache Spark and Trino Locally for Testing

#### Apache Spark

If you want to connect to Polaris with [Apache Spark](https://spark.apache.org/), you'll need to start by cloning Spark. As in the [prerequisites]({{% ref "install-dependencies#git" %}}), make sure [git](https://git-scm.com/) is installed first.

Then, clone Spark and check out a versioned branch. This guide uses [Spark 3.5](https://spark.apache.org/releases/spark-release-3-5-0.html).

```shell
cd ~
git clone https://github.com/apache/spark.git
cd ~/spark
git checkout branch-3.5
```

#### Trino
If you want to connect to Polaris with [Trino](https://trino.io/), it is recommended to set up a test instance of Trino using Docker. As in the [prerequisites]({{% ref "install-dependencies#docker" %}}), make sure [Docker](https://www.docker.com/) is installed first

```shell
docker run --name trino -d -p 8080:8080 trinodb/trino
```

## Next Steps
Congrats, you now have a running instance of Polaris! For further information regarding how to use Polaris, check out the [Using Polaris]({{% ref "using-polaris" %}}) page. 