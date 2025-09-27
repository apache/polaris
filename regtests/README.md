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

# End-to-end regression tests

Regression tests are either run in Docker, using docker-compose to orchestrate the tests, or
locally.

## Prerequisites

It is recommended to clean the `regtests/output` directory before running tests. This can be done by
running:

```shell
rm -rf ./regtests/output && mkdir -p ./regtests/output && chmod -R 777 ./regtests/output
```

## Run Tests With Docker Compose

Tests can be run with docker-compose using the provided `./regtests/docker-compose.yml` file, as
follows:

```shell
./gradlew \
  :polaris-server:assemble \
  :polaris-server:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true
docker compose -f ./regtests/docker-compose.yml up --build --exit-code-from regtest
```

In this setup, a Polaris container will be started in a docker-compose group, using the image
previously built by the Gradle build. Then another container, including a Spark SQL shell, will run
the tests. The exit code will be the same as the exit code of the Spark container.

This is the flow used in CI and should be done locally before pushing to GitHub to ensure that no
environmental factors contribute to the outcome of the tests.

**Important**: if you are also using minikube, for example to test the Helm chart, you may need to
_unset_ the Docker environment that was pointing to the Minikube Docker daemon, otherwise the image
will be built by the Minikube Docker daemon and will not be available to the local Docker daemon.
This can be done by running, _before_ building the image and running the tests:

```shell
eval $(minikube -p minikube docker-env --unset)
```

## Run Tests Locally

Regression tests can be run locally as well, using the test harness.

In this setup, a Polaris server must be running on localhost:8181 before running tests. The simplest
way to do this is to run the Polaris server in a separate terminal window:

```shell
./gradlew run
```

Note: the regression tests expect Polaris to run with certain options, e.g. with support for `FILE`
storage, default realm `POLARIS` and root credentials `root:secret`; if you run the above command,
this will be the case. If you run Polaris in a different way, make sure that Polaris is configured
appropriately.

Running the test harness will automatically run the idempotent setup script. From the root of the
project, just run:

```shell
env POLARIS_HOST=localhost ./regtests/run.sh
```

The catalog federation tests rely on the following configurations in `application.properties` to
be set in order to succeed.

```
polaris.features."ENABLE_CATALOG_FEDERATION"=true
polaris.features."ALLOW_OVERLAPPING_CATALOG_URLS"=true
```

To run the tests in verbose mode, with test stdout printing to console, set the `VERBOSE`
environment variable to `1`; you can also choose to run only a subset of tests by specifying the
test directories as arguments to `run.sh`. For example, to run only the `t_spark_sql` tests in
verbose mode:

```shell
env VERBOSE=1 POLARIS_HOST=localhost ./regtests/run.sh t_spark_sql/src/spark_sql_basic.sh
```

## Run with Cloud resources
Several tests require access to cloud resources, such as S3 or GCS. To run these tests, you must export the appropriate
environment variables prior to running the tests. Each cloud can be enabled independently.
Create a .env file that contains the following variables:

```
# AWS variables
AWS_TEST_ENABLED=true
AWS_ACCESS_KEY_ID=<your_access_key>
AWS_SECRET_ACCESS_KEY=<your_secret_key>
AWS_STORAGE_BUCKET=<your_s3_bucket>
AWS_ROLE_ARN=<iam_role_with_access_to_bucket>
AWS_TEST_BASE=s3://<your_s3_bucket>/<any_path>

# GCP variables
GCS_TEST_ENABLED=true
GCS_TEST_BASE=gs://<your_gcs_bucket>
GOOGLE_APPLICATION_CREDENTIALS=/tmp/credentials/<your_credentials.json>

# Azure variables
AZURE_TEST_ENABLED=true
AZURE_TENANT_ID=<your_tenant_id>
AZURE_DFS_TEST_BASE=abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<any_path>
AZURE_BLOB_TEST_BASE=abfss://<container-name>@<storage-account-name>.blob.core.windows.net/<any_path>
```
`GOOGLE_APPLICATION_CREDENTIALS` must be mounted to the container volumes. Copy your credentials file
into the `credentials` folder. Then specify the name of the file in your .env file - do not change the
path, as `/tmp/credentials` is the folder on the container where the credentials file will be mounted.


## Fixing a failed test due to incorrect expected output

If a test fails due to incorrect expected output, the test harness will generate a script to help
you compare the actual output with the expected output. The script will be located in the `output`
directory, and will have the same name as the test, with the extension `.fixdiffs.sh`.

For example, if the test `t_hello_world` fails, the script to compare the actual and expected output
will be located at `output/t_hello_world/hello_world.sh.fixdiffs.sh`:

```
Tue Apr 23 06:32:23 UTC 2024: Running all tests
Tue Apr 23 06:32:23 UTC 2024: Starting test t_hello_world:hello_world.sh
Tue Apr 23 06:32:23 UTC 2024: Test run concluded for t_hello_world:hello_world.sh
Tue Apr 23 06:32:23 UTC 2024: Test FAILED: t_hello_world:hello_world.sh
Tue Apr 23 06:32:23 UTC 2024: To compare and fix diffs: /tmp/polaris-regtests/t_hello_world/hello_world.sh.fixdiffs.sh
Tue Apr 23 06:32:23 UTC 2024: Starting test t_spark_sql:spark_sql_basic.sh
Tue Apr 23 06:32:32 UTC 2024: Test run concluded for t_spark_sql:spark_sql_basic.sh
Tue Apr 23 06:32:32 UTC 2024: Test SUCCEEDED: t_spark_sql:spark_sql_basic.sh
```

Simply execute the specified `fixdiffs.sh` file, which will in turn run `meld` and fix the ref file:

```
/tmp/polaris-regtests/t_hello_world/hello_world.sh.fixdiffs.sh
```

Then commit the changes to the ref file.

## Run a spark-sql interactive shell

With a Polaris server running, you can run a spark-sql interactive shell to test. From the root of
the project:

```shell
env POLARIS_HOST=localhost ./regtests/run_spark_sql.sh
```

Some SQL commands that you can try:

```sql
create database db1;
show databases;
create table db1.table1 (id int, name string);
insert into db1.table1 values (1, 'a');
select * from db1.table1;
```

Other commands are available in the `regtests/t_spark_sql/src` directory.

## Python Tests

Python tests are based on `pytest`. They rely on a python Polaris client, which is generated from the openapi spec.
The client can be generated using two commands:

```shell
# generate the management api client
docker run --rm \
  -v ${PWD}:/local openapitools/openapi-generator-cli generate \
  -i /local/spec/polaris-management-service.yml \
  -g python \
  -o /local/client/python --additional-properties=packageName=polaris.management --additional-properties=apiNamePrefix=polaris

# generate the iceberg rest client
docker run --rm \
  -v ${PWD}:/local openapitools/openapi-generator-cli generate \
  -i /local/spec/polaris-catalog-service.yaml \
  -g python \
  -o /local/client/python --additional-properties=packageName=polaris.catalog --additional-properties=apiNameSuffix="" --additional-properties=apiNamePrefix=Iceberg
```

Tests rely on Python 3.9 or higher. `pyenv` can be used to install a current version and mapped to the local directory
by using

```shell
pyenv install 3.9
pyenv local 3.9
```

Once you've done that, you can run `setup.sh` to generate a python virtual environment (installed at `~/polaris-venv`)
and download all of the test dependencies into it. From here, `run.sh` will be able to execute any pytest present.

To debug, setup IntelliJ to point at your virtual environment to find your test dependencies
(see https://www.jetbrains.com/help/idea/configuring-python-sdk.html). Then run the test in your IDE.

The above is handled automatically when running reg tests from the docker image.
