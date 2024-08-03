<!--

 Copyright (C) 2024 Snowflake Computing Inc.
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

-->

# End-to-end regression tests

## Run Tests With Docker Compose

Tests can be run with docker-compose by executing

```bash
docker compose up --build --exit-code-from regtest
```

This is the flow used in CI and should be done locally before pushing to GitHub to ensure that no environmental
factors contribute to the outcome of the tests.

## Run all tests

Polaris REST server must be running on localhost:8181 before running tests.

Running test harness will automatically run idempotent setup script.

```
./run.sh
```

## Run in VERBOSE mode with test stdout printing to console

```
VERBOSE=1 ./run.sh t_spark_sql/src/spark_sql_basic.sh
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

## Setup without running tests

Setup is idempotent.

```
./setup.sh
```

## Experiment with failed test

```
rm t_hello_world/ref/hello_world.sh.ref
./run.sh
```

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

Simply run the specified fixdiffs file to run `meld` and fix the ref file.

```
/tmp/polaris-regtests/t_hello_world/hello_world.sh.fixdiffs.sh
```

## Run a spark-sql interactive shell

With in-memory standalone Polaris running:

```
./run_spark_sql.sh
```

## Python Tests

Python tests are based on `pytest`. They rely on a python Polaris client, which is generated from the openapi spec.
The client can be generated using two commands:

```bash
# generate the management api client
$ docker run --rm \
  -v ${PWD}:/local openapitools/openapi-generator-cli generate \
  -i /local/spec/polaris-management-service.yml \
  -g python \
  -o /local/regtests/client/python --additional-properties=packageName=polaris.management --additional-properties=apiNamePrefix=polaris

# generate the iceberg rest client
$ docker run --rm \
  -v ${PWD}:/local openapitools/openapi-generator-cli generate \
  -i /local/spec/rest-catalog-open-api.yaml \
  -g python \
  -o /local/regtests/client/python --additional-properties=packageName=polaris.catalog --additional-properties=apiNameSuffix="" --additional-properties=apiNamePrefix=Iceberg
```

Tests rely on Python 3.8 or higher. `pyenv` can be used to install a current version and mapped to the local directory
by using

```bash
pyenv install 3.8
pyenv local 3.8
```

Once you've done that, you can run `setup.sh` to generate a python virtual environment (installed at `~/polaris-venv`)
and download all the test dependencies into it. From here, `run.sh` will be able to execute any pytest present.

To debug, setup IntelliJ to point at your virtual environment to find your test dependencies
(see https://www.jetbrains.com/help/idea/configuring-python-sdk.html). Then run the test in your IDE.

The above is handled automatically when running reg tests from the docker image.
