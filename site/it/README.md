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

# Polaris Guide Testing

This repository contains tools and scripts for testing Apache Spark guides.
It provides a framework for running automated tests against guide content,
ensuring that at least the basic functionality works.

## Overview

Guide testing is performed via the "entry" shell script `site_guide_ci.sh`,
which can also be invoked locally.

The "entry" script calls a Python tool to extract the code blocks from a
guide's Markdown file and write it to a temporary file as a bash script.
Only `shell` code blocks are extracted, see [Spark SQL](#spark-sql-code) below. 

The "entry" script then executes the generated bash script.

Executed code blocks that do not complete successfully, stop the execution of
the generated test script and mark the test for that guide as failed. 

Each generated temporary bash script, one per guide, is called with the
current working directory set to the projects _workspace_ directory.

## Running the tests

Requirements: Python 3.14

To run tests for all guides in `site/content/guides`, run
```shell
cd site/it
./markdown-testing.py
```

To run tests for one or more guides in `site/content/guides`, use the following pattern:
```shell
cd site/it
./markdown-testing.py content/guides/rustfs/index.md content/guides/telemetry/index.md
```

## Constraints

1. `docker compose` invocations must be on a single line in a `shell` code block.
2. Spark SQL shell invocations must be the only statement `shell` code block in a guide. 

## Adding assertions and test specific code

"Assertions" and other executions for the guide-tests can be added to the
Markdown file by adding a `shell` code block inside an HTML comment.
The code block will be executed but not appear in the guide.

````html
<!--
```shell
# add shell script code to test things
```
-->
````

## Environment variables available to the generated bash scripts

| Variable                  | Description                                         |
|---------------------------|-----------------------------------------------------|
| `SITE_TEST_WORKSPACE_DIR` | Absolute path to the project's workspace directory. |
| `SITE_TEST_GUIDE_DIR`     | Absolute path to the guide's directory.             |

## Applied `shell` code block tweaks

* Many guides start Docker Compose without the [`--detach` option](https://docs.docker.com/reference/cli/docker/compose/up/).
  The generated test code adds the `--detach --wait` options to the `docker compose up`
  command.
* `curl` command invocations get the `--fail-with-body` option.
* `spark-sql` invocations get the SQL statements to execute via a file that is generated
  from the following `sql` code blocks.

## Tips for Docker usage

- Do not use any of the `--interactive` or `--tty` options

## Tips for Docker Compose files

- All "daemon" services should have a healthcheck and depend on it using 
  `condition: service_healthy`.
- Always put port-mappings in quotes, for example:
  ```yaml
  ports:
    - "9874:9874"
  ```
- "Setup" services, for example to create a bucket or to setup Polaris, **MUST**
  explicitly declare the `restart: "no"`. This is important to avoid `docker compose up`
  from returning an error code when the "setup" service exits, even if the service
  exited with code 0.
- Do not let "setup" services run ("hang") forever.
- Service dependencies (`depends_on`) must be declared with the correct `condition`,
  meaning to use the "Long syntax" as described in the
  [Docker Compose documentation](https://docs.docker.com/compose/compose-file/compose-file-v3/#depends_on).
- Dependencies on "setup" services MUST be declared using a syntax like this:
  ```yaml
  polaris:
    image: apache/polaris:latest
    depends_on:
      setup_bucket:
        condition: service_completed_successfully
  ```  
- Dependencies on "daemon" services should be declared using a syntax like this:
  ```yaml
  polaris:
    image: apache/polaris:latest
    depends_on:
      minio:
        condition: service_healthy
  ```

## Tips for inspecting Docker compose logs

When inspecting the logs of Docker Compose, watch the logged timestamps.
The order of the log entries emitted by Docker Compose is not necessarily in the
natural order of execution between different services.
Timestamps of each log entry are emitted to the console and the test log files.

## Spark SQL code

Many guides use Spark SQL code blocks.
The Python tool recognizes a sequence of a `shell` code block that starts with
a line containing `bin/spark-sql` followed by one or more `sql` code blocks and
unifies both into one execution of Spark SQL.

## GitHub Workflow

The GitHub workflow `site_guide_ci.yml` is used to run the guide tests on pull requests.

The executed generated bash scripts and the corresponding log files are uploaded as
GitHub workflow artifacts.

## Linting

Run the local lint checks with:

```bash
./lint.sh
```
