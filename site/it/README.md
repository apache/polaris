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
  The generated test code adds the `--detach --wait --quiet-pull` options to the
  `docker compose up` command.
* `curl` command invocations get the `--fail-with-body` option.
  Adding either the `--fail` or `--fail-with-body` option should be considered for
  ever usage of `curl`, otherwise `curl` will exit with code 0 for error responses
  like 401 or 404 and others. 
* `spark-sql` invocations get the SQL statements to execute via a file that is generated
  from the following `sql` code blocks.

## Tips

See [Authoring Guides](../content/guides/_index.md#authoring-guides).


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
