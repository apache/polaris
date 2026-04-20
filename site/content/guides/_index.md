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
linkTitle: "Guides"
title: "Apache Polaris Guides"
weight: 200
cascade:
    type: docs
menus:
    main:
        parent: Guides
        weight: 1
        identifier: overview
        name: Overview
---

You can quickly get started with Polaris by playing with the docker-compose examples provided
by the guides shown on the left of this page.
Each guide has detailed instructions.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [jq](https://stedolan.github.io/jq/download/) (for some examples)

## Getting Started Examples

- [Spark](spark): An example that uses an in-memory metastore, automatically bootstrapped, with
  Apache Spark and a Jupyter notebook.

- [Telemetry](telemetry): An example that includes Prometheus and Jaeger to collect metrics and
  traces from Apache Polaris. This example automatically creates a `polaris_demo` catalog.

- [Keycloak](keycloak): An example that uses Keycloak as an external identity provider (IDP) for
  authentication.

## Authoring Guides

Writing new Guides or updating existing ones, especially those that use Docker Compose, should follow these
guidelines. This ensures that the examples work as expected and that the guides pass the tests
run in CI and locally.

### Running guides CI tests locally

Requirements: Python 3.14 installed locally. Other Python 3 versions should work as well, but
have not been validated.

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

More information about guides-testing can be found in the `site/it/README.md` file in the
[source repository](https://github.com/apache/polaris).

### Constraints

1. `docker compose` invocations must be on a single line in a `shell` code block.
2. When invoking Spark SQL shell in a `shell` code block, the `bin/spark-sql ...` invocation
   must be the only statement in that code block.
3. Currently, only one _docker-compose_ file is supported per guide. Using multiple
   docker-compose files needs some changes to the testing code,
   see [here](https://github.com/apache/polaris/pull/3553#discussion_r2868090144).

### Tips for Docker usage

- Do not use any of the `--interactive` or `--tty` options

### Tips for Docker Compose files

- All "daemon" services should have a healthcheck and depend on it using
  `condition: service_healthy`.
- Always put port-mappings in quotes, for example:
  ```yaml
  ports:
    - "9874:9874"
  ```
- "Final setup services," for example the service that sets up Polaris, are a bit
  tricky to get right in the sense that those do not let `docker compose up --detach --wait`
  fail.
  The former docker-compose command would yield an error if any service exits.
  To prevent this, setup-services should have a final "endless" command to keep them
  running and have a health check to inform about its success.

  "Intermediate setup services," for example the service to create a bucket, do **not**
  and **should not** have a final `sleep` command, because that would delay the overall
  startup. The Polaris service must depend on the intermediate service to setup a bucket
  with the `condition` `service_completed_successfully`.

  Working example:
  ```yaml
  polaris-setup:
    image: alpine/curl:8.17.0
    depends_on:
      polaris:
        condition: service_healthy
    (...)
    entrypoint: "/bin/sh"
    command:
      - "-c"
      - >-
        do_stuff &&
        touch /tmp/polaris-setup-done &&
        tail -f /dev/null
    healthcheck:
      test: ["CMD", "test", "-f", "/tmp/polaris-setup-done"]
  ```
- Take care to propagate _all_ failures of a setup service's commands.
  This can be achieved by chaining the commands with `&&` and using `set -e` at the
  beginning of executed scripts.
- For `curl`, use the `--fail-with-body` or `--fail` options or add specialized logic
  to validate the HTTP response code.
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
- Dependencies on "daemon" services with a "proper" health check should be declared using
  a syntax like this:
  ```yaml
  polaris:
    image: apache/polaris:latest
    depends_on:
      minio:
        condition: service_healthy
  ```

### Tips for inspecting Docker compose logs

When inspecting the logs of Docker Compose, watch the logged timestamps.
The order of the log entries emitted by Docker Compose is not necessarily in the
natural order of execution between different services.
Timestamps of each log entry are emitted to the console and the test log files.
