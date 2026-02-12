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

# GitHub Secrets & Variables

## [GitHub `apache`](https://github.com/apache) organization secrets

These secrets are provided and managed by ASF Infra.

Note: Organization level secrets are _not_ shown the repository's list of secrets.

| Secret                      | Purpose                                                                                               |
|-----------------------------|-------------------------------------------------------------------------------------------------------|
| `DEVELOCITY_ACCESS_KEY`     | Access key to [ASF Develocity](https://develocity.apache.org), used to publish Gradle build scans     |
| `DOCKERHUB_USER`            | [Docker Hub](https://hub.docker.com/r/apache/polaris) user name, used to publish container images.    |
| `DOCKERHUB_TOKEN`           | Token for the above.                                                                                  |
| `NEXUS_STAGE_DEPLOYER_USER` | [Apache Nexus](https://repository.apache.org/) repository, used to stage and publish Maven artifacts. |
| `NEXUS_STAGE_DEPLOYER_PW `  | Password for the above                                                                                |

## [`apache/polaris`](https://github.com/apache/polaris) repository

### GitHub Secrets

[These secrets](https://github.com/apache/polaris/settings/secrets/actions) are either provided and managed by Apache
Infra or the Polaris (P)PMC.

| Secret                     | Purpose                                                                           | Provided by    |
|----------------------------|-----------------------------------------------------------------------------------|----------------|
| `POLARIS_GPG_PRIVATE_KEY`  | GPG signing key, used to sign release artifacts.                                  | ASF Infra      |
| `POLARIS_SVN_DEV_USERNAME` | [Apache SVN](https://dist.apache.org/repos/dist) username.                        | ASF Infra      |
| `POLARIS_SVN_DEV_PASSWORD` | Password for the above.                                                           | ASF Infra      |
| `TEST_PYPI_API_TOKEN`      | [TestPyPI](https://test.pypi.org/) API token, used to publish snapshot artifacts. | Polaris (P)PMC |

### GitHub variables

None.

## [`apache/polaris-tools`](https://github.com/apache/polaris-tools) repository

### GitHub Secrets

[These secrets](https://github.com/apache/polaris-tools/settings/secrets/actions) are either provided and managed by
Apache Infra or the Polaris (P)PMC.

| Secret                | Purpose                                                                           | Provided by    |
|-----------------------|-----------------------------------------------------------------------------------|----------------|
| `TEST_PYPI_API_TOKEN` | [TestPyPI](https://test.pypi.org/) API token, used to publish snapshot artifacts. | Polaris (P)PMC |

### GitHub Variables

None.
