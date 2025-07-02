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
Title: Installing Dependencies
type: docs
weight: 100
---

This guide serves as an introduction to several key entities that can be managed with Apache Polaris (Incubating), describes how to build and deploy Polaris locally, and finally includes examples of how to use Polaris with Apache Spark&trade;.

# Prerequisites

This guide covers building Polaris, deploying it locally or via [Docker](https://www.docker.com/), and interacting with it using the command-line interface and [Apache Spark](https://spark.apache.org/). Before proceeding with Polaris, be sure to satisfy the relevant prerequisites listed here.

## Git

To get the latest Polaris code, you'll need to clone the repository using [git](https://git-scm.com/). You can install git using [homebrew](https://brew.sh/) on MacOS:

```shell
brew install git
```

Please follow instructions from the [Git Documentation](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) for instructions on installing Git on other platforms.

Then, use git to clone the Polaris repo:

```shell
git clone https://github.com/apache/polaris.git ~/polaris
```

## Docker

It is recommended to deploy Polaris inside [Docker](https://www.docker.com/) for the Quickstart workflow. Instructions for deploying the Quickstart workflow on the supported Cloud Providers (AWS, Azure, GCP) will be provided only with Docker. However, non-Docker deployment instructions for local deployments can also be followed on Cloud Providers.

Instructions to install Docker can be found on the [Docker website](https://docs.docker.com/engine/install/). Ensure that Docker and the Docker Compose plugin are both installed.

### Docker on MacOS
Docker can be installed using [homebrew](https://brew.sh/):

```shell
brew install --cask docker
```

There could be a [Docker permission issues](https://github.com/apache/polaris/pull/971) related to seccomp configuration. To resolve these issues, set the `seccomp` profile to "unconfined" when running a container. For example:

```shell
docker run --security-opt seccomp=unconfined apache/polaris:latest
```

Note: Setting the seccomp profile to "unconfined" disables the default system call filtering, which may pose security risks. Use this configuration with caution, especially in production environments.

### Docker on Amazon Linux
Docker can be installed using a modification to the CentOS instructions. For example:

```shell
sudo dnf update -y
# Remove old version
sudo dnf remove -y docker docker-client docker-client-latest docker-common docker-latest docker-latest-logrotate docker-logrotate docker-engine
# Install dnf plugin
sudo dnf -y install dnf-plugins-core
# Add CentOS repository
sudo dnf config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
# Adjust release server version in the path as it will not match with Amazon Linux 2023
sudo sed -i 's/$releasever/9/g' /etc/yum.repos.d/docker-ce.repo
# Install as usual
sudo dnf -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

### Confirm Docker Installation

Once installed, make sure that both Docker and the Docker Compose plugin are installed:

```shell
docker version
docker compose version
```

Also make sure Docker is running and is able to run a sample Docker container:

```shell
docker run hello-world
```

## Java

If you plan to build Polaris from source yourself or using this tutorial's instructions on a Cloud Provider, you will need to satisfy a few prerequisites first.

Polaris is built using [gradle](https://gradle.org/) and is compatible with Java 21. We recommend the use of [jenv](https://www.jenv.be/) to manage multiple Java versions. For example, to install Java 21 via [homebrew](https://brew.sh/) and configure it with jenv:

```shell
cd ~/polaris
brew install openjdk@21 jenv
jenv add $(brew --prefix openjdk@21)
jenv local 21
```

Ensure that `java --version` and `javac` both return non-zero responses.

## jq

Most Polaris Quickstart scripts require [jq]((https://jqlang.org/download/)). You can install jq using [homebrew](https://brew.sh/):
```shell
brew install jq
```
