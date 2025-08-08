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

# Polaris Release Automation Scripts

This directory contains scripts to automate the Apache Polaris release process, following the [official release guide](https://github.com/apache/polaris/blob/main/site/content/release-guide.md).

## Prerequisites
### General Prerequisites
Before using these scripts, ensure:

1. **GPG Setup**: Configure signing key in `~/.gradle/gradle.properties`
2. **Apache Credentials**: Set `apacheUsername` and `apachePassword` in gradle.properties or environment variables
3. **Git Remote**: Apache remote configured as "apache" pointing to https://github.com/apache/polaris.git
4. **GitHub Token**: Set `GITHUB_TOKEN` environment variable with a personal access token for CI status verification
5. **Permissions**: Write access to dist.apache.org (not verified by scripts)
6. **Docker Setup** (for step 6): Docker with buildx support and DockerHub credentials configured

### GitHub Token Setup

The release scripts require a GitHub personal access token to verify that all tests have been run. Create a token with the following permissions:
- `repo:status` - to read commit status
- `public_repo` - to access public repository information

Set the token as the `GITHUB_TOKEN` environment variable.

## Release Scripts

These scripts are used to perform actual releases:

### 1. Prerequisites Verification
```bash
./01-prerequisites.sh
```
Verifies that your environment is ready for releases:
- GPG setup and key configuration
- Maven/Gradle credentials
- Git remote setup for Apache repository
- GitHub token for test status verification

### 2. Create Release Branch
```bash
./02-create-release-branch.sh --version 1.0.0-incubating-rc1 [--commit HEAD] [--recreate]
```
Creates a release branch and sets the target release version:
- Creates `release/x.y.z` branch from specified commit
- Updates `version.txt` with the release version
- Updates `CHANGELOG.md` using Gradle task
- Pushes changes to Apache remote

### 3. Create Release Candidate Tag
```bash
./03-create-release-candidate-tag.sh --version 1.0.0-incubating-rc1
```
Creates a release candidate tag for a release candidate:
- Creates and pushes `apache-polaris-x.y.z-incubating-rcN` tag
- Validates RC sequence (RC2+ requires previous RC to exist)
- Checks out the created tag

### 4. Build and Verify Tests
```bash
./04-build-and-test.sh
```
Builds Polaris and ensures all integration and regression tests have been run:
- Performs clean build with Gradle
- Verifies that all GitHub checks have passed for current commit
- Ensures all CI workflows are successful before release

### 5. Build and Stage Distributions
```bash
./05-build-and-stage-distributions.sh
```
Builds and stages release artifacts:
- Must be run from a release candidate tag
- Builds source and binary distributions
- Stages artifacts to Apache dist dev repository
- Publishes Maven artifacts to Apache staging repository

### 6. Build and Stage Docker Images
```bash
./06-build-and-stage-docker-images.sh
```
Builds and publishes multi-platform Docker images:
- Must be run from a release candidate tag
- Builds polaris-server and polaris-admin Docker images
- Publishes images to DockerHub with RC tag
- Supports multi-platform builds (linux/amd64, linux/arm64)

## Test Scripts

These scripts are used to test the release automation without making actual changes:

### Running Tests
```bash
# Test individual scripts
./test/test-02-create-release-branch.sh
./test/test-03-create-release-candidate-tag.sh
./test/test-04-build-and-test.sh
./test/test-05-build-and-stage-distributions.sh
./test/test-06-build-and-stage-docker-images.sh
```

All test scripts run in dry-run mode and verify the exact commands that would be executed.

## Environment Variables
The releases script initialize environment variables if not already set.
This means that it is possible to override the default values by setting the environment variables before running the scripts.
Example:

- `DRY_RUN=0` - Disable dry-run mode (default: enabled)
- `APACHE_REMOTE_NAME=my-apache-remote` - Name of git remote where release branches and tags are pushed (default: "apache")
- `KEYSERVER` - GPG keyserver URL (default: "hkps://keyserver.ubuntu.com")
- ...

See the content of `libs/_constants.sh` for the list of all environment variables and their default values.