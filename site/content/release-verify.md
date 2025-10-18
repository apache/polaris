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

# Release Verification

This guide walks you through the process of verifying a staged Apache Polaris release candidate.

Verifying a (staged) release of an Apache project has to follow a bunch of tasks, which can be
grouped into tasks that can be automated and those that need human intervention.

Tasks that are automated:
* Checksums and PGP signatures are valid.
* All expected artifacts are present.
* Built artifacts are [reproducible](#reproducible-builds).
* Build passes.
* `DISCLAIMER`, `LICENSE` and `NOTICE` files are included.
* main and sources jar artifacts contain `META-INF/LICENSE` and `META-INF/NOTICE` files.
* main distribution artifacts contain `DISCLAIMER`, `LICENSE` and `NOTICE` files in the top-level directory.

Tasks that need human intervention:
* Download links are valid.
* Source code artifacts have correct names matching the current release.
* `DISCLAIMER`, `LICENSE` and `NOTICE` files are correct for the repository.
* Contents of jar artifacts `META-INF/LICENSE` and `META-INF/NOTICE` files are correct.
* All files have license headers if necessary.
  This is (mostly) verified using the "rat" tool during builds/CI.
* No compiled archives bundled in source archive.
  This is a (soft) requirement to be held true by committers.   

# Verifying a release candidate

Instead of performing all mentioned steps manually, you can leverage the script
`tools/verify-release/verify-release.sh` available in the main repository to perform the
automatable tasks.

That script requires a couple of tools installed.
The script will check for the presence of these tools.

To run the script, you need the following pieces of information:
* The version number of the release.
* The RC number of the release.
* The Git SHA of the corresponding source commit.
* The Maven staging repository ID.

Example (from the 1.2.0-rc2 release)
```bash
tools/verify-release/verify-release.sh -s 354a5ef6b337bf690b7a12fefe2c984e2139b029 -v 1.2.0 -r 2 -m 1033
```

The verification script creates a temporary directory that will eventually contain a fresh Git clone,
all downloaded and all built artifacts.
This temporary directory is deleted after the script has finished. To keep the temporary directory
around, you can use the `-k` option.

A log file is created containing verbose information about the identified issues.

# Reproducible builds

A build is reproducible if the built artifacts are identical on every build from the same source.

The Apache Polaris build is currently mostly reproducible, with some release version specific exceptions.

## Exceptions for all Apache Polaris versions

Pending on full support for reproducible builds in Quarkus:
* Jars containing generated code are not guaranteed to be reproducible. Affects the following jars:
    * */quarkus/generated-bytecode.jar
    * */quarkus/transformed-bytecode.jar
    * */quarkus/quarkus-application.jar
* Re-assembled jars are not guaranteed to be reproducible: Affects the following jars:
    * admin/app/polaris-admin-*.jar
    * server/app/polaris-server-*.jar
* Zips and tarballs containing any of the above are not guaranteed to be reproducible.

Helm chart package tarball is not binary reproducible because there is no option to influence the
mtime and POSIX attributes of the archive entries.
The actual content of the archive entries is reproducible.

## Exceptions for Apache Polaris up to 1.2 (including)

* Depending on the operating system being used by the release manager and the "verifier", jar and zip files
  might be reported as different, even if the content of the jar and zip files is identical.
  This also leads to reported differences of the Gradle *.module files, because the checksums are different.
  Fixed via https://github.com/apache/polaris/pull/2819
* Source tarball is not binary reproducible because of non-constant mtime for tar entries.
  Fixed via https://github.com/apache/polaris/pull/2823
* The content of the parent pom contains dynamically generated content for the lists of developers and
  contributors.
  Fixed via https://github.com/apache/polaris/pull/2826

## Exceptions for Apache Polaris up to 1.1 (including)

Apache Polaris builds up to 1.1 are not reproducible.
