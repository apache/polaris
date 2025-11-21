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

# Release Verification Guide

**Audience**: Committers and interested contributors.

This guide walks you through the process of **verifying** a staged Apache Polaris release candidate.

Verifying a (staged) release of an Apache project has to follow a bunch of tasks, which can be
grouped into tasks that can be automated and those that need human intervention.
Polaris provides a tool to automate the tasks that can be automated.

Tasks that are automated:
* Checksums and PGP signatures are valid.
* All expected artifacts are present.
* Source code artifacts have correct names matching the current release.
* Built artifacts are [reproducible](#reproducible-builds).
* Build passes.
* `DISCLAIMER`, `LICENSE` and `NOTICE` files are included.
* main and sources jar artifacts contain `META-INF/LICENSE` and `META-INF/NOTICE` files.
* main distribution artifacts contain `DISCLAIMER`, `LICENSE` and `NOTICE` files in the top-level directory.

Tasks that need human intervention:
* Download links are valid. Check all links in the `[VOTE]` email for the release:
    * Tag on the GitHub website
    * Commit on the GitHub website
    * SVN repository with the source tarball and binary release artifacts
    * SVN repository with the Helm chart
    * Link to the KEYS file (_MUST_ be equal to `https://downloads.apache.org/incubator/polaris/KEYS`)
    * Maven staging repository
* `DISCLAIMER`, `LICENSE` and `NOTICE` files are correct for the repository.
* Contents of jar artifacts `META-INF/LICENSE` and `META-INF/NOTICE` files are correct.
* All files have license headers if necessary.
  This is (mostly) verified using the "rat" tool during builds/CI.
* No disallowed binary artifacts are bundled in the source archive.
  This is a (soft) requirement to be held true by committers.   

**Imply good intent!**
Although the release manager is responsible for producing a "proper" release, mistakes can and will happen.
The Polaris project is committed to providing reproducible builds as an essential building block of
_Apache trusted releases_.
The project depends on frameworks which also strive to provide reproducible builds, but not all
these frameworks can provide fully reproducible builds yet.
The Polaris project's release verification tool will therefore report some issues that are currently expected.
See [below](#reproducible-builds) for details.

# Verifying a release candidate

Instead of performing all mentioned steps manually, you can leverage the script
`tools/verify-release/verify-release.sh` available in the main repository to perform the
automatable tasks.

Always run the most recent version of the script using the following command:
```bash
bash <(curl \
  -s https://raw.githubusercontent.com/apache/polaris/refs/heads/main/tools/verify-release/verify-release.sh) \
  --help
```

The tool is intended for Polaris versions 1.3 and newer.
The tool may report issues, see [below](#reproducible-builds) for details.

That script requires a couple of tools installed and will check that those are available
and report those that need to be installed.

To run the script, you need the following pieces of information:
* The *full* Git SHA of the corresponding source commit.
* The version number of the release, something like `1.3.0`
* The RC number of the release, for example `1` or `2`
* The Maven staging repository ID, for example `1033` (the full number at the end of the Maven repository URL `https://repository.apache.org/content/repositories/orgapachepolaris-1033/`).

Example (values taken from the 1.2.0-rc2 release)
```bash
bash <(curl \
  -s https://raw.githubusercontent.com/apache/polaris/refs/heads/main/tools/verify-release/verify-release.sh) \
  --git-sha 354a5ef6b337bf690b7a12fefe2c984e2139b029 \
  --version 1.2.0 \
  --rc 2 \
  --maven-repo-id 1033
```

Same example, but using the short option names:
```bash
bash <(curl \
  -s https://raw.githubusercontent.com/apache/polaris/refs/heads/main/tools/verify-release/verify-release.sh) \
  -s 354a5ef6b337bf690b7a12fefe2c984e2139b029 \
  -v 1.2.0 \
  -r 2 \
  -m 1033
```

The verification script creates a temporary directory that will eventually contain a fresh Git clone,
all downloaded and all built artifacts.
This temporary directory is deleted after the script has finished. To keep the temporary directory
around, you can use the `--keep-temp-dir` (`-k`) option.

A log file, the name matches the pattern `polaris-release-verify-*.log`,
will be created and contain detailed information about the identified issues reported on the console.

Note: The script is maintained in the Polaris source tree in the `/tools/verify-release` directory.

## Verifications performed by the tool

After some startup checks, the tool emits some information about the release candidate. For example:
```
Verifying staged release
========================

Git tag:           apache-polaris-1.2.0-incubating-rc2
Git sha:           354a5ef6b337bf690b7a12fefe2c984e2139b029
Full version:      1.2.0-incubating
Maven repo URL:    https://repository.apache.org/content/repositories/orgapachepolaris-1033/
Main dist URL:     https://dist.apache.org/repos/dist/dev/incubator/polaris/1.2.0-incubating
Helm chart URL:    https://dist.apache.org/repos/dist/dev/incubator/polaris/helm-chart/1.2.0-incubating
Verify directory:  /tmp/polaris-release-verify-2025-10-23-14-22-31-HPmmiybzk

A verbose log containing the identified issues will be available here:
    /home/snazy/devel/polaris/polaris/polaris-release-verify-2025-10-23-14-22-31.log
```

After that, release candidate verification starts immediately, performing the following operations:

1. Create `gpg` keyring for signature verification from the project's `KEYS` file.
2. Clone the Git repository directly from GitHub.
3. Git commit SHA and Git tag match
4. Verifies that the mandatory files are present in the source tree
5. Helm chart GPG signature and checksum checks
6. Source tarball and binary artifacts GPG signature and checksum checks
7. Build Polaris from the Git commit/tag
8. Compares that the list of Maven artifacts produced by the build matches those in the Nexus staging repository.
9. Compares the individual Maven artifacts of the local build with the ones in the Nexus staging repository.
10. Compares the main binary distribution artifacts for Polaris server and Polaris admin tool.
11. Compares the locally built Helm chart with the one in the staging repository.

Found issues are reported on the console in _red_.

Details for each reported issue will be reported in the log file, depending on the issue and file type.
The intent is to provide as much information as possible to eventually fix a reproducible build issue.
* Text files: Output of `diff` of the local and staged files.
* Zip/Jar files: output of `zipcmp`.
  If `zipcmp` reports no difference, the output of `zipinfo` for both files is logged.
* Tarballs: output of `diff --recursive` the extracted local and staged tarballs.
  If `diff` reports no difference, the output of `tar tvf` for both files is logged.
* Other files: Output of `diff` of the local and staged files.

Note: GPG signatures are verified **only** against the project's `KEYS` file.

# Reproducible builds

A build is reproducible if the built artifacts are identical on every build from the same source.

The Apache Polaris build is currently mostly reproducible, with some release-version specific exceptions.

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
