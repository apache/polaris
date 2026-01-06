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
linkTitle: Polaris Tools Manual Release Guide
title: Polaris Tools Manual Release Guide
type: docs
weight: 200
aliases:
  - /release-guide
  - /community/release-guide
---

**Audience**: Release Managers

This guide walks you through the process of **creating** a release for a Polaris Tool. The guide uses `iceberg-catalog-migrator` as an example.

## Prerequisites

To create a release candidate, you will need:

* your Apache credentials (for repository.apache.org and dist.apache.org repositories)
* a [GPG key](https://www.apache.org/dev/release-signing#generate) for signing artifacts, published in [KEYS](https://downloads.apache.org/incubator/polaris/KEYS) file

### Publish your GPG key

If you haven't published your GPG key yet, you must publish it before starting the release process:

```
svn checkout https://dist.apache.org/repos/dist/release/incubator/polaris polaris-dist-release
cd polaris-dist-release
echo "" >> KEYS # append a new line
gpg --list-sigs <YOUR KEY ID HERE> >> KEYS # append signatures
gpg --armor --export <YOUR KEY ID HERE> >> KEYS # append public key block
svn commit -m "add key for <YOUR NAME HERE>"
```

To send the key to the Ubuntu key-server, Apache Nexus needs it to validate published artifacts:
```
gpg --keyserver hkps://keyserver.ubuntu.com --send-keys <YOUR KEY ID HERE>
```

### Dist repository

The Apache dist repository (dist.apache.org) is used to populate download.apache.org and archives.apache.org. There are two spaces on dist:

* `dev` is where you stage the source distribution and other user convenient artifacts (distributions, helm charts, ...)
* `release` is where the artifacts will be copied when the release vote passed

Apache dist is a svn repository, you need your Apache credentials to commit there.

### Maven repository

Apache uses Nexus as Maven repository (repository.apache.org) where releases are staged (during vote) and copied to Maven Central when the release vote passed.

You have to use Apache credentials on Nexus, configured in `~/.gradle/gradle.properties` file using `mavenUser` and `mavenPassword`:

```
apacheUsername=yourApacheId
apachePassword=yourPassword
```

Note: an alternative is to use `ORG_GRADLE_PROJECT_apacheUsername` and `ORG_GRADLE_PROJECT_apachePassword` environment variables:

```
export ORG_GRADLE_PROJECT_apacheUsername=yourApacheId
export ORG_GRADLE_PROJECT_apachePassword=bar
```

### PGP signing

During release process, the artifacts will be signed with your key, using `gpg-agent`.

To configure gradle to sign the artifacts, you can add the following settings in your `~/.gradle/gradle.properties` file:

```
signing.gnupg.keyName=Your Key Name
``` 

To use `gpg` instead of `gpg2`, also set `signing.gnupg.executable=gpg`.

For more information, see the Gradle [signing documentation](https://docs.gradle.org/current/userguide/signing_plugin.html#sec:signatory_credentials).

### GitHub Repository

The release should be executed against https://github.com/apache/polaris-tools.git repository (not a fork).
Set it as remote with name `apache` for release if it's not already set up.

## Creating a release candidate

### Initiate a discussion about the release with the community

This step can be useful to gather ongoing patches that the community thinks should be in the upcoming Polaris Tool release.

The communication can be started via a `[DISCUSS]` mail on the dev@polaris.apache.org mailing list and the desired tickets can be added to the github milestone of the next release, specifically on the Polaris Tool you plan to release.

Note, creating a milestone in github requires a committer.
Also note since Polaris is an incubating project, make sure what every branch / tag you are creating is suffixed with `-incubating`.

### Create Polaris Tool release branch

If it's the first RC for the release, you have to create a release branch:

```
git branch release/tool/x.y.z
git push apache release/tool/x.y.z
```

Go in the branch:

```
git checkout release/tool/x.y.z
```

Do the needed modifications according to the Polaris Tool (version bump in `version.txt`, ...) and push the changes on the branch.

### Create release tag

On the release release branch, you create a tag for the RC:

```
git tag apache-polaris-tool-x.y.z-rci
git push apache apache-polaris-tool-x.y.z-rci
```

Switch to the tag:

```
git checkout apache-polaris-tool-x.y.z-rci
```

### Verify the build pass

This is an optional step, but good to do. It also depends of the tool (each tool might have a different build logic).

### Build and stage the distributions

If the Polaris Tool you are releasing contains a specific logic (gradle task, script, ...) to create the source distribution, you should use it.

Else, you can create the source distribution directly on the repository.

First cleanup the repository (to be sure we don't have previous build generated file):

```
git clean -fdx
```

Now, you can create the archive to the source distribution:

```
tar zcvf polaris-tool-x.y.z.tar.gz tool
shasum -c 512 polaris-tool-x.y.z.tar.gz > polaris-tool-x.y.z.tar.gz.sha512
gpg --armor --output polaris-tool-x.y.z.tar.gz.asc --detach-sig polaris-tool-x.y.z.tar.gz
```

The Polaris Tool usually provide a command to create a binary distribution.

Now, you can stage the distribution artifacts (source and binary) to dist dev repository:

```
svn checkout https://dist.apache.org/repos/dist/dev/incubator/polaris polaris-dist-dev
cd polaris-dist-dev
mkdir tool/x.y.z
cp [distribution] tool/x.y.z
svn add tool/x.y.z
```

### Build and stage Maven artifacts

If applicable to the Polaris Tool you are releasing, you can publish the Maven artifacts on a Nexus staging repository.

### Start the vote thread

```
Subject: [VOTE] Release Apache Polaris Tool x.y.z (rci)

Hi everyone,

I propose that we release the following RC as the official
Apache Polaris Tool x.y.z release.

* This corresponds to the tag: apache-polaris-tool-x.y.z-rci
* https://github.com/apache/polaris-tool/commits/apache-polaris-tool-x.y.z-rci
* https://github.com/apache/polaris-tool/tree/<SHA1>

The release tarball, signature, and checksums are here:
* https://dist.apache.org/repos/dist/dev/incubator/polaris/tool/x.y.z

You can find the KEYS file here:
* https://downloads.apache.org/incubator/polaris/KEYS

Convenience binary artifacts are staged on Nexus. The Maven
repositories URLs are:
* https://repository.apache.org/content/repositories/orgapachepolaris-<ID>/

Please download, verify, and test.

Please vote in the next 72 hours.

[ ] +1 Release this as Apache Polaris Tool x.y.z
[ ] +0
[ ] -1 Do not release this because...

Only PPMC members and mentors have binding votes, but other community
members are
encouraged to cast non-binding votes. This vote will pass if there are
3 binding +1 votes and more binding +1 votes than -1 votes.

NB: if this vote passes, a new vote has to be started on the Incubator
general mailing
list.
```

When a candidate is passed or rejected, reply with the vote result:

```
[RESULT][VOTE] Release Apache Polaris Tool x.y.z (rci)
```

```
Thanks everyone who participated in the vote for Release Apache Polaris Tool x.y.z (rci).

The vote result is:

+1: a (binding), b (non-binding)
+0: c (binding), d (non-binding)
-1: e (binding), f (non-binding)

A new vote is starting in the Apache Incubator general mailing list.
```

### Start a new vote on the Incubator general mailing list

As Polaris is an Apache Incubator project, you now have to start a new vote on the Apache Incubator general mailing list.

You have to send this email to general@incubator.apache.org:

```
[VOTE] Release Apache Polaris Tool x.y.z (rci)
```

```
Hello everyone,

The Apache Polaris community has voted and approved the release of Apache Polaris Tool x.y.z (rci).
We now kindly request the IPMC members review and vote for this release.

Polaris community vote thread:
* https://lists.apache.org/thread/<VOTE THREAD>

Vote result thread:
* https://lists.apache.org/thread/<VOTE RESULT>

The release candidate:
* https://dist.apache.org/repos/dist/dev/incubator/polaris/tool/x.y.z

Git tag for the release:
* https://github.com/apache/polaris-tool/releases/tag/apache-polaris-tool-x.y.z-rci

Git commit for the release:
* https://github.com/apache/polaris-tool/commit/<COMMIT>

Maven staging repository:
* https://repository.apache.org/content/repositories/orgapachepolaris-<ID>/

Please download, verify and test.

Please vote in the next 72 hours.

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason

To learn more about apache Polaris, please see https://polaris.apache.org/

Checklist for reference:

[ ] Download links are valid.
[ ] Checksums and signatures.
[ ] LICENSE/NOTICE files exist
[ ] No unexpected binary files
[ ] All source files have ASF headers
[ ] Can compile from source
```

Binding votes are the votes from the IPMC members. Similar to the previous vote, send the result on the Incubator general mailing list:

```
[RESULT][VOTE] Release Apache Polaris Tool x.y.z (rci)
```

```
Hi everyone,

This vote passed with the following result:


```

## Finishing the release

After the release votes passed, you need to release the last candidate's artifacts.

### Publishing the release

First, copy the distribution from the dist dev space to the dist release space:

```
svn mv https://dist.apache.org/repos/dist/dev/incubator/polaris/tool/x.y.z https://dist.apache.org/repos/dist/release/incubator/polaris/tool
```

Next, add a release tag to the git repository based on the candidate tag:

```
git tag -a apache-polaris-tool-x.y.z apache-polaris-tool-x.y.z-rci
```

Update GitHub with the release: https://github.com/apache/polaris-tool/releases/tag/apache-polaris-tool-x.y.z

Then release the candidate repository on [Nexus](https://repository.apache.org/#stagingRepositories).