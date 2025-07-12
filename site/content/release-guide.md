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

# Release Guide

This guide walks you through the release process of the Apache Polaris podling.

## Setup

To create a release candidate, you will need:

* your Apache credentials (for repository.apache.org and dist.apache.org repositories)
* a [GPG key](https://www.apache.org/dev/release-signing#generate) for signing artifacts, published in [KEYS](https://downloads.apache.org/incubator/polaris/KEYS) file

If you haven't published your GPG key yet, you must publish it before starting the release process:

```
svn co https://dist.apache.org/repos/dist/release/incubator/polaris polaris-dist-release
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

During release process, the artifacts will be signed with your key, eventually using `gpg-agent`.

To configure gradle to sign the artifacts, you can add the following settings in your `~/.gradle/gradle.properties` file:

```
signing.gnupg.keyName=Your Key Name
``` 

To use `gpg` instead of `gpg2`, also set `signing.gnupg.executable=gpg`.

For more information, see the Gradle [signing documentation](https://docs.gradle.org/current/userguide/signing_plugin.html#sec:signatory_credentials).

### GitHub Repository

The release should be executed against https://github.com/apache/polaris.git repository (not a fork).
Set it as remote with name `apache` for release if it's not already set up.

## Creating a release candidate

### Initiate a discussion about the release with the community

This step can be useful to gather ongoing patches that the community thinks should be in the upcoming release.

The communication can be started via a `[DISCUSS]` mail on the dev@polaris.apache.org mailing list and the desired tickets can be added to the github milestone of the next release.

Note, creating a milestone in github requires a committer. However, a non-committer can assign tasks to a milestone if added to the list of collaborators in [.asf.yaml](https://github.com/apache/polaris/blob/main/.asf.yaml).

### Create release branch

If it's the first RC for the release, you have to create a release branch:

```
git branch release/x.y.z
git push apache release/x.y.z
```

Go in the branch, and set the target release version:

```
git checkoout release/x.y.z
echo "x.y.z" > version.txt
git commit -a
git push
```

Update `CHANGELOG.md`:
```
./gradlew patchChangelog
git commit -a
git push
```

Note: You should submit a PR to propagate (automated) CHANGELOG updates from the release
branch to `main`.

If more changes are cherry-picked for the next RC, and those change introduce CHANGELOG entries,
follow this update process:
* Manually add an `-rcN` suffix to the previously generated versioned CHANGELOG section.
* Rerun the `patchChangelog` command
* Manually remove RC sections from the CHANGELOG
* Submit a PR to propagate CHANGELOG updates from the release branch to `main`.

Note: the CHANGELOG patch commit should probably be the last commit on the release branch when
an RC is cut. If more changes are cherry-picked for the next RC, it is best to drop the
CHANGELOG patch commit, apply cherry-picks, and re-run `patchChangelog`.

Note: You should also submit a PR on `main` branch to bump the version in the `version.txt` file.

### Create release tag

On the release branch, you create a tag for the RC:

```
git tag apache-polaris-x.y.z-rci
git push apache apache-polaris-x.y.z-rci
```

Switch to the tag:

```
git checkout apache-polaris-x.y.z.rci
```

### Verify the build pass

This is an optional step, but good to do. The purpose here is to verify the build works fine:

```
./gradlew clean build
```

It's also welcome to verify the regression tests (see [regtests/README.md](https://github.com/apache/polaris/blob/main/regtests/README.md) for details).

### Build and stage the distributions

You can now build the source distribution:

```
./gradlew build sourceTarball -Prelease -PuseGpgAgent -x test -x intTest
```

The source distribution archives are available in `build/distribution` folder.

The binary distributions (for convenience) are available in:
* `runtime/distribution/build/distributions`

Now, we can stage the artifacts to dist dev repository:

```
svn co https://dist.apache.org/repos/dist/dev/incubator/polaris polaris-dist-dev
cd polaris-dist-dev
mkdir x.y.z
cp /path/to/polaris/github/clone/repo/build/distribution/* x.y.z
cp /path/to/polaris/github/clone/repo/runtime/distribution/build/distributions/* x.y.z
cp -r /path/to/polaris/github/clone/repo/helm/polaris helm-chart/x.y.z 
svn add x.y.z
svn add helm-chart/x.y.z
svn commit -m"Stage Apache Polaris x.y.z RCx"
```

### Build and stage Maven artifacts

You can now build and publish the Maven artifacts on a Nexus staging repository:

```
./gradlew publishToApache -Prelease -PuseGpgAgent
```

Next, you have to close the staging repository:

1. Go to [Nexus](https://repository.apache.org) and log in
2. In the left menu, click on "Staging Repositories"
3. Select the Polaris repository
4. At the top, select "Close" and follow the instructions
5. In the comment field, use "Apache Polaris x.y.z RCi"

### Start the vote thread

The last step for a release candidate is to create a VOTE thread on the dev mailing list.

A generated email template is available in the `build/distribution` folder.

Example title subject:

```
[VOTE] Release Apache Polaris x.y.z (rci)
```

Example content:

```
Hi everyone,

I propose that we release the following RC as the official
Apache Polaris x.y.z release.

* This corresponds to the tag: apache-polaris-x.y.z-rci
* https://github.com/apache/polaris/commits/apache-polaris-x.y.z-rci
* https://github.com/apache/polaris/tree/<SHA1>

The release tarball, signature, and checksums are here:
* https://dist.apache.org/repos/dist/dev/incubator/polaris/x.y.z

Helm charts are available on:
* https://dist.apache.org/repos/dist/dev/incubator/polaris/helm-chart
NB: the docker images (polaris-server and polaris-admin) will be
published on DockerHub once release vote passes.

You can find the KEYS file here:
* https://downloads.apache.org/incubator/polaris/KEYS

Convenience binary artifacts are staged on Nexus. The Maven
repositories URLs are:
* https://repository.apache.org/content/repositories/orgapachepolaris-<ID>/

Please download, verify, and test.

Please vote in the next 72 hours.

[ ] +1 Release this as Apache polaris x.y.z
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
[RESULT][VOTE] Release Apache Polaris x.y.z (rci)
```

```
Thanks everyone who participated in the vote for Release Apache Polaris x.y.z (rci).

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
[VOTE] Release Apache Polaris x.y.z (rci)
```

```
Hello everyone,

The Apache Polaris community has voted and approved the release of Apache Polaris x.y.z (rci).
We now kindly request the IPMC members review and vote for this release.

Polaris community vote thread:
* https://lists.apache.org/thread/<VOTE THREAD>

Vote result thread:
* https://lists.apache.org/thread/<VOTE RESULT>

The release candidate:
* https://dist.apache.org/repos/dist/dev/incubator/polaris/x.y.z

Git tag for the release:
* https://github.com/apache/polaris/releases/tag/apache-polaris-x.y.z-rci

Git commit for the release:
* https://github.com/apache/polaris/commit/<COMMIT>

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
[RESULT][VOTE] Release Apache Polaris x.y.z (rci)
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
svn mv https://dist.apache.org/repos/dist/dev/incubator/polaris/x.y.z https://dist.apache.org/repos/dist/release/incubator/polaris
svn mv https://dist.apache.org/repos/dist/dev/incubator/polaris/helm-chart/x.y.z https://dist.apache.org/repos/dist/release/incubator/polaris/helm-chart
```

Next, add a release tag to the git repository based on the candidate tag:

```
git tag -a apache-polaris-x.y.z apache-polaris-x.y.z-rci
```
Update GitHub with the release: https://github.com/apache/polaris/releases/tag/apache-polaris-x.y.z

Then release the candidate repository on [Nexus](https://repository.apache.org/#stagingRepositories).

### Publishing docs 
1. Open a PR against branch [`versioned-docs`](https://github.com/apache/polaris/tree/versioned-docs) to publish the documentation
2. Open a PR against the `main` branch to update website
    - Add download links and release notes in [Download page](https://github.com/apache/polaris/blob/main/site/content/downloads/_index.md)
    - Add the release in the [website menu](https://github.com/apache/polaris/blob/main/site/hugo.yaml)

### Announcing the release
To announce the release, wait until Maven Central has mirrored the artifacts.


Send a mail to dev@iceberg.apache.org and announce@apache.org:

```
[ANNOUNCE] Apache Polaris x.y.z 
```

```
The Apache Polaris team is pleased to announce Apache Polaris x.y.z.

<Add Quick Description of the Release>

This release can be downloaded https://www.apache.org/dyn/closer.cgi/incubator/polaris/apache-polaris-x.y.z.

Release notes: https://polaris.apache.org/blog/apache-polaris-x.y.z

Artifacts are available on Maven Central.

Apache Polaris is an open-source, fully-featured catalog for Apache Iceberg™. It implements Iceberg's REST API, enabling seamless multi-engine interoperability across a wide range of platforms, including Apache Doris™, Apache Flink®, Apache Spark™, Dremio®, StarRocks, and Trino.


Enjoy !
```

## How to verify a release

### Validating distributions

Release vote email includes links to:

* Distribution archives (source, admin, server) on dist.apache.org
* Signature files (.asc)
* Checksum files (.sha512)
* KEYS file

After downloading the distributions archives, signatures, checksums, and KEYS file, here are the instructions on how to verify signatures, checksums.

#### Verifying signatures

First, import the keys in your local keyring:

```
curl https://downloads.apache.org/incubator/polaris/KEYS -o KEYS
gpg --import KEYS
```

Next, verify all `.asc` files:

```
gpg --verify apache-polaris-[...].asc
```

#### Verifying checksums

```
shasum -a 512 --check apache-polaris-[...].sha512
```

#### Verifying build and test

In the source distribution:

```
./gradlew build
```

### Voting

Votes are cast by replying on the vote email on the dev mailing list, with either `+1`, `0`, `-1`.

In addition to your vote, it's customary to specify if your vote is binding or non-binding. Only members of the PPMC and mentors have formally binding votes, and IPMC on the vote on the Incubator general mailing list. 
If you're unsure, you can specify that your vote is non-binding. You can find more details on https://www.apache.org/foundation/voting.html.
