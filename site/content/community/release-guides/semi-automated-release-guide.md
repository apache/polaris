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
linkTitle: Semi-Automated Release Guide
title: Semi-Automated Release Guide
type: docs
weight: 100
params:
  show_page_toc: true
---

**Audience**: Release Managers

## Overview
The steps performed in the [Manual Release Guide](../manual-release-guide/) have been automated to a large extent. This semi-automated release guide outlines the workflows that can be used to perform a release with little manual intervention.

_Note that all screenshots in this page are for illustration purposes only. The actual repository name, version numbers, etc. may be different._

## Dry-run mode
Each of the Github Workflows that have been developed comes with a `dry-run` mode. It is enabled ticking the `Dry run mode` checkbox before starting the workflow. When enabled, the workflow will not perform any destructive action (e.g. tag creation, branch deletion, etc.) but instead print out the commands that would have been executed.

Dry-run mode is enabled by default. So ensure that you uncheck the `Dry run mode` checkbox before starting the workflow.

## Prerequisites
### Announce the intent to cut a release
Polaris follows a schedule-driven release model. The first thing to do is to send an e-mail to the dev@polaris.apache.org mailing list to announce the intent to cut a release, approximately a week before the scheduled release date. The e-mail should include the following information:
* The version number of the release (e.g. 1.4.0)
* The tentative date of the release (e.g. 2026-01-19)

Note that the tentative date is only a suggestion. The actual date of the release will be determined by taking community feedback into account. For instance, if specific pull requests are about to be merged, the release may be delayed by a couple of days to include them.

```
[DISCUSS] Apache Polaris [major].[minor].[patch]
```

```
Hello everyone,

The purpose of this e-mail is to collect feedback on the upcoming Apache
Polaris [major].[minor].[patch] release. The tentative release date is
YYYY-MM-DD. Please let me know if you have any concerns or comments, or if
there are some specific pull requests that you would like to see included in
the release.

Thanks,
```

### Ensure that the changelog is up to date
As part of Polaris development process, for each major change in the codebase, a new entry should be added to the `CHANGELOG.md` file. This is usually verified during pull request reviews. But some changes may have been missed. So before cutting a release, it is important to ensure that the changelog is up to date.

## Release branch creation workflow
The first Github workflow to run is [`Release - 1 - Create Release Branch`](https://github.com/apache/polaris/actions/workflows/release-1-create-release-branch.yml). This workflow will create a release branch from the main branch. The release branch is named after the release number and does not include the patch version. For instance, the release branch for version 1.3.0 is named `release/1.3.x`. It should **only be executed only once** per `major.minor` version.


![Screenshot of the first release workflow for 1.3.0-incubating](/img/release-guides/github-workflow-1.png "Screenshot of the first release workflow for 1.3.0-incubating")

Once the workflow has run, the run details page contains a recap of the main information, including whether dry-run mode was enabled, the release branch name and the Git SHA of the commit that was used to create the release branch.

![Screenshot of a detailed run of the first release workflow for 1.3.0-incubating](/img/release-guides/github-workflow-1-detail.png "Screenshot of a detailed run of the first release workflow for 1.3.0-incubating")

## Release candidate tag creation workflow
The second Github workflow to run is [`Release - 2 - Update version and Changelog for Release Candidate`](https://github.com/apache/polaris/actions/workflows/release-2-update-release-candidate.yml). This workflow will:
* Verify that all Github checks are green for the release branch.
* For RC0 only: update the project version files with the final version number, update the changelog, and commit the changes to the release branch.
* Create the `apache-polaris-[major].[minor].[patch]-incubating-rc[N]` tag

The rules to create the `[major].[minor].[patch]-rc[N]` tag are as follows:
* Find the last patch number for the `[major].[minor]` version by looking at existing tags, or 0 if no tag exists.
* Find the last RC number for the `[major].[minor].[patch]` version by looking at existing tags, or 0 if no tag exists.
* If a final release tag exists for the `[major].[minor].[patch]` version, then use `[major].[minor].[patch+1]-rc0`
* Else if an RC tag exists for the `[major].[minor].[patch]-rc[N]` version, then create `[major].[minor].[patch]-rc[N+1]`
* Else create `[major].[minor].[patch]-rc0`

This workflow can only be run from a `release/[major].[minor].x` branch. Selecting any other branch in the Github Actions UI will result in a failure.

![Screenshot of the second release workflow for 1.3.0-incubating](/img/release-guides/github-workflow-2.png "Screenshot of the second release workflow for 1.3.0-incubating")

Like for the other workflow runs, the run details page contains a recap of the main information, with all the steps that were executed.

![Screenshot of a detailed run of the second release workflow for 1.3.0-incubating](/img/release-guides/github-workflow-2-detail.png "Screenshot of a detailed run of the second release workflow for 1.3.0-incubating")

## RC≥1 only - Update release branch and create tag
If the first release candidate is rejected, additional code changes may be needed.

Each code change that should be added to the release branch must be cherry-picked from the main branch and proposed in a dedicated pull request. The pull request must be reviewed and approved before being merged. This step is mandatory so that Github runs the CI checks. The subsequent workflows will verify that those checks passed.

Once the pull requests have been merged, run the second workflow again to create a new RC tag. The workflow will automatically determine the next RC number.

## Build and publish release artifacts
The third Github workflow to run is [`Release - 3 - Build and publish release artifacts`](https://github.com/apache/polaris/actions/workflows/release-3-build-and-publish-artifacts.yml). This workflow will:
* Build the source and binary artifacts
* Stage artifacts to the Apache dist dev repository
* Publish the source and binary artifacts to the Apache Nexus repository
* Close the Apache Nexus repository
* Build Docker images for the server and the admin tool
* Build the Helm chart
* Create signature and checksum for all package files
* Copy package files to Apache dist dev repository

This workflow must be run from an RC tag (e.g., `apache-polaris-1.3.0-incubating-rc0`). Select the tag from the `Use workflow from` dropdown in the Github Actions UI. Selecting any other reference in the Github Actions UI will result in a failure.

![Screenshot of the third release workflow for 1.3.0-incubating](/img/release-guides/github-workflow-3.png "Screenshot of the third release workflow for 1.3.0-incubating")

## Start the vote thread

The last step for a release candidate is to create a VOTE thread on the dev mailing list.

Recommended title subject:

```
[VOTE] Release Apache Polaris [major].[minor].[patch] (rc[N])
```

Recommended content:

```
Hi everyone,

I propose that we release the following RC as the official Apache Polaris [major].[minor].[patch]
release.

* This corresponds to the tag: apache-polaris-[major].[minor].[patch]-incubating-rc[N]
* https://github.com/apache/polaris/commits/apache-polaris-[major].[minor].[patch]-incubating-rc[N]
* https://github.com/apache/polaris/tree/<SHA1>

The release tarball, signature, and checksums are here:
* https://dist.apache.org/repos/dist/dev/incubator/polaris/[major].[minor].[patch]-incubating

Helm charts are available on:
* https://dist.apache.org/repos/dist/dev/incubator/polaris/helm-chart/[major].[minor].[patch]-incubating/

NB: you have to build the Docker images locally in order to test Helm charts.

You can find the KEYS file here:
* https://downloads.apache.org/incubator/polaris/KEYS

Convenience binary artifacts are staged on Nexus. The Maven repositories URLs
are:
* https://repository.apache.org/content/repositories/orgapachepolaris-<ID>/

Please download, verify, and test according to the release verification guide, which can be found at
https://polaris.apache.org/community/release-verify/.

Please vote in the next 72 hours.

[ ] +1 Release this as Apache polaris [major].[minor].[patch]
[ ] +0
[ ] -1 Do not release this because...

Only PPMC members and mentors have binding votes, but other community members
are encouraged to cast non-binding votes. This vote will pass if there are 3
binding +1 votes and more binding +1 votes than -1 votes.

NB: if this vote passes, a new vote has to be started on the Incubator general
mailing list.
```

The next steps depend on the vote result.

## Close the vote thread
### If the vote failed
If the vote failed, run the Github workflow [`Release - X - Cancel Release Candidate After Vote Failure`](https://github.com/apache/polaris/actions/workflows/release-X-cancel-release-candidate.yml). This workflow will:
* Drop the Apache Nexus staging repository
* Delete the release artifacts (including Helm Chart) from the Apache dist dev repository
* Prepare an e-mail template to notify the community of the vote result

![Screenshot of the cancel RC workflow for 1.3.0-incubating](/img/release-guides/github-workflow-X.png "Screenshot of the cancel RC workflow for 1.3.0-incubating")

The run details page contains a recap of the main information, with all the steps that were executed.
It also contains the e-mail template to notify the community of the vote result.
Ensure to replace the `[REASON - TO BE FILLED BY RELEASE MANAGER]` placeholder with the actual reason for the vote failure.
Then send the e-mail to the Polaris dev mailing list.

![Screenshot of a detailed run of the cancel RC workflow for 1.3.0-incubating](/img/release-guides/github-workflow-X-detail.png "Screenshot of a detailed run of the cancel RC workflow for 1.3.0-incubating")

### If the vote passed
When the release candidate vote passes, send a new e-mail with the vote result:

```
[RESULT][VOTE] Release Apache Polaris [major].[minor].[patch] (rc[N])
```

```
Thanks everyone who participated in the vote for Release Apache Polaris [major].[minor].[patch] (rc[N]).

The vote result is:

+1: a (binding), b (non-binding)
+0: c (binding), d (non-binding)
-1: e (binding), f (non-binding)

A new vote is starting in the Apache Incubator general mailing list.
```

## Start a new vote thread on the Incubator general mailing list
As Polaris is an Apache Incubator project, you now have to start a new vote on the Apache Incubator general mailing list.

You have to send this email to general@incubator.apache.org:

```
[VOTE] Release Apache Polaris [major].[minor].[patch] (rc[N])
```

```
Hello everyone,

The Apache Polaris community has voted and approved the release of Apache Polaris [major].[minor].[patch] (rc[N]).
We now kindly request the IPMC members review and vote for this release.

Polaris community vote thread:
* https://lists.apache.org/thread/<VOTE THREAD>

Vote result thread:
* https://lists.apache.org/thread/<VOTE RESULT>

* This corresponds to the tag: apache-polaris-[major].[minor].[patch]-incubating-rc[N]
* https://github.com/apache/polaris/commits/apache-polaris-[major].[minor].[patch]-rc[N]
* https://github.com/apache/polaris/tree/<SHA1>

The release tarball, signature, and checksums are here:
* https://dist.apache.org/repos/dist/dev/incubator/polaris/[major].[minor].[patch]

Helm charts are available on:
* https://dist.apache.org/repos/dist/dev/incubator/polaris/helm-chart
NB: you have to build the Docker images locally in order to test Helm charts.

You can find the KEYS file here:
* https://downloads.apache.org/incubator/polaris/KEYS

Convenience binary artifacts are staged on Nexus. The Maven
repositories URLs are:
* https://repository.apache.org/content/repositories/orgapachepolaris-<ID>/

Please download, verify, and test according to the release verification guide, which can be found at
https://polaris.apache.org/community/release-verify/.

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

The next steps depend on the vote result.

## Close the vote thread on the Incubator general mailing list
### If the vote failed
When a release candidate is rejected during the IPMC vote, you need to send two e-mails to inform the community of the vote result.
First, reply in the same thread (in the general Incubator mailing list) with the vote result.

```
Hello everyone,

Thanks to all who participated in the vote for Release Apache Polaris [major].[minor].[patch] (rc[N]).

The vote failed due to [reason].

Thanks,
```

Then, run the Github workflow [`Release - X - Cancel Release Candidate After Vote Failure`](https://github.com/apache/polaris/actions/workflows/release-X-cancel-release-candidate.yml). This workflow will:
* Drop the Apache Nexus staging repository
* Delete the release artifacts (including Helm Chart) from the Apache dist dev repository
* Prepare an e-mail template to notify the community of the vote result

![Screenshot of the cancel RC workflow for 1.3.0-incubating](/img/release-guides/github-workflow-X.png "Screenshot of the cancel RC workflow for 1.3.0-incubating")

The run details page contains a recap of the main information, with all the steps that were executed.
It also contains the e-mail template to notify the community of the vote result.
Ensure to replace the `[REASON - TO BE FILLED BY RELEASE MANAGER]` placeholder with the actual reason for the vote failure.
Then send the e-mail to the Polaris dev mailing list.

![Screenshot of a detailed run of the cancel RC workflow for 1.3.0-incubating](/img/release-guides/github-workflow-X-detail.png "Screenshot of a detailed run of the cancel RC workflow for 1.3.0-incubating")

### If the vote passed
When the release candidate vote passes, send a new e-mail with the vote result:

```
[RESULT][VOTE] Release Apache Polaris [major].[minor].[patch] (rc[N])
```

```
Hello everyone,

The vote to release Apache Polaris [major].[minor].[patch] (rc[N]) has passed with [N] +1 binding and [M] +1 non-binding votes.

Binding +1 votes:
* [NAME]
* [NAME]
* [NAME]
* ...

Non-binding +1 votes:
* [NAME]
* ...

Vote thread: https://lists.apache.org/thread/<VOTE THREAD>

We will proceed with publishing the approved artifacts and sending out the announcement soon.
```

## Publish the release
The final workflow to run is [`Release - 4 - Publish Release After Vote Success`](https://github.com/apache/polaris/actions/workflows/release-4-publish-release.yml). This workflow will:
* Verify that the release branch HEAD matches the last RC tag
* Copy artifacts from the dist dev to the dist release SVN repository
* Update the Helm index in dist release repository accordingly
* Create a final release tag
* Rebuild and publish Docker images to Docker Hub
* Create a Github release with the release artifacts
* Release the candidate repository on Apache Nexus

This workflow can only be run from the `release/[major].[minor].x` branch for which a vote has passed. The workflow verifies that no commits have been added to the release branch since the last RC was created. It also requires the Nexus staging repository id (`orgapachepolaris-<ID>`) that was created by the previous workflow.

![Screenshot of the fourth release workflow for 1.3.0-incubating](/img/release-guides/github-workflow-4.png "Screenshot of the fourth release workflow for 1.3.0-incubating")

## Publish docs
These steps have not been automated yet.

1. Open a PR against branch [`versioned-docs`](https://github.com/apache/polaris/tree/versioned-docs) to publish the documentation
2. Open a PR against the `main` branch to update website
    - Add download links and release notes in [Download page](https://github.com/apache/polaris/blob/main/site/content/downloads/_index.md)
    - Add the release in the [website menu](https://github.com/apache/polaris/blob/main/site/hugo.yaml)
