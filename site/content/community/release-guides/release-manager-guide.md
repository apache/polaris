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
linkTitle: Release Manager Role
title: Role of the Release Manager in Apache Polaris
type: docs
weight: 400
---

As our community continues to grow, we’ve seen interest from contributors and new committers in understanding the
mechanics behind an Apache Polaris release.

In the Apache Software Foundation (ASF), **a release is not just a technical milestone, but a very formal "Act of the
Foundation" that carries legal weight and requires specific community governance**.

The Release Manager (RM) is the individual who volunteers to shepherd this process. To help those unfamiliar with the
role, here is a guide focused on release management in Apache Polaris with a bunch of background information.

{{< alert warning >}}
This write-up cannot and does not cover all details and can only serve as an informal and "brief" introduction.
You can find more information in the contents of the linked pages.
The ASF provides a lot of additional relevant content on this topic.
{{< /alert >}}

{{< alert note >}}
This page adds more details to the _Release manager_ paragraph on the [_Release Guides_ page](../#release-manager).
{{< /alert >}}

## What is a Release Manager?

The RM is a committer who takes **responsibility** for the **mechanics** of a release.
You aren't "in charge" of what goes into the release (that's a community decision), but you are the **facilitator**
ensuring that the release process follows the [_ASF Release Policy_](https://www.apache.org/legal/release-policy.html).

As the whole release process is a very formal act, it is strictly required to follow the _ASF Release Policy_.
Not following the policy can lead to “failed” releases or even more serious consequences.
Especially the legal aspects, most prominently the presence and content of all the LICENSE and NOTICE files but also the
contents of all the formal emails, must be strictly followed.

## The Polaris "Semi-Automated" Workflow

While the ASF has historically relied on complex manual scripts, Apache Polaris uses a [_Semi-Automated Release
Guide_](../semi-automated-release-guide/) powered by GitHub Actions, we also call it "_Releasey_".

For a better understanding of what happens “under the covers,” read the [_Manual Release
Guide_](../manual-release-guide/) first to understand the underlying steps, but perform the release using the
_Semi-Automated Guide_ to minimize human error.

## Background

The release process at the ASF is not formal and complex for the sake of having something that is "brutally formal,"
but for legal and compliance reasons.

Every email related to an official release and quite some pieces of the official release artifact(s) are subject to
this formal process carrying quite a legal weight. The following sections provide an overview of the critical pieces.

A primary responsibility of the Release Manager is to ensure that the project complies with the ASF’s legal policies.
Every release is an offering of the foundation, and failure to comply with licensing requirements can expose the foundation to legal risk.

### Mandatory Release Artifacts

Every Apache release package must contain two critical files at the root of the distribution: `LICENSE` and `NOTICE`.
This includes all artifacts published by the project, the source tarball as well as all convenience binary artifacts.

1. **LICENSE**
   This file must contain the full text of the Apache License 2.0.
   If the release bundles third-party software under different licenses (e.g., BSD or MIT), the RM must append the full text of those licenses to the `LICENSE` file.
2. **NOTICE**
   This file provides mandatory attributions.
   It must always include the standard ASF attribution: _"This product includes software developed at The Apache Software Foundation (http://www.apache.org/)."_
   If any bundled third-party libraries require their own attribution notices, those must be added here.
3. **DISCLAIMER** (only for Apache Incubator podlings)
   This file must contain the content as mentioned on the [_Apache Incubation Policy_](https://incubator.apache.org/policy/incubation.html).
   This file must **not** be present for Apache top level projects like Apache Polaris.

The RM must also ensure that every source file contains the standard ASF license header
and identify unauthorized binary files in the source distribution.
Apache Polaris uses [Apache RAT](https://creadur.apache.org/rat/) (Release Audit Tool) to automate the checking of headers in CI.

### Audit of Dependencies

Apache projects are [generally permitted](https://www.apache.org/legal/resolved.html) to include software with
compatible, permissive licenses.
However, they are strictly prohibited from including software with 
[restrictive or unapproved licenses](https://www.apache.org/legal/resolved.html#category-x),
such as GPL or AGPL, without _explicit_ board approval.
The RM must audit the project's dependency tree and ensure that no non-compliant code is bundled into the release.

{{< alert warning >}}
Although the CI automatically audits the dependency tree of the Polaris admin-tool and Polaris server,
it is just a best effort and does not guarantee compliance!
Especially so-called "uber-jars" or "shaded jars" may contain non-compliant code.
{{< /alert >}}

### Binary Files

Source releases should not contain any compiled code.
Binary executables are practically banned from the Polaris source tree and must never be added.
Other binary artifacts should be avoided, if possible, and only used in exceptional cases or if there is no alternative,
for example, for some image formats.

### Managing the Vote Thread

The RM initiates the process by sending a \[VOTE\] email to the dev@ mailing list.
This email is the legal record of the proposal and must contain all information necessary for a voter to make an informed decision.

* **Links to Artifacts**:
  Direct links to the staged source package and its signature.
  Should also contain links to the staged convenience binaries and their signatures.
* **Revision/Commit**: The specific Git commit hash and Git tag being voted upon.
* **Release Notes**: A summary of the changes included in the release.
* **Verification Instructions**:
  Encourage community members to use the [_Release Verification Guide_](../release-verification-guide/) to check
  reproducibility, signatures and checksums before voting.

#### Vote Tabulation and Resolution

After the 72-hour period, the RM tabulates the results.
A successful vote requires at least **three binding +1** votes from PMC members and more positive than negative binding votes.
If a vote receives a -1, it is not a veto (unlike code modification votes), but it usually signals a blocking issue that must be addressed.
In most cases, the community chooses to cancel the vote, fix the issue, and roll a new RC.

| Vote Type           | Definition           | Significance                                 |
|---------------------|----------------------|----------------------------------------------|
| **+1** (Approve)    | Positive endorsement | Binding if from a PMC member                 |
| **0** (Neutral)     | No objection         | Does not count toward the quorum of three    |
| **-1** (Disapprove) | Identified blocker   | RM should investigate and potentially cancel |

The RM then sends a \[VOTE\]\[RESULT\] email summarizing the binding and non-binding votes and confirming whether the release has passed.

### Apache Releases

The definition of a release within the ASF is significantly broader than in many other software contexts.
Generically, a release is any publication of software artifacts beyond the group that owns it, which in the Apache context means anyone outside the project's development community.
If a project instructs the general public to download a package, that package has been released, regardless of whether it is labeled as "alpha," "beta," or "stable."

Official Apache releases are characterized by a focus on source code.
While the foundation permits the distribution of convenience binaries, such as JAR files, Python wheels,
or Docker images, the source package is the only artifact that constitutes the formal release.
This distinction is vital for long-term project sustainability and legal protection, as it ensures that users always have access to the underlying logic and can build the software independently of the project's specific build infrastructure.

{{< alert warning >}}
**Projects are
[strictly prohibited from directing outsiders toward unofficial packages](https://www.apache.org/legal/release-policy.html#publication),
such as nightly builds or release candidates, through the project's primary website or download pages**.
{{< /alert >}}

#### Taxonomy of Distributed Packages

To maintain clarity for users and protect the foundation's brand, the ASF distinguishes between several types of distributions, each with its own governance requirements.

| Distribution Type          | Audience           | Policy Requirement                       | Official Status                     |
|----------------------------|--------------------|------------------------------------------|-------------------------------------|
| **Official Release**       | General Public     | Full PMC Vote (3+ binding +1s)           | Formal Act of the Foundation        |
| **Release Candidate (RC)** | Developers/Testers | No public links; internal testing only   | Unofficial; proposed for approval   |
| **Nightly Build**          | Automated Testers  | Built from trunk/branch; no public links | Unofficial; regression testing only |
| **Snapshots**              | Developers         | Ongoing development testing              | Unofficial; unstable by definition  |
| **Test Packages**          | Developers         | Discussion limited to dev lists          | Unofficial; ad-hoc testing          |

### Communication and Standards

The final duty of the Release Manager is to announce the release to the community.
This is a critical step for user adoption and maintainers of downstream projects.

Announcements are sent to the project's user@ and dev@ lists.
For major releases, an announcement should also be sent to `announce@apache.org`.

The Release Manager operates as a representative of the project and the foundation.
As such, all interactions on public mailing lists must adhere to the [_ASF Code of Conduct_](https://www.apache.org/foundation/policies/conduct),
which applies to all communication by everyone at the ASF.

### Netiquette

Release-related emails must follow several "netiquette" and technical rules.

Generally applicable to all communication on the mailing lists:

* **Plain Text Only**
  HTML emails are frequently rejected by ASF spam filters and are less accessible to some users.
* **No Attachments**
  Email attachments are often rejected because they are not easily reproducible and can lead to confusion.
* **Concise Subject Lines**
  Use a clear format like "_\[ANNOUNCE\] Apache ProjectName X.Y.Z released_".

Release-related emails:

* **Project Blurb**
  Because subscribers to announce@apache.org may not be familiar with the project, the RM should include a 3-5 line description of what the software does.
* **Signature Verification**
  The announcement should include links to the PGP signature and the project's KEYS file, along with a brief explanation of how to verify the release.

### Website and Metadata Updates

Beyond mailing lists, the project's digital presence must be updated:

* **Project Website**
  Update the download page with the new links and the documentation page with the new version’s guides.
* **Apache Reporter System**
  Log the release in [reporter.apache.org](https://reporter.apache.org/) (PMC members only).
  This system aggregates release data for use in quarterly reports to the ASF Board.
* **Social Media and Blogs**
  Many projects (like Flink and Spark) utilize blog posts to highlight major new features and recognize the contributors who made the release possible.

### Communication Etiquette

While especially important for the release related communication, all interactions with the community should
follow these guidelines:

* **Avoid Personal Attacks**
  Focus on technical and procedural issues rather than individual contributors.
* **Practice Patience**
  Remember that most ASF participants are volunteers.
  A delay in a vote response or a bug fix should be handled with professional courtesy.
* **Public Accountability**
  All important decisions and discussions regarding a release must occur on the public dev@ mailing list.
  The "Apache Motto" _If it didn't happen on a mailing list, it didn't happen_ is especially relevant to the release process.
* **Bias for Action**
  The RM should be "brutal" in triaging issues.
  If a bug is not a blocker, it should be rescheduled for a future release to avoid stalling the current release's progress.

### Security Management and Vulnerability Handling

The ASF handles security through a private, coordinated disclosure process to protect users.
Potential vulnerabilities are (reported)[../security-report/] to the project's private mailing list
`private@polaris.apache.org` or to `security@apache.org`, the latter is preferred for outsiders of the project.
This work is always done in a private environment to prevent "zero-day" exploitations until the fix has been
released via an [official release](#taxonomy-of-distributed-packages).

## Step-by-Step Release Process

### Preparation

1. Start a [\[DISCUSS\] thread](../semi-automated-release-guide/#announce-the-intent-to-cut-a-release)
   on the dev@ mailing list to propose the release and discuss major issues and pull requests,
   gathering consensus on the contents of the release.
2. Leverage the GitHub milestone for the version to be released to organize the issues and pull requests.
3. As the Polaris project follows a “release train model,” consider moving features that are not yet ready to the
   milestone of the **next** release.
4. Verify the contents of the [CHANGELOG.md](https://github.com/apache/polaris/blob/main/CHANGELOG.md). Make sure the
   content is updated, if necessary.

### Consensus & Branching

1. Send an informal message to the \[DISCUSS\] thread on the dev@ mailing list to ensure the community is ready for a
   release, stating that you are about to create the release branch.
2. Once there is consensus, let “Releasey” [create the release branch](../semi-automated-release-guide/#release-branch-creation-workflow)
   (e.g., `release/1.4.x`) directly on the [`apache/polaris`](https://github.com/apache/polaris) repository.

### Amendments to the release branch

1. Generally, the contents of the release branch should be as close as possible to the contents of the base branch
   (usually the "main" branch).
2. All release branches are “protected branches” in GitHub and adhere to the same protection rules as the “main” branch.
   All changes to a release branch must happen via GitHub pull requests.

### Creating the release candidate

Instead of performing a lot of manual steps, you will [trigger](../semi-automated-release-guide/#release-candidate-tag-creation-workflow)
the “Releasey” [workflows](../semi-automated-release-guide/#build-and-publish-release-artifacts)
using GitHub Actions that:

* Build the source and binary distributions.
* Generate SHA-512 checksums and GPG signatures.
* Stage Maven artifacts in the Apache Nexus staging repository.
* Publish Docker images and Helm charts.

### The Community Vote

1. **The 72-Hour Rule**:
   Once artifacts are staged, send a [\[VOTE\] email](../semi-automated-release-guide/#start-the-vote-thread)
   to `dev@polaris.apache.org`.
   The vote must remain open for **at least 72 hours**.
2. **Quorum**:
   You need at least **three binding +1 votes**
   There are no “vetos” for votes on releases, but -1s from anyone will be carefully considered and, if technically or
   legally justified, lead to a cancellation of the release vote.
3. **Verification**:
   Encourage community members to use the [_Release Verification Guide_](../release-verification-guide/) to check
   signatures and checksums before voting.

### Finalization

In **any case**, send a [\[VOTE\]\[RESULT\] email](../semi-automated-release-guide/#close-the-vote-thread)
stating the result of the vote, summarizing the binding and non-binding votes.

If the vote passes:

1. **Publish the release**:
   Leverage the “Releasey” workflows to eventually [publish](../semi-automated-release-guide/#publish-the-release)
   the legally important source release, the convenience binary artifacts and create the convenience release on GitHub.
2. **Announce**:
   Send the [ANNOUNCE] email to announce@apache.org and the Polaris dev/user lists.
3. **Perform the "post-release steps"**
   Not all tasks have been automated in Releasey yet.
    1. **Update the website**
       Consult the _Semi Automated Release-Guide_ for [details](../semi-automated-release-guide/#publish-docs).
    2. **Close the GitHub milestone**
       The milestone for the release is now “obsolete” and should be closed but **not** deleted.

## Why do we do this?

This process ensures that every artifact we distribute is verified, immutable, and legally endorsed by the Foundation.
By using our semi-automated tools, we ensure these high standards are met with "little manual intervention."
