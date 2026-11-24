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
title: CVE Status for Dependencies
linkTitle: CVE Status for Dependencies
type: docs
weight: 510
no_list: true
outputs:
  - HTML
  - cyclonedx
  - openvex
---

Apache Polaris depends on many third-party libraries. Security scanners routinely flag CVEs in those
libraries, but a CVE in a dependency does not automatically mean that Polaris is vulnerable. That
depends on whether Polaris ships the affected code, and whether the vulnerable behavior can be
exploited through Polaris (see the
[Polaris security threat model](https://github.com/apache/polaris/blob/main/SECURITY-THREAT-MODEL.md#upstream-dependencies)).

We publish our assessment of dependency CVEs as machine-readable
[VEX (Vulnerability Exploitability eXchange)](https://cyclonedx.org/capabilities/vex/) documents.
VEX lets a project state whether a CVE applies to its product, and why. The assessment is available
in both [CycloneDX 1.6](https://cyclonedx.org/) and [OpenVEX](https://openvex.dev/) JSON formats
(download links below). It covers the binary distributions and container images published by the
Polaris project. Custom builds, for example with optional extensions such as Hive federation, can
include other dependencies.

If your scanner supports VEX, point it at one of these files to suppress the findings that do not
apply to Polaris. If it does not, use the table below to triage flagged CVEs manually.

For example, [Docker Scout](https://docs.docker.com/scout/) can apply the OpenVEX file when scanning
the Polaris container image:

```bash
curl -sO https://polaris.apache.org/community/dependency-cves/polaris.openvex.json
docker scout cves apache/polaris:1.8.0 --vex-location polaris.openvex.json --vex-author '.*'
```

CVEs marked `not_affected` are dropped from the results, while the others remain. The Polaris VEX
documents are authored by the Apache Polaris project, so `--vex-author '.*'` is required: by default,
Docker Scout only trusts VEX statements authored by `*@docker.com`.

To report a vulnerability in Polaris itself, follow the [security reporting procedure](../security-report/).

{{< dependency-cves >}}
