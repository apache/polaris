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

# Distribution SBOMs

Polaris generates CycloneDX 1.6 JSON documents for its three Spark bundle JARs
and binary ZIP/TAR distributions. Quarkus produces application inventories;
the official CycloneDX Gradle plugin produces Spark inventories. Licensee and
existing attribution checks remain the policy authority and publication gates.

Run `:polaris-spark-3.5_2.12:finalizeDistributionSbom` (or either other Spark
variant) and `:polaris-distribution:assembleDistributionSboms` to generate them.
Ordinary `check` does not finalize SBOMs. Native Quarkus inventories remain
build-local inputs to the binary archive adapter; no separate application-tree
SBOM or descriptor is generated.

Spark documents select actual Shadow inputs, including first-party JARs, and use
the Maven `bundle` classifier. The final artifact SHA-256 identifies the complete
bundle; input hashes are separate properties. Inputs inside Iceberg's already
bundled JAR are not discovered by scanning it. Supported producer dependency edges
are retained; completeness is explicitly unknown. Producer fields unsupported by
the CycloneDX Java model, including `provides`, are not preserved.

Binary archive sidecars are named `polaris-bin-V.zip.cdx.json` and
`polaris-bin-V.tgz.cdx.json`. They hash the actual archive, check native application
component locations against the actual application bytes and archive contents, namespace references by
application, omit the explicitly excluded dependency-list file, and record other
packaged files with hashes. Shared dependencies in admin/server have distinct
occurrences. Existing producer relationships remain within each application;
no inferred relationships or complete-provenance claim are added. Native reported
hashes can differ from the assembled files in Quarkus 3.39.5; actual fast-JAR
bytes supply component hashes, with differing producer hashes kept as labelled
properties.

Spark SBOMs are attached to existing Maven publications with classifier
`bundle-sbom-cyclonedx`, extension `json`, and signed through existing signing.
The release workflow stages their same documents/signatures with ZIP/TAR sidecars
in the existing Apache distribution paths and includes them as GitHub assets.
Archive sidecars use existing distribution checksum/signature helpers.
Each Spark project's `stageSparkSboms` task collects its publication SBOM and
SHA-512 checksum under `build/spark-sboms/PROJECT`. Running `stageSparkSboms`
without a project prefix selects the task for all configured Spark variants.
With `-Prelease` or `-PsignArtifacts`, it also builds and collects their existing
Maven signatures. Each producer synchronizes its staging subdirectory; the release
workflow clears the staging area before building and copies the sidecars into
the existing Apache version directory.
No public endpoint or embedded SBOM is enabled. Container SBOMs are separate.

Every finalized SBOM links to the separately maintained CycloneDX VEX feed at
`https://polaris.apache.org/community/dependency-cves/polaris.vex.json`, using a
BOM-level `exploitability-statement` external reference. Generation writes only
the URL; it does not fetch or embed assessments, or pin the mutable feed's hash.
The link makes VEX discoverable. Consumers must establish product/version
applicability before applying an assessment; the link does not itself establish
that a finding can be suppressed, including for Spark artifacts.

Verify the sidecar's detached signature against a trusted Polaris release key,
then compare `metadata.component.hashes` SHA-256 with the downloaded JAR/ZIP/TAR.
A signature authenticates the document; the subject hash binds it to those bytes.

## Implementation and portability

`gradle/license-check-plugin` keeps CycloneDX/Licensee dependencies on the private
applied-script classpath. `SbomCore`, `FinalizeDistributionSbom`, `SbomSubject`
and `PackageKind` come from the Nessie implementation with Polaris identity
adaptations. `DistributionSbom` wires Polaris producers/publications;
`ArchiveSbom` and `FinalizeBinaryArchiveSbom` cover its binary archive layout.
Only Gradle providers and plain maps cross the Shadow/build-logic boundary.
Absolute paths in Shadow input metadata limit cross-checkout build-cache reuse;
ordinary configuration-cache reuse is supported.
