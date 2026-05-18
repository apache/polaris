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

# Apache Polaris Threat Model

## Purpose

This document defines the security boundaries for Apache Polaris. It is intended
to guide maintainers, security reviewers, and automated analysis tools when
evaluating potential vulnerabilities.

Use this document to decide whether a finding affects a protected Polaris asset,
crosses a Polaris trust boundary, violates a Polaris security invariant, and
should be handled as a Polaris security issue.

This document is guidance for analysis and triage. It does not make policy
decisions, accept or reject vulnerability reports, assign ASF severity, allocate
CVEs, or determine disclosure handling. Human project and ASF security review is
required for those decisions.

## Scope

Apache Polaris is a catalog service for managing Apache Iceberg catalogs,
namespaces, tables, views, principals, roles, policies, and related metadata.

This threat model covers:

- Polaris server and runtime components.
- REST APIs exposed by Polaris.
- Authentication and authorization behavior.
- Catalog metadata access and mutation.
- Persistence-layer interactions.
- Integration points with storage, catalog, policy, and identity backends where
  Polaris makes authentication, authorization, or trust decisions.

This threat model does not cover:

- Vulnerabilities in client applications using Polaris incorrectly.
- Compromise of the underlying database, object store, identity provider,
  container platform, host operating system, or deployment platform.
- Denial of service caused solely by insufficient infrastructure sizing.
- Bugs in third-party dependencies unless Polaris exposes them through unsafe
  configuration or usage.

## Component Families

Polaris has several component families with different entry points, deployment
models, and trust boundaries:

| Component family | Representative entry points | Deployment model | Threat-model scope |
| --- | --- | --- | --- |
| Polaris server and runtime | Management APIs, catalog APIs, service runtime | Long-running service | In scope for authentication, authorization, metadata, persistence, storage, policy, and credential-handling decisions. |
| Polaris admin tool | Administrative CLI commands and generated local profiles | Operator tool | In scope when handling credentials, configuration, administration, logs, or generated artifacts. |
| Python CLI under `client/python/` | Client commands, local configuration, command output | User or operator CLI | In scope when handling credentials, tokens, catalog metadata, local profiles, logs, or generated artifacts. |
| Release artifacts | Source release, runnable tarballs, container images, Helm charts | Distribution and deployment | In scope for packaged defaults, included modules, enabled features, generated configuration, and documented deployment paths. |
| Reusable modules | `polaris-core`, `polaris-runtime-service`, extension modules, and selected Gradle project combinations | Embedded or customized downstream use | In scope when Polaris code makes security decisions; downstream-only integration code is evaluated separately. |
| Optional integrations | Persistence backends, authentication modes, authorization services, storage providers, federation, and optional extensions | Runtime-selected or build-enabled features | In scope for coherent supported configurations; findings must identify the active variant and relevant configuration. |
| Related Polaris tools | Tools published from `apache/polaris-tools` and referenced by the Polaris project or website | Developer, operator, migration, synchronization, MCP, benchmark, or web UI tools | Evaluated with tool-specific audience, deployment model, protected assets, and trust boundaries; not automatically Polaris server vulnerabilities. |

## System Overview

Polaris accepts requests from clients, authenticates callers through configured
authentication mechanisms, authorizes operations against Polaris roles,
privileges, policies, and catalog metadata, and persists metadata through the
configured persistence backend.

Polaris may integrate with external systems, including identity providers,
object stores, policy decision points, catalog backends, and deployment
infrastructure. These systems can affect Polaris security when Polaris relies on
their output for authentication, authorization, routing, or persistence
decisions.

## Actors And Roles

- Anonymous caller: A caller without valid authentication.
- Authenticated principal: A valid user or service principal known to Polaris.
- Catalog user: An authenticated principal with privileges on one or more
  catalog resources.
- Catalog administrator: A principal with administrative privileges over a
  catalog.
- Realm administrator: A principal with administrative privileges over the
  Polaris realm.
- Deployment operator: A person or system with access to deploy, configure, or
  operate Polaris infrastructure.
- External identity provider: A trusted authentication authority configured by
  the deployment.
- External policy decision point: A configured authorization service that may
  participate in Polaris access decisions.
- Persistence backend: The configured database or storage system used by
  Polaris.

## Protected Assets

Polaris treats the following as security-sensitive:

- Authentication credentials, bearer tokens, client secrets, signing keys, and
  refresh tokens.
- Principal, role, privilege, and policy metadata.
- Principal, principal-role, and catalog-role names when a deployment treats
  identity or role names as sensitive or personal data.
- Catalog, namespace, table, and view metadata whose visibility is governed by
  Polaris authorization.
- Storage locations, table locations, metadata locations, manifest locations,
  statistics locations, and other URI-bearing metadata that define where
  catalog-managed data or metadata resides.
- Temporary or delegated storage credentials, scoped storage policies,
  credential access boundaries, session policies, and provider-specific policy
  expressions.
- Configuration values that affect authentication, authorization, token
  validation, policy evaluation, credential vending, storage boundaries,
  federation, or backend connectivity.
- Audit-relevant request identity and authorization context.

## Trust Boundaries

Polaris assumes the following boundaries:

- Network callers are untrusted until authenticated.
- Authenticated principals are not inherently trusted to access resources; every
  protected operation must pass authorization.
- Request-provided identifiers, catalog names, namespace names, table names,
  view names, role names, policy names, and principal names are untrusted input.
- Identity-provider claims are trusted only after token validation according to
  the configured authentication mechanism.
- External policy decisions are trusted only according to the configured policy
  integration and the request context Polaris supplies to that integration.
- Management-plane APIs, catalog data-plane APIs, admin tools, and client tools
  may expose different metadata and require separate authorization checks.
- Catalog properties, entity properties, and configuration values may cross from
  management-plane configuration into data-plane client-visible responses; they
  must be classified by intended visibility before storing sensitive values.
- Credential vending crosses from Polaris authorization into object-store or
  external-system authorization; delegated credentials must be scoped to the
  authorized actor, operation, and storage locations.
- Storage locations and URI-bearing metadata cross from Polaris metadata into
  object stores, metastores, filesystems, or external catalogs; caller-supplied
  locations are untrusted until validated against the effective storage policy.
- Provider-specific storage policy expressions, IAM policies, access boundaries,
  and session policies are security boundaries and must safely encode
  caller-controlled identifier or path material.
- Externally configured endpoints, federation targets, identity-provider
  metadata, object-store endpoints, and catalog backends are trusted only for
  the configured purpose and must not silently redirect secrets or privileged
  requests outside that trust relationship.
- Persistence backends are trusted to store and return data, but persistence-layer
  access by internal Polaris callsites is not by itself proof that the data may be
  returned to an external caller or used to mutate protected state. Authorization
  must be enforced before protected data is exposed or protected state is changed.
- Deployment operators are trusted with configuration and infrastructure-level
  secrets.

## Security Invariants

The following properties must hold:

- Protected API operations require authentication unless explicitly documented as
  public.
- Authorization checks must be performed before returning or mutating protected
  catalog metadata.
- Grant and role-management operations must enforce the intended grant authority
  defined by Polaris authorization rules. A principal must not be able to obtain
  privileges that direct checks would reject by using indirect role creation,
  self-grants, role nesting, privilege delegation, or other second-order effects.
- Role, privilege, and policy changes must not bypass scope restrictions.
- Realm, catalog, namespace, table, and view identifiers must not allow access
  across authorization boundaries.
- Token issuance, token exchange, credential reset, and credential rotation must
  preserve the intended principal, realm, role scope, expiration, and revocation
  semantics.
- Tokens, credentials, and secrets must not be logged, returned in API
  responses, or persisted in plaintext unless explicitly required and protected
  by deployment controls.
- Properties and configuration values that are returned to clients must be
  treated as client-visible and must not be used as a secret store.
- Storage locations and URI-bearing metadata must be validated against the
  effective catalog, namespace, table, and storage-policy boundaries before they
  are persisted, used to access storage, or used to mint delegated credentials.
- Credential vending must not occur before the relevant location, authorization,
  and scope checks have completed.
- Temporary storage credentials must be scoped as narrowly as the configured
  storage provider and documented mode allow. Provider-specific limitations that
  broaden scope must be explicit to operators.
- Within the applicable realm and configured storage-policy scope, reused,
  overlapping, or ambiguous storage locations must not create an unintended
  authorization bypass. Explicitly configured overlap modes, table clones, and
  documented credential-vending scope limitations should be treated according to
  their documented behavior, not reported solely because overlap exists.
- Provider policy documents and expressions must escape or otherwise safely
  encode caller-controlled identifiers, paths, and property values.
- User-controlled input must not be used to construct SQL, filesystem paths,
  object-store paths, URLs, or process commands without appropriate validation or
  safe APIs.
- Error messages must not disclose secrets or unauthorized metadata.
- Existence, names, and relationships of principals, roles, catalogs,
  namespaces, tables, views, and policies must not be disclosed to unauthorized
  callers unless that disclosure is explicitly documented and accepted.
- Configuration changes that create, reset, rotate, store, or delete credentials
  or secrets must be authorized, consistently applied, and must not leave stale
  credentials or orphaned secrets with unintended access.
- Internal and administrative APIs must not be exposed as public unauthenticated
  APIs.
- Optional build features, extensions, and distribution-specific packaging must
  not weaken Polaris authentication, authorization, credential handling, or
  metadata isolation without explicit configuration and documentation.

## Security Issues

The following should generally be treated as potential security vulnerabilities:

- Authentication bypass.
- Authorization bypass or privilege escalation.
- Cross-catalog or cross-realm data access caused by missing checks.
- Unauthorized mutation of catalog metadata, roles, policies, or principals.
- Exposure of credentials, tokens, secrets, or sensitive configuration.
- Exposure of secrets through client-visible properties, configuration
  responses, logs, profiles, command output, browser storage, or generated
  artifacts.
- Credential-vending behavior that grants broader storage access than the
  caller's authorized operation or effective storage boundary.
- Storage-location validation bypasses, including unvalidated URI-bearing table
  metadata, reused or overlapping locations, and provider-policy construction
  issues.
- Token or credential lifecycle issues that allow access to continue beyond the
  intended reset, rotation, revocation, realm, or scope boundary.
- Unauthorized disclosure of identity, role, policy, catalog, namespace, table,
  or view existence when that existence is itself sensitive.
- SQL injection, command injection, unsafe deserialization, server-side request
  forgery, path traversal, or template injection reachable through untrusted
  input.
- Unsafe use of externally configured endpoints that can redirect privileged
  traffic, credentials, tokens, metadata, or storage requests outside the
  intended trust relationship.
- Logging or returning sensitive values to unauthorized users.
- Use of weak cryptographic validation for authentication or authorization
  decisions.
- Insecure defaults that expose protected APIs or disable required security
  checks.

## Non-Issues And Deployment Responsibilities

The following are not normally treated as Polaris vulnerabilities by themselves:

- A deployment operator choosing weak credentials or exposing the service
  publicly without required network controls.
- A compromised administrator using their legitimate administrative privileges.
- A compromised database, object store, Kubernetes cluster, host operating
  system, identity provider, or external policy decision point.
- Lack of rate limiting unless it enables a concrete security impact beyond
  resource exhaustion.
- Information visible to a principal that is explicitly authorized to access it.
- Storage-provider limitations that are accurately documented and do not give
  Polaris callers more access than the deployment intentionally configured.
- Test-only code, local development defaults, or example configuration not used
  in production paths, unless clearly reachable in production builds.

Polaris does not, by itself, provide the following security properties:

- Protection after compromise of deployment infrastructure, persistence
  backends, object stores, identity providers, external policy decision points,
  Kubernetes clusters, hosts, or administrator workstations.
- Protection against authorized administrators using their legitimate privileges
  in ways the deployment does not intend.
- Network isolation, TLS termination, firewalling, ingress policy, Kubernetes
  policy, or cloud-account isolation beyond what the deployment provides.
- Secret management for values that operators place outside Polaris-controlled
  secret storage or configuration mechanisms.
- Stronger delegated-credential isolation than the selected storage provider,
  configured policy model, and documented deployment mode can support.
- Safety of arbitrary downstream packaging, module combinations, integrations,
  or local patches that are not part of an official Polaris artifact or
  documented supported configuration.

Deployment operators and downstream integrators are responsible for:

- Protecting service credentials, signing keys, client secrets, object-store
  credentials, identity-provider configuration, policy-service credentials, and
  local CLI or admin-tool profiles.
- Configuring TLS, network exposure, ingress, firewalls, service accounts,
  Kubernetes permissions, cloud IAM, object-store policy, and persistence-backend
  access according to the deployment's security requirements.
- Choosing production-appropriate authentication, authorization, persistence,
  storage, and policy-service settings before exposing Polaris to untrusted
  clients.
- Avoiding production use of local-development defaults, sample credentials,
  test fixtures, example configuration, and benchmark-only tooling unless those
  choices are explicitly intended and protected.
- Rotating and revoking credentials, tokens, secrets, and delegated access
  according to the deployment's operational requirements.
- Protecting logs, generated configuration, local profiles, backups, database
  snapshots, object-store metadata, and other artifacts that may contain
  security-sensitive values.

The following patterns often require careful triage because they can be
vulnerabilities, documentation hardening items, deployment responsibilities, or
false positives depending on the actor, configuration, and reachable path:

- Treating client-visible properties, catalog metadata, generated configuration,
  local profiles, command output, browser storage, or logs as secret stores.
- Assuming delegated storage credentials are narrower than the configured
  provider policy, access-boundary mechanism, or documented deployment mode
  actually supports.
- Assuming every custom build, Gradle project combination, optional dependency,
  extension, or downstream package is an official supported Polaris artifact.
- Applying server threat-model assumptions unchanged to CLIs, web UIs,
  migration tools, MCP servers, synchronization tools, benchmarks, or other
  related tools.
- Treating test fixtures, mocked trust decisions, direct internal object
  construction, or already-authorized access as proof of a production
  vulnerability.
- Treating dependency advisories as Polaris vulnerabilities without showing that
  the vulnerable behavior is present, reachable, and crosses a Polaris trust
  boundary.

Known non-findings are cases already covered by the non-issues, dependency,
variant, distribution, and related-tool sections above. Common examples include
authorized access, unreachable dependency advisories, test-only behavior,
compromised deployment infrastructure, and impossible variant combinations.

## Consumption And Distribution Boundaries

Users consume Polaris in several ways, including:

- Release tarballs containing the runnable Quarkus applications for the Polaris
  server and Polaris admin tool.
- Container images for the Polaris server and Polaris admin tool.
- Helm charts for deploying the Polaris server.
- The Python-based CLI tool under `client/`.
- Custom applications or distributions built from Polaris modules such as
  `polaris-core`, `polaris-runtime-service`, extension modules, or other Gradle
  project combinations.

The Polaris threat model applies directly to artifacts and modules produced by
the Apache Polaris project. It also applies to custom distributions when they use
Polaris code to make authentication, authorization, catalog metadata, policy, or
credential-handling decisions.

Custom distributions can change the effective threat model. When evaluating a
finding, determine whether the behavior is present in:

- An official Polaris release artifact.
- An official Polaris source release built with documented build options.
- An official Polaris container image.
- An official Polaris Helm chart.
- The Polaris CLI.
- A reusable production or library module intended for external consumption.
- An optional extension or dependency enabled by build configuration.
- Downstream-only integration code or deployment packaging.

A finding in downstream-only code, private packaging, custom deployment
configuration, or third-party integration logic is not automatically a Polaris
vulnerability. Findings in custom distributions or downstream integrations may
still be useful to report when they indicate a weakness in Polaris code,
supported configuration, documented integration guidance, reusable modules,
extension points, or official release artifacts. Such findings are evaluated case
by case and are not automatically accepted as Polaris vulnerabilities.

Building a custom distribution from an official Polaris source release can help
establish that the affected code originates from Polaris, but it does not by
itself make every selected module combination, optional dependency, Gradle
property, packaging choice, or deployment configuration an official supported
artifact.

The severity and handling of a finding depend on the affected actor, protected
asset, trust boundary, exploitability, default exposure, affected official
artifacts, documented usage, and whether Polaris code or configuration is
responsible for the security decision.

Findings against unreleased code, including development branches such as `main`,
may still be useful to report, but they are not automatically treated the same as
findings affecting released Polaris artifacts. Handling depends on whether the
issue affects released code, is likely to ship, or indicates a broader weakness
in Polaris code, defaults, extension points, or documented guidance.

## Backend And Authorization Variants

Official Polaris artifacts may include multiple backend, authentication,
authorization, storage, and extension implementations. Automated analysis should
consider all implementations present in the artifact, but concrete findings must
identify the coherent runtime configuration under which the behavior is
reachable.

The presence of multiple implementations in one artifact does not mean their
runtime assumptions can be freely combined. Do not mix assumptions from
different persistence backends, authentication modes, authorization services,
storage providers, optional extensions, or deployment profiles unless the
combination is supported and reachable in one deployment.

Polaris behavior depends on configured persistence, authentication, and
authorization components. Automated analysis must identify the active variant
before assessing reachability, impact, and responsibility.

Persistence backends may include JDBC, in-memory, NoSQL, test fixtures, or other
implementations. A finding may be backend-specific when it depends on
transaction semantics, consistency, uniqueness constraints, secret storage,
query behavior, serialization, indexing, cleanup behavior, or backend-specific
configuration.

Authentication modes may include internal Polaris principals and secrets,
external OIDC identity providers, or mixed configurations. A finding may be
authentication-mode-specific when it depends on token validation, claim mapping,
realm selection, tenant mapping, credential reset, client secrets, token
exchange, or principal provisioning.

Authorization may be enforced through internal Polaris RBAC, external policy
decision points, Ranger, OPA, or mixed decision paths. A finding may be
authorization-mode-specific when it depends on privilege derivation, request
context passed to an external service, policy fallback behavior, deny/allow
precedence, fail-open or fail-closed behavior, caching, or mismatches between
internal and external decisions.

When evaluating a finding, identify:

- The persistence backend involved.
- The authentication mode involved.
- The authorization service or model involved.
- Whether the behavior is common across variants or variant-specific.
- Whether unsupported, test-only, or development-only variants are required.
- Whether a production-supported configuration is affected.

A finding may be valid when it affects:

- All variants.
- One specific supported variant.
- A documented combination of variants.
- An optional implementation present in an official artifact but activated only
  by configuration.

A finding is weaker, and may be a false positive, when it requires combining
mutually exclusive runtime assumptions or test-only behavior that is not
reachable in a supported deployment.

Security invariants must hold consistently across supported variants unless a
variant-specific limitation is explicitly documented.

External authorization integrations must receive enough request context to make
the intended decision and must fail according to documented fail-open or
fail-closed behavior.

Internal and external authorization paths must not disagree in a way that grants
access broader than the configured policy intends.

Persistence backends must preserve authorization-relevant invariants such as
principal identity, role grants, privilege scope, catalog boundaries, credential
state, and entity uniqueness.

## Related Polaris Tools

Related tools maintained by the Apache Polaris project may affect Polaris
security when they are released, documented for user or operator use, handle
Polaris credentials or metadata, expose reachable interfaces, or can affect a
Polaris deployment through administration, migration, synchronization,
credential handling, generated artifacts, or copied examples.

These tools can have different audiences, deployment models, protected assets,
and trust boundaries than the Polaris server. Evaluate findings against the
tool-specific threat model and agent guidance in the tools repository where
available. A finding in a related tool is not automatically a Polaris server
vulnerability.

## Upstream Dependencies

Polaris depends on Apache Iceberg and other open source projects. Vulnerabilities
in those dependencies can affect Polaris when Polaris exposes the vulnerable
behavior through a reachable code path, depends on the vulnerable behavior for a
security decision, ships an affected dependency in a supported distribution, or
uses unsafe configuration that changes the impact.

A dependency vulnerability is not automatically a Polaris vulnerability. When
evaluating one, determine whether:

- The vulnerable dependency code is present in a supported Polaris artifact.
- The vulnerable behavior is reachable through Polaris.
- Exploitation crosses a Polaris trust boundary.
- Polaris authentication, authorization, metadata, credentials, or configuration
  are affected.
- Polaris needs a release, mitigation, configuration change, or advisory.

Respect the upstream project's security policy and disclosure process. Do not
include non-public upstream vulnerability details in Polaris issues, commits,
documentation, tests, or public discussions unless the information has already
been publicly disclosed or coordinated disclosure explicitly permits it.

## Security-Relevant Documentation Findings

Documentation, examples, defaults, and generated reference material can affect
security when they shape how users deploy, configure, integrate, or operate
Polaris.

A documentation issue may be security-relevant when it could reasonably cause
users, operators, integrators, or downstream tool authors to misunderstand a
security boundary, protected asset, trust assumption, default behavior, supported
configuration, operational responsibility, or security-sensitive workflow.

Examples include, but are not limited to, documentation that could lead users to:

- Store secrets in client-visible properties or metadata.
- Expose protected APIs, tools, or admin interfaces unintentionally.
- Assume credentials, tokens, storage policies, or access boundaries are
  narrower than they are.
- Use development-only defaults or examples in production.
- Misunderstand authentication, authorization, role scope, realm scope, or
  external policy behavior.
- Configure storage, federation, identity providers, policy services, TLS, or
  endpoints in a way that weakens intended security.
- Miss provider-specific limitations that affect isolation or credential scope.

This category applies across all Polaris areas, including server behavior,
clients, tools, packaging, deployment, configuration, authentication,
authorization, storage, federation, persistence backends, policy integrations,
dependency usage, upgrade guidance, and examples.

Security-relevant documentation findings are not automatically Polaris
vulnerabilities and do not automatically require an advisory or CVE. They should
still be reported as hardening or operator-safety findings when unclear
documentation, examples, defaults, or missing warnings could create a realistic
path to unsafe use or realistic user misunderstanding.

ASF security guidance remains authoritative for whether a finding should be
handled as an undisclosed vulnerability, advisory, CVE, normal bug, or
documentation hardening item.

When reporting such a finding, identify:

- The documented behavior, example, or missing warning.
- The security assumption a user may incorrectly make.
- The actor, asset, or boundary affected by the misunderstanding.
- Whether the underlying product behavior is correct, unsafe, or ambiguous.
- Proposed safer wording or placement for the clarification.

## Guidance For Automated Analysis

When evaluating a potential finding:

1. Identify the actor required to exploit it.
2. Identify the protected asset affected.
3. Identify the trust boundary crossed.
4. Identify the missing or incorrect security invariant.
5. Distinguish between:
   - a vulnerability in Polaris,
   - a deployment misconfiguration,
   - a dependency vulnerability not exposed by Polaris,
   - a false positive caused by test, demo, or unreachable code.
6. Prefer findings with a concrete reachable path from an untrusted or
   lower-privileged actor to unauthorized access, mutation, disclosure, or
   privilege escalation.
7. For findings involving packaging, optional dependencies, Gradle properties,
   container images, Helm charts, or custom distributions, identify the exact
   source revision, such as the official source release version, Git commit,
   branch, or tag, along with the selected modules, enabled Gradle properties,
   optional extensions, packaging format, and deployment-relevant configuration
   before assessing reachability, supported status, and severity.
8. For findings involving related Polaris tools, use the tool-specific threat
   model and agent guidance where available. A finding in a related tool is not
   automatically a Polaris server vulnerability.
9. For findings involving storage locations or credential vending, enumerate all
   URI-bearing inputs and metadata fields used by the operation, identify when
   they are validated, and verify that the same effective locations are used for
   authorization, persistence, storage access, and delegated credential scope.
10. For findings involving properties or configuration values, classify whether
    each value is secret, operator-visible, management-plane visible,
    data-plane client-visible, logged, stored in local profiles, or exported to
    generated artifacts.
11. For findings involving tokens, principal credentials, reset, rotation, or
    token exchange, identify the principal, realm, requested scope, effective
    scope, expiration, revocation behavior, and whether previously issued
    credentials or tokens can still cross the intended boundary.
12. For findings involving existence checks, compare `not found` and
    `forbidden` behavior and decide whether the target object's existence,
    name, or relationship is itself a protected asset in the relevant
    deployment.
13. For findings involving external endpoints, federation, object stores, or
    policy integrations, identify which party controls the endpoint, which
    credentials or metadata are sent to it, whether transport security is
    required, and whether redirects or custom endpoints can cross a trust
    boundary.
14. For variant-specific findings, do not mix assumptions from different
    persistence backends, authentication modes, authorization services, storage
    providers, optional extensions, or deployment profiles unless the
    combination is supported and reachable in one coherent deployment. Identify
    the concrete configuration under which the finding is reachable, or state
    that the finding is common across variants.
15. For documentation findings, separate security-relevant documentation issues
    from confirmed vulnerabilities. Report unclear docs, examples, defaults, or
    missing warnings when they create a realistic path to unsafe deployment,
    credential handling, authorization, storage, integration, upgrade, or
    operational choices, or a realistic user misunderstanding.

Do not report an issue solely because code, configuration, or documentation is
security-adjacent. For documentation, a useful finding should meet the same
realistic unsafe-use or realistic user-misunderstanding bar. For code and
configuration, a useful finding should explain a realistic path to impact. Avoid
over-reporting dependency entries that
are not reachable through Polaris, intentional local-development defaults,
test-only or demo-only code, admin-only behavior that matches this model,
downstream-only customization errors, and documentation preferences that do not
affect a security-relevant decision.

When reporting a potential finding, include structured triage metadata where it
helps reviewers reproduce, prioritize, and route the issue. These labels are
non-authoritative estimates for human review; they do not determine ASF
severity, advisory status, CVE allocation, disclosure handling, or whether the
finding is accepted as a vulnerability.

Recommended triage fields:

- Finding type: vulnerability, dependency advisory, documentation hardening,
  configuration risk, or false positive candidate.
- Affected area: server, admin tool, CLI, Helm chart, Docker image, client,
  related Polaris tool, dependency, documentation, or other concrete component.
- Deployment scope: official release artifact, source build, unreleased branch
  or commit, custom downstream integration, or unknown.
- Variant scope: concrete backend, authentication mode, authorization service,
  storage provider, tool, packaging format, and relevant optional features.
- Security property: confidentiality, integrity, availability, authentication,
  authorization, isolation, auditability, or another affected property.
- Proof status: proven by exploit, proven by boundary-crossing test, obvious
  from code, plausible but unproven, or scanner-only.
- Preconditions: unauthenticated, authenticated, administrative, local,
  privileged deployment access, specific configuration, or unknown.
- ASF severity estimate: Critical, Important, Moderate, Low, or Unknown, with a
  short rationale tied to attacker role, exploitability, configuration
  likelihood, and confidentiality, integrity, or availability impact.
- CVE/advisory candidate: yes, no, or uncertain, with rationale. Treat this as a
  routing hint only.
- Disclosure status: public-safe or potentially sensitive.
- Recommended handling: private security report, normal bug, documentation
  hardening, dependency tracking, false positive, or needs human triage.

Recommended triage dispositions:

- `VALID`: The finding violates a Polaris security invariant through an in-scope
  actor, protected asset, trust boundary, and coherent reachable configuration.
- `VALID-HARDENING`: No clear vulnerability is established, but the behavior
  creates realistic security risk, misuse risk, or defense-in-depth value worth
  addressing.
- `DOCUMENTATION-HARDENING`: The primary issue is unclear, incomplete, or
  misleading documentation, examples, defaults, generated references, or upgrade
  guidance that could realistically cause unsafe use.
- `DEPENDENCY-TRACKING`: The issue is in an upstream dependency and needs
  tracking, mitigation, upgrade, coordination, or reachability analysis for
  Polaris impact.
- `OUT-OF-MODEL`: The finding depends on an actor, asset, configuration,
  component, or deployment responsibility that this model places outside Polaris
  security guarantees.
- `DOWNSTREAM-ONLY`: The behavior is in downstream integration code, private
  packaging, local patches, custom deployment logic, or third-party glue rather
  than Polaris code, official artifacts, documented configuration, reusable
  modules, or project-maintained tools.
- `FALSE-POSITIVE`: The report does not show a reachable path to security impact
  or depends on impossible state, privileged fixtures, mocked trust decisions,
  already-authorized access, or mutually exclusive variants.
- `MODEL-GAP`: The finding cannot be cleanly classified using this threat model;
  update the model or ask for project clarification before making a final triage
  decision.

Recommended finding output:

```md
## Finding

## Impact

## Affected Version, Artifact, And Configuration

## Affected Area And Variant

## Preconditions

## Evidence

## Proof Status

## Severity Estimate

## CVE/Advisory Candidate

## Recommended Handling

## Public-Safety Note
```

Always identify the exact version, artifact, source revision, packaging format,
enabled modules or properties, and relevant runtime configuration before
assessing reachability, severity, or supported status.

Evidence must show that the actor in the stated preconditions can reach the
claimed impact through a supported or realistically reachable path. A unit test,
integration test, or reproduction is not sufficient proof if it relies on
already-authorized access, privileged test fixtures, direct internal object
construction, mocked trust decisions, protected information the actor would not
have, disabled security controls, or impossible deployment state. In those cases,
report the finding as plausible, scanner-only, or a false positive candidate
unless a real boundary crossing can also be demonstrated.

Do not include private vulnerability details, exploit payloads, reporter names,
private mailing-list content, secrets, or non-public infrastructure details in
findings, documentation, tests, comments, commit messages, or PR descriptions.

## Maintenance

Review this threat model when Polaris adds or substantially changes:

- Authentication modes, authorization services, policy integrations, or identity
  provider behavior.
- Persistence backends, storage integrations, credential-vending flows, token
  exchange behavior, or externally configured endpoints.
- Release artifacts, deployment formats, Helm charts, container images, Gradle
  feature flags, or supported build profiles.
- Public APIs, admin APIs, CLI commands, migration tools, web UIs, MCP servers,
  benchmarks, or other related Polaris tools.
- Documentation that changes security-relevant defaults, examples, deployment
  guidance, upgrade guidance, or operational responsibilities.

## References

- [Project security policy](SECURITY.md): how to report suspected
  vulnerabilities in Apache Polaris.
- [Security reporting page](https://polaris.apache.org/community/security-report/):
  public reporting instructions and previously published security advisories and
  CVEs.
- [ASF Security Team](https://www.apache.org/security/): ASF-wide security
  reporting and vulnerability handling overview.
- [ASF Project Security for Committers](https://www.apache.org/security/committers.html):
  ASF guidance for PMCs and committers handling possible vulnerabilities.
