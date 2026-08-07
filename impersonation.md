# GCP Service Account Impersonation for Vended Storage Credentials

Polaris vends short-lived GCS credentials by impersonating a GCP service
account (`GcpCredentialsStorageIntegration`, see
`polaris-core/src/main/java/org/apache/polaris/core/storage/gcp/`) and then
downscoping the impersonated token to the caller's granted locations. This
doc covers creating that service account, granting it and Polaris the
required IAM bindings, and — if you want vended tokens to last longer than
the 1-hour default — binding the
`constraints/iam.allowServiceAccountCredentialLifetimeExtension` org policy
constraint to it.

There are two identities involved:

- **Caller identity**: whatever credential Polaris itself runs as (e.g. a
  GCE/GKE workload identity, or a service account key referenced by
  `GOOGLE_APPLICATION_CREDENTIALS`). This is `sourceCredentials` in the code.
- **Target service account**: the account Polaris impersonates to mint
  vended tokens. This is the `gcsServiceAccount` value set on the catalog's
  storage configuration (`GcpStorageConfigurationInfo.getGcpServiceAccount()`).

All commands below assume the [gcloud CLI](https://cloud.google.com/sdk/docs/install)
is installed and authenticated (`gcloud auth login`) against an account with
sufficient privileges (Owner or IAM Admin on the project, and Organization
Policy Administrator at the org/folder/project level for the policy step).

## 1. Create the target service account

```bash
gcloud iam service-accounts create polaris-storage-vending \
    --project=PROJECT_ID \
    --display-name="Polaris vended storage credentials"
```

This creates
`polaris-storage-vending@PROJECT_ID.iam.gserviceaccount.com` — use this
email as `SERVICE_ACCOUNT_EMAIL` in the remaining steps.

## 2. Grant the service account access to the target GCS buckets

Credential Access Boundary downscoping can only *narrow* permissions the
impersonated service account already has — it cannot grant more. Give the
service account whatever GCS role covers the operations Polaris needs to be
able to hand out (read-only catalogs can use `roles/storage.objectViewer`;
catalogs that need write access should use `roles/storage.objectAdmin`):

```bash
gcloud storage buckets add-iam-policy-binding gs://BUCKET_NAME \
    --member=serviceAccount:SERVICE_ACCOUNT_EMAIL \
    --role=roles/storage.objectAdmin
```

Repeat for each bucket the catalog needs to access.

## 3. Let Polaris's caller identity impersonate the target service account

Grant the **Service Account Token Creator** role
(`roles/iam.serviceAccountTokenCreator`) on the target service account to
whatever identity Polaris runs as:

```bash
gcloud iam service-accounts add-iam-policy-binding SERVICE_ACCOUNT_EMAIL \
    --member=serviceAccount:CALLER_SERVICE_ACCOUNT_EMAIL \
    --role=roles/iam.serviceAccountTokenCreator
```

(If Polaris runs under a user account instead of a service account, use
`--member=user:USER_EMAIL`.)

Without this binding, `IamCredentialsClient.generateAccessToken` calls from
Polaris fail with a permission-denied error.

## 4. Point the catalog's storage configuration at the service account

Set `gcsServiceAccount` to `SERVICE_ACCOUNT_EMAIL` in the catalog's storage
configuration (the `GcpStorageConfigInfo` passed when creating/updating the
catalog via the Polaris management API).

## 5. (Optional) Extend the vended token lifetime beyond 1 hour

By default GCP's IAM Service Account Credentials API caps
`generateAccessToken` lifetimes at 1 hour, and Polaris's
`STORAGE_CREDENTIAL_DURATION_SECONDS` config (default `3600`) is clamped to
that unless the target service account is covered by an org policy with the
`constraints/iam.allowServiceAccountCredentialLifetimeExtension` list
constraint. That constraint can raise the ceiling to 12 hours (`43200`
seconds) — Polaris clamps to that value regardless of what you configure.

Bind the constraint to the service account at the organization, folder, or
project level (org/folder are recommended if you manage multiple projects;
use whichever scope contains the service account's project):

```bash
gcloud resource-manager org-policies allow \
    constraints/iam.allowServiceAccountCredentialLifetimeExtension \
    SERVICE_ACCOUNT_EMAIL \
    --organization=ORGANIZATION_ID
```

Or scoped to a single project:

```bash
gcloud resource-manager org-policies allow \
    constraints/iam.allowServiceAccountCredentialLifetimeExtension \
    SERVICE_ACCOUNT_EMAIL \
    --project=PROJECT_ID
```

To add multiple service accounts to the same policy, list them all:

```bash
gcloud resource-manager org-policies allow \
    constraints/iam.allowServiceAccountCredentialLifetimeExtension \
    SERVICE_ACCOUNT_EMAIL_1 \
    SERVICE_ACCOUNT_EMAIL_2 \
    --organization=ORGANIZATION_ID
```

Verify the policy took effect:

```bash
gcloud resource-manager org-policies describe \
    constraints/iam.allowServiceAccountCredentialLifetimeExtension \
    --organization=ORGANIZATION_ID
```

The output should list `SERVICE_ACCOUNT_EMAIL` under `allowedValues`.

Then set Polaris's `STORAGE_CREDENTIAL_DURATION_SECONDS` realm config to the
desired duration in seconds (up to `43200`). If the org policy isn't bound
to the service account and a value above `3600` is configured, GCP will
reject the `generateAccessToken` call and Polaris will surface it as
"Unable to impersonate GCP service account: ...".

## Reference

- [Create short-lived credentials for a service account](https://cloud.google.com/iam/docs/create-short-lived-credentials-direct)
- [Restrict service account usage via organization policy](https://cloud.google.com/resource-manager/docs/organization-policy/restricting-service-accounts)
- [Downscoping with Credential Access Boundaries](https://cloud.google.com/iam/docs/downscoping-short-lived-credentials)
