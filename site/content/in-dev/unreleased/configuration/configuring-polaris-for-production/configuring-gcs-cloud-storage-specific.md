# Configuring GCS with Polaris

This guide covers how to configure Google Cloud Storage (GCS) as a storage backend for Polaris catalogs, including credential vending, IAM configuration, and access control.

## Overview

Polaris uses **credential vending** to securely manage access to GCS objects. When you configure a catalog with GCS storage, Polaris issues scoped (vended) tokens with limited permissions and duration for each operation, rather than using long-lived credentials.

## Storage Configuration

When creating a Polaris catalog with GCS storage, you need to specify:

1. **Storage Type**: `GCS`
2. **Base Location**: The default GCS path for the catalog (e.g., `gs://your-bucket/catalogs/catalog-name`)
3. **Allowed Locations**: GCS paths where the catalog can read/write data

### Example Catalog Configuration

```json
{
  "catalog": {
    "type": "INTERNAL",
    "name": "my_catalog",
    "properties": {
      "default-base-location": "gs://your-bucket/catalogs/my_catalog"
    },
    "storageConfigInfo": {
      "storageType": "GCS",
      "allowedLocations": [
        "gs://your-bucket"
      ]
    }
  }
}
```

## IAM Configuration

### Service Account Permissions

The service account running Polaris (e.g., on Cloud Run) needs appropriate IAM roles to access GCS:

**Required IAM Roles:**
- `roles/storage.objectAdmin` - For read/write access to objects
- OR `roles/storage.objectViewer` + `roles/storage.objectCreator` - For more granular control

Grant the role at the bucket level:

```bash
gsutil iam ch serviceAccount:polaris-sa@project.iam.gserviceaccount.com:roles/storage.objectAdmin gs://your-bucket
```

### User Access Permissions

In addition to GCS IAM, users need Polaris catalog roles to access tables:

1. Create a catalog role with appropriate privileges:
   - `TABLE_READ_DATA` - Read table data
   - `TABLE_WRITE_DATA` - Write table data
   - `NAMESPACE_FULL_METADATA` - Access namespace/table metadata
2. Assign the catalog role to a principal role (e.g., `service_admin`)

This two-level permission model ensures both GCS access (via IAM) and Polaris access control (via catalog roles) are properly configured.

## Google Cloud Storage Limitation

Polaris does not support Hierarchical Namespaces (HNS) on the bucket.

## Troubleshooting

### 403 Forbidden Errors

1. Verify Polaris service account has IAM permissions on the bucket
2. Check that paths are within catalog's `allowedLocations`
3. Verify user has appropriate catalog role permissions
