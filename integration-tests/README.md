# Polaris Integration Tests

## Overview

This module contains integration tests for Polaris. These tests are designed as black box tests
that can be run against a local or remote Polaris instance.

## Runtime Expectations

### PolarisApplicationIntegrationTest

This test expects the server to be configured with the following features configured:

* `ALLOW_OVERLAPPING_CATALOG_URLS`: `true`

The server must also be configured to reject request body sizes larger than 1MB (1000000 bytes).

### PolarisManagementServiceIntegrationTest

This test expects the server to be configured with the following features configured:

* `ALLOW_OVERLAPPING_CATALOG_URLS`: `true`
* `ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING`: `true`

### PolarisRestCatalogIntegrationTest

This test expects the server to be configured with the following features configured:

* `ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING`: `false`

### PolarisRestCatalogView*IntegrationTest

These tests expect the server to be configured with teh feature `SUPPORTED_CATALOG_STORAGE_TYPES`
set to the appropriate storage type.

### PolarisSparkIntegrationTest

This test expects the server to be configured with the following features enabled:

* `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION`: `true`
* `ALLOW_OVERLAPPING_CATALOG_URLS`: `true`