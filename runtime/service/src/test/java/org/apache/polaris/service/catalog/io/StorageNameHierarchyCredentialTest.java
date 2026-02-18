/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.catalog.io;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.Test;

/**
 * Service-layer tests verifying that {@code RESOLVE_CREDENTIALS_BY_STORAGE_NAME} and the
 * hierarchical storage-config resolver interact correctly.
 *
 * <p>Specifically: when a namespace entity carries a {@code storageName} and a table in that
 * namespace has no own storage config, the hierarchy resolver must return the namespace entity —
 * not the catalog — so that {@code PolarisStorageIntegrationProviderImpl} dispatches to the correct
 * named credential set.
 *
 * <p>NOTE: The {@code TestServices} in-memory layer does not invoke real AWS STS; therefore these
 * tests assert the observable {@code storageName} on the effective storage config returned by the
 * management API (GET table storage config), which is the direct precondition for {@code
 * PolarisStorageIntegrationProviderImpl} to dispatch correctly. End-to-end STS-credential
 * assertions are covered in the full-HTTP integration tests in {@code
 * integration-tests/.../PolarisStorageConfigIntegrationTest.java} (Tasks 3–5).
 */
public class StorageNameHierarchyCredentialTest {

  private static final String CATALOG = "cred_test_catalog";
  private static final String NAMESPACE = "cred_ns";
  private static final String TABLE = "cred_table";
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  /**
   * Build a {@link TestServices} with the given feature-flag value for {@code
   * RESOLVE_CREDENTIALS_BY_STORAGE_NAME} and bootstrap a catalog + namespace + table. Returns the
   * configured service instance.
   */
  private TestServices buildServices(boolean resolveByStorageName) {
    TestServices services =
        TestServices.builder()
            .config(
                Map.of(
                    "RESOLVE_CREDENTIALS_BY_STORAGE_NAME",
                    resolveByStorageName,
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("S3", "FILE"),
                    "ALLOW_INSECURE_STORAGE_TYPES",
                    true))
            .build();

    // Catalog with storageName="cat-creds"
    FileStorageConfigInfo catalogStorage =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file:///tmp/test/"))
            .setStorageName("cat-creds")
            .build();

    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(CATALOG)
            .setProperties(new CatalogProperties("file:///tmp/test/"))
            .setStorageConfigInfo(catalogStorage)
            .build();

    try (Response r =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(r.getStatus())
          .as("Catalog creation should succeed")
          .isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    try (Response r =
        services
            .restApi()
            .createNamespace(
                CATALOG,
                CreateNamespaceRequest.builder().withNamespace(Namespace.of(NAMESPACE)).build(),
                services.realmContext(),
                services.securityContext())) {
      assertThat(r.getStatus())
          .as("Namespace creation should succeed")
          .isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response r =
        services
            .restApi()
            .createTable(
                CATALOG,
                NAMESPACE,
                CreateTableRequest.builder().withName(TABLE).withSchema(SCHEMA).build(),
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(r.getStatus())
          .as("Table creation should succeed")
          .isEqualTo(Response.Status.OK.getStatusCode());
    }

    return services;
  }

  /**
   * When {@code RESOLVE_CREDENTIALS_BY_STORAGE_NAME=true} and the namespace has a storage config
   * with {@code storageName="ns-creds"}, the effective (resolved) config for a table in that
   * namespace must carry {@code storageName="ns-creds"} — not the catalog's {@code
   * storageName="cat-creds"}.
   *
   * <p>This is the key pre-condition for {@code PolarisStorageIntegrationProviderImpl} to dispatch
   * {@code stsCredentials("ns-creds")} rather than {@code stsCredentials("cat-creds")} when the
   * flag is enabled.
   */
  @Test
  public void testNamespaceStorageNameReachesCredentialProviderWhenFlagEnabled() {
    TestServices services = buildServices(true);

    // PUT namespace storage config with storageName="ns-creds"
    AwsStorageConfigInfo nsStorage =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://ns-bucket/"))
            .setRoleArn("arn:aws:iam::123456789012:role/ns-role")
            .setStorageName("ns-creds")
            .build();

    try (Response r =
        services
            .catalogsApi()
            .setNamespaceStorageConfig(
                CATALOG,
                NAMESPACE,
                nsStorage,
                services.realmContext(),
                services.securityContext())) {
      assertThat(r.getStatus())
          .as("Setting namespace storage config should succeed")
          .isEqualTo(Response.Status.OK.getStatusCode());
    }

    // GET table effective storage config — must resolve to the namespace entity, not the catalog
    try (Response r =
        services
            .catalogsApi()
            .getTableStorageConfig(
                CATALOG, NAMESPACE, TABLE, services.realmContext(), services.securityContext())) {
      assertThat(r.getStatus())
          .as("GET effective table storage config should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());

      StorageConfigInfo effective = r.readEntity(StorageConfigInfo.class);
      assertThat(effective).isNotNull();

      // The effective storageName must be "ns-creds" (from namespace), not "cat-creds" (catalog).
      // PolarisStorageIntegrationProviderImpl will call stsCredentials("ns-creds") when
      // RESOLVE_CREDENTIALS_BY_STORAGE_NAME=true, giving the correct isolated credential set.
      assertThat(effective.getStorageName())
          .as(
              "Effective storageName must be 'ns-creds' from the namespace, not 'cat-creds' "
                  + "from the catalog — this is the direct precondition for the credential "
                  + "provider to dispatch to the namespace-scoped credential set")
          .isEqualTo("ns-creds");
      assertThat(effective.getStorageName())
          .as("Catalog storageName 'cat-creds' must not leak into the effective config")
          .isNotEqualTo("cat-creds");
    }
  }

  /**
   * When {@code RESOLVE_CREDENTIALS_BY_STORAGE_NAME=false} (the default), the catalog-level storage
   * config is the fallback for credential-provider dispatch. Even if a namespace config with a
   * different {@code storageName} exists, the flag being off means the provider will use the
   * default AWS credential chain — but the hierarchy still resolves to the most-specific entity.
   *
   * <p>This test verifies that the storageName on the namespace is still returned in the effective
   * config when the namespace has a config (the hierarchy walk is independent of the flag), while
   * documenting that the flag itself controls whether the storageName is *acted upon* at the
   * credential-provider layer.
   */
  @Test
  public void testHierarchyAlwaysResolvesNamespaceStorageNameRegardlessOfFlag() {
    // RESOLVE_CREDENTIALS_BY_STORAGE_NAME=false — flag off
    TestServices services = buildServices(false);

    // PUT namespace storage config with storageName="ns-creds"
    AwsStorageConfigInfo nsStorage =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://ns-bucket/"))
            .setRoleArn("arn:aws:iam::123456789012:role/ns-role")
            .setStorageName("ns-creds")
            .build();

    try (Response r =
        services
            .catalogsApi()
            .setNamespaceStorageConfig(
                CATALOG,
                NAMESPACE,
                nsStorage,
                services.realmContext(),
                services.securityContext())) {
      assertThat(r.getStatus())
          .as("Setting namespace storage config should succeed")
          .isEqualTo(Response.Status.OK.getStatusCode());
    }

    // The hierarchy ALWAYS resolves to the most-specific entity regardless of the flag.
    // With the flag off, PolarisStorageIntegrationProviderImpl ignores storageName and uses the
    // default AWS credential chain — but the resolved storageName is still "ns-creds" in the
    // entity that would be passed to the provider.
    try (Response r =
        services
            .catalogsApi()
            .getTableStorageConfig(
                CATALOG, NAMESPACE, TABLE, services.realmContext(), services.securityContext())) {
      assertThat(r.getStatus())
          .as("GET effective table storage config should return 200")
          .isEqualTo(Response.Status.OK.getStatusCode());

      StorageConfigInfo effective = r.readEntity(StorageConfigInfo.class);
      assertThat(effective).isNotNull();
      // The hierarchy still stops at the namespace regardless of the flag.
      assertThat(effective.getStorageName())
          .as(
              "Hierarchy always resolves to the namespace-level storageName; the flag only "
                  + "controls whether the credential provider acts on it")
          .isEqualTo("ns-creds");
    }
  }
}
