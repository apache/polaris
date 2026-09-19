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
package org.apache.polaris.service.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.OAuthClientCredentialsParameters;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.secrets.UnsafeInMemorySecretsManager;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.identity.provider.DefaultServiceIdentityProvider;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ManagementServiceTest {
  private TestServices services;

  @BeforeEach
  public void setup() {
    services =
        TestServices.builder()
            .config(
                Map.of(
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("S3", "GCS", "AZURE"),
                    "ALLOW_SETTING_S3_ENDPOINTS",
                    Boolean.FALSE,
                    "ALLOW_SETTING_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS",
                    Boolean.FALSE,
                    "ENABLE_CATALOG_FEDERATION",
                    Boolean.TRUE))
            .build();
  }

  @Test
  public void testCreateCatalogWithDisallowedStorageConfig() {
    FileStorageConfigInfo fileStorage =
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://"))
            .build();
    String catalogName = "my-external-catalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("file:///tmp/path/to/data"))
            .setStorageConfigInfo(fileStorage)
            .build();
    assertThatThrownBy(
            () ->
                services
                    .catalogsApi()
                    .createCatalog(
                        new CreateCatalogRequest(catalog),
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported storage type: FILE");
  }

  @Test
  public void testCreateCatalogWithDisallowedS3Endpoints() {
    AwsStorageConfigInfo.Builder storageConfig =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://bucket/path/to/data"));
    String catalogName = "test-catalog";
    Supplier<Catalog> catalog =
        () ->
            PolarisCatalog.builder()
                .setType(Catalog.TypeEnum.INTERNAL)
                .setName(catalogName)
                .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
                .setStorageConfigInfo(storageConfig.build())
                .build();
    Supplier<Response> createCatalog =
        () ->
            services
                .catalogsApi()
                .createCatalog(
                    new CreateCatalogRequest(catalog.get()),
                    services.realmContext(),
                    services.securityContext());

    storageConfig.setEndpoint("http://example.com");
    assertThatThrownBy(createCatalog::get)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Explicitly setting S3 endpoints is not allowed.");

    storageConfig.setEndpoint(null);
    storageConfig.setStsEndpoint("http://example.com");
    assertThatThrownBy(createCatalog::get)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Explicitly setting S3 endpoints is not allowed.");

    storageConfig.setStsEndpoint(null);
    storageConfig.setEndpointInternal("http://example.com");
    assertThatThrownBy(createCatalog::get)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Explicitly setting S3 endpoints is not allowed.");

    storageConfig.setEndpointInternal(null);
    storageConfig.setStsUnavailable(false);
    assertThatThrownBy(createCatalog::get)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Explicitly disabling STS is not allowed.");
  }

  @Test
  public void testUpdateCatalogWithDisallowedStorageConfig() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://bucket/path/to/data"))
            .build();
    String catalogName = "mycatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
            .setStorageConfigInfo(awsConfigModel)
            .build();
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    Catalog fetchedCatalog;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(catalogName, services.realmContext(), services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = (Catalog) response.getEntity();

      assertThat(fetchedCatalog.getName()).isEqualTo(catalogName);
      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://bucket/path/to/data"));
      assertThat(fetchedCatalog.getEntityVersion()).isGreaterThan(0);
    }

    FileStorageConfigInfo fileStorage =
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://"))
            .build();
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "file:///tmp/path/to/data/"),
            fileStorage,
            null);

    // failure to update
    assertThatThrownBy(
            () ->
                services
                    .catalogsApi()
                    .updateCatalog(
                        catalogName,
                        updateRequest,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported storage type: FILE");

    UpdateCatalogRequest update2 =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of(),
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setAllowedLocations(List.of("s3://bucket/path/to/data"))
                .setRoleArn("arn:aws:iam::123456789012:role/my-role")
                .setEndpoint("http://example.com")
                .build(),
            null);
    assertThatThrownBy(
            () ->
                services
                    .catalogsApi()
                    .updateCatalog(
                        catalogName, update2, services.realmContext(), services.securityContext()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Explicitly setting S3 endpoints is not allowed.");
  }

  @Test
  public void testCreateCatalogWithDisallowedConfigs() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://bucket/path/to/data"))
            .build();
    ConnectionConfigInfo connectionConfigInfo =
        IcebergRestConnectionConfigInfo.builder(
                ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setUri("https://myorg-my_account.snowflakecomputing.com/polaris/api/catalog")
            .setRemoteCatalogName("my-remote-catalog")
            .setAuthenticationParameters(
                OAuthClientCredentialsParameters.builder(
                        AuthenticationParameters.AuthenticationTypeEnum.OAUTH)
                    .setClientId("my-client-id")
                    .setClientSecret("my-client-secret")
                    .setScopes(List.of("PRINCIPAL_ROLE:ALL"))
                    .build())
            .build();
    String catalogName = "mycatalog";
    CatalogProperties catalogProperties =
        CatalogProperties.builder("s3://bucket/path/to/data")
            .addProperty("polaris.config.enable-sub-catalog-rbac-for-federated-catalogs", "true")
            .build();
    Catalog catalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(catalogName)
            .setProperties(catalogProperties)
            .setStorageConfigInfo(awsConfigModel)
            .setConnectionConfigInfo(connectionConfigInfo)
            .build();
    Supplier<Response> createCatalog =
        () ->
            services
                .catalogsApi()
                .createCatalog(
                    new CreateCatalogRequest(catalog),
                    services.realmContext(),
                    services.securityContext());
    assertThatThrownBy(createCatalog::get)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Explicitly setting polaris.config.enable-sub-catalog-rbac-for-federated-catalogs is not allowed because ALLOW_SETTING_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS is set to false.");
  }

  @Test
  public void testUpdateCatalogWithDisallowedConfigs() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://bucket/path/to/data"))
            .build();
    ConnectionConfigInfo connectionConfigInfo =
        IcebergRestConnectionConfigInfo.builder(
                ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setUri("https://myorg-my_account.snowflakecomputing.com/polaris/api/catalog")
            .setRemoteCatalogName("my-remote-catalog")
            .setAuthenticationParameters(
                OAuthClientCredentialsParameters.builder(
                        AuthenticationParameters.AuthenticationTypeEnum.OAUTH)
                    .setClientId("my-client-id")
                    .setClientSecret("my-client-secret")
                    .setScopes(List.of("PRINCIPAL_ROLE:ALL"))
                    .build())
            .build();
    String catalogName = "mycatalog";
    CatalogProperties catalogProperties =
        CatalogProperties.builder("s3://bucket/path/to/data").build();
    Catalog catalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(catalogName)
            .setProperties(catalogProperties)
            .setStorageConfigInfo(awsConfigModel)
            .setConnectionConfigInfo(connectionConfigInfo)
            .build();
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    Catalog fetchedCatalog;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(catalogName, services.realmContext(), services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = (Catalog) response.getEntity();

      assertThat(fetchedCatalog.getName()).isEqualTo(catalogName);
      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://bucket/path/to/data"));
      assertThat(fetchedCatalog.getEntityVersion()).isGreaterThan(0);
    }

    UpdateCatalogRequest update =
        UpdateCatalogRequest.builder()
            .setProperties(
                Map.of("polaris.config.enable-sub-catalog-rbac-for-federated-catalogs", "true"))
            .build();
    assertThatThrownBy(
            () ->
                services
                    .catalogsApi()
                    .updateCatalog(
                        catalogName, update, services.realmContext(), services.securityContext()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Explicitly setting polaris.config.enable-sub-catalog-rbac-for-federated-catalogs is not allowed because ALLOW_SETTING_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS is set to false.");
  }

  private PolarisAdminService setupPolarisAdminService(
      PolarisMetaStoreManager metaStoreManager, PolarisCallContext callContext) {
    PrincipalEntity rootPrincipal =
        new PrincipalEntity.Builder()
            .setName(PolarisEntityConstants.getRootPrincipalName())
            .build();
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            rootPrincipal.getName(),
            Map.of(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, rootPrincipal),
            Set.of(PolarisEntityConstants.getNameOfPrincipalServiceAdminRole()));
    return new PolarisAdminService(
        callContext,
        services.resolutionManifestFactory(),
        metaStoreManager,
        new UnsafeInMemorySecretsManager(),
        new DefaultServiceIdentityProvider(),
        principal,
        new PolarisAuthorizerImpl(services.realmConfig()),
        ReservedProperties.NONE);
  }

  private PrincipalEntity createPrincipal(
      PolarisMetaStoreManager metaStoreManager, PolarisCallContext callContext, String name) {
    return new PrincipalEntity.Builder()
        .setName(name)
        .setCreateTimestamp(Instant.now().toEpochMilli())
        .setId(metaStoreManager.generateNewEntityId(callContext).getId())
        .build();
  }

  private PrincipalRoleEntity createRole(
      PolarisMetaStoreManager metaStoreManager,
      PolarisCallContext callContext,
      String name,
      boolean isFederated) {
    return new PrincipalRoleEntity.Builder()
        .setId(metaStoreManager.generateNewEntityId(callContext).getId())
        .setName(name)
        .setFederated(isFederated)
        .setProperties(Map.of())
        .setCreateTimestamp(Instant.now().toEpochMilli())
        .setLastUpdateTimestamp(Instant.now().toEpochMilli())
        .build();
  }

  @Test
  public void testCannotAssignFederatedEntities() {
    PolarisMetaStoreManager metaStoreManager = services.metaStoreManager();
    PolarisCallContext callContext = services.newCallContext();
    PolarisAdminService polarisAdminService =
        setupPolarisAdminService(metaStoreManager, callContext);

    PrincipalEntity principal = createPrincipal(metaStoreManager, callContext, "principal_id");
    metaStoreManager.createPrincipal(callContext, principal);

    PrincipalRoleEntity role = createRole(metaStoreManager, callContext, "federated_role_id", true);
    EntityResult result = metaStoreManager.createEntityIfNotExists(callContext, null, role);
    assertThat(result.isSuccess()).isTrue();

    assertThatThrownBy(
            () -> polarisAdminService.assignPrincipalRole(principal.getName(), role.getName()))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  public void testCanListCatalogs() {
    PolarisMetaStoreManager metaStoreManager = services.metaStoreManager();
    PolarisCallContext callContext = services.newCallContext();
    PolarisAdminService polarisAdminService =
        setupPolarisAdminService(metaStoreManager, callContext);

    CreateCatalogResult catalog1 =
        metaStoreManager.createCatalog(
            callContext,
            new PolarisBaseEntity(
                PolarisEntityConstants.getNullId(),
                metaStoreManager.generateNewEntityId(callContext).getId(),
                PolarisEntityType.CATALOG,
                PolarisEntitySubType.NULL_SUBTYPE,
                PolarisEntityConstants.getRootEntityId(),
                "my-catalog-1"),
            List.of());
    assertThat(catalog1.isSuccess()).isTrue();

    CreateCatalogResult catalog2 =
        metaStoreManager.createCatalog(
            callContext,
            new PolarisBaseEntity(
                PolarisEntityConstants.getNullId(),
                metaStoreManager.generateNewEntityId(callContext).getId(),
                PolarisEntityType.CATALOG,
                PolarisEntitySubType.NULL_SUBTYPE,
                PolarisEntityConstants.getRootEntityId(),
                "my-catalog-2"),
            List.of());
    assertThat(catalog2.isSuccess()).isTrue();

    List<Catalog> catalogs = polarisAdminService.listCatalogs();
    assertThat(catalogs.size()).isEqualTo(2);
    assertThat(catalogs)
        .extracting(Catalog::getName)
        .containsExactlyInAnyOrder("my-catalog-1", "my-catalog-2");
  }

  @Test
  public void testUpdateCatalogChangeAwsAccountIdBlockedByDefault() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://bucket/path/to/data"))
            .build();
    String catalogName = "mycatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
            .setStorageConfigInfo(awsConfigModel)
            .build();
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    Catalog fetchedCatalog;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(catalogName, services.realmContext(), services.securityContext())) {
      fetchedCatalog = (Catalog) response.getEntity();
    }

    // Changing the AWS account ID should be rejected by default
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/path/to/data"),
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setAllowedLocations(List.of("s3://bucket/path/to/data"))
                .setRoleArn("arn:aws:iam::999999999999:role/other-role")
                .build(),
            null);
    assertThatThrownBy(
            () ->
                services
                    .catalogsApi()
                    .updateCatalog(
                        catalogName,
                        updateRequest,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageStartingWith("Cannot modify AWS account ID");
  }

  @Test
  public void testUpdateCatalogChangesWithinSameAccountAllowed() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://bucket/path/to/data"))
            .build();
    String catalogName = "mycatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
            .setStorageConfigInfo(awsConfigModel)
            .build();
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    Catalog fetchedCatalog;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(catalogName, services.realmContext(), services.securityContext())) {
      fetchedCatalog = (Catalog) response.getEntity();
    }

    // Changing role name within the same account should succeed
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/path/to/data"),
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setAllowedLocations(List.of("s3://bucket/path/to/data"))
                .setRoleArn("arn:aws:iam::123456789012:role/other-role")
                .build(),
            null);
    try (Response response =
        services
            .catalogsApi()
            .updateCatalog(
                catalogName, updateRequest, services.realmContext(), services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testUpdateCatalogChangeExternalIdBlockedByDefault() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("my-external-id")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://bucket/path/to/data"))
            .build();
    String catalogName = "mycatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
            .setStorageConfigInfo(awsConfigModel)
            .build();
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    Catalog fetchedCatalog;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(catalogName, services.realmContext(), services.securityContext())) {
      fetchedCatalog = (Catalog) response.getEntity();
    }

    // Changing the external ID should be rejected by default
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/path/to/data"),
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setAllowedLocations(List.of("s3://bucket/path/to/data"))
                .setRoleArn("arn:aws:iam::123456789012:role/my-role")
                .setExternalId("different-external-id")
                .build(),
            null);
    assertThatThrownBy(
            () ->
                services
                    .catalogsApi()
                    .updateCatalog(
                        catalogName,
                        updateRequest,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageStartingWith("Cannot modify ExternalId");
  }

  @Test
  public void testUpdateCatalogStorageConfigChangesAllowedWithFeatureFlag() {
    TestServices flagEnabledServices =
        TestServices.builder()
            .config(
                Map.of(
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("S3", "GCS", "AZURE"),
                    "ALLOW_UNRESTRICTED_STORAGE_CONFIG_ROLE_CHANGES",
                    Boolean.TRUE))
            .build();

    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("my-external-id")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://bucket/path/to/data"))
            .build();
    String catalogName = "mycatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
            .setStorageConfigInfo(awsConfigModel)
            .build();
    try (Response response =
        flagEnabledServices
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                flagEnabledServices.realmContext(),
                flagEnabledServices.securityContext())) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    Catalog fetchedCatalog;
    try (Response response =
        flagEnabledServices
            .catalogsApi()
            .getCatalog(
                catalogName,
                flagEnabledServices.realmContext(),
                flagEnabledServices.securityContext())) {
      fetchedCatalog = (Catalog) response.getEntity();
    }

    // Changing both the AWS account ID and external ID should succeed with the feature flag
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/path/to/data"),
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setAllowedLocations(List.of("s3://bucket/path/to/data"))
                .setRoleArn("arn:aws:iam::999999999999:role/other-role")
                .setExternalId("different-external-id")
                .build(),
            null);
    try (Response response =
        flagEnabledServices
            .catalogsApi()
            .updateCatalog(
                catalogName,
                updateRequest,
                flagEnabledServices.realmContext(),
                flagEnabledServices.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreateCatalogReturnErrorOnFailure() {
    PolarisMetaStoreManager metaStoreManager = Mockito.spy(services.metaStoreManager());
    PolarisCallContext callContext = services.newCallContext();
    PolarisAdminService polarisAdminService =
        setupPolarisAdminService(metaStoreManager, callContext);

    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://bucket/path/to/data"))
            .build();
    String catalogName = "mycatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
            .setStorageConfigInfo(awsConfigModel)
            .build();
    CreateCatalogResult resultWithError =
        new CreateCatalogResult(
            BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, "Unexpected Error Occurred");
    Mockito.doAnswer(invocation -> resultWithError)
        .when(metaStoreManager)
        .createCatalog(Mockito.any(), Mockito.any(), Mockito.any());
    Assertions.assertThatThrownBy(
            () -> polarisAdminService.createCatalog(new CreateCatalogRequest(catalog)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            String.format(
                "Cannot create Catalog %s: %s with extraInfo %s",
                catalogName,
                resultWithError.getReturnStatus(),
                resultWithError.getExtraInformation()));
  }

  // --- Update-diff validation for named storage configurations (storageConfigInfos) ---

  private Catalog createCatalogWithDefaultAndNamedAwsConfig(
      String catalogName, String namedRoleArn, String namedExternalId) {
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder()
                    .setRoleArn("arn:aws:iam::123456789012:role/my-role")
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                    .setAllowedLocations(List.of("s3://bucket/path/to/data"))
                    .build())
            .setStorageConfigInfos(
                List.of(
                    AwsStorageConfigInfo.builder()
                        .setRoleArn(namedRoleArn)
                        .setExternalId(namedExternalId)
                        .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                        .setAllowedLocations(List.of("s3://named-bucket/path/"))
                        .setStorageName("hot")
                        .build()))
            .build();
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(catalogName, services.realmContext(), services.securityContext())) {
      return (Catalog) response.getEntity();
    }
  }

  @Test
  public void testUpdateCatalogNamedConfigStorageTypeChangeBlockedByDefault() {
    String catalogName = "mycatalog";
    Catalog fetchedCatalog =
        createCatalogWithDefaultAndNamedAwsConfig(
            catalogName, "arn:aws:iam::123456789012:role/named-role", "named-external-id");

    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/path/to/data"),
            fetchedCatalog.getStorageConfigInfo(),
            List.of(
                AzureStorageConfigInfo.builder()
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
                    .setTenantId("tenant-id")
                    .setAllowedLocations(List.of("abfs://container@acct.dfs.core.windows.net/"))
                    .setStorageName("hot")
                    .build()));
    assertThatThrownBy(
            () ->
                services
                    .catalogsApi()
                    .updateCatalog(
                        catalogName,
                        updateRequest,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageStartingWith("Cannot modify storage type");
  }

  @Test
  public void testUpdateCatalogNamedConfigAwsAccountIdBlockedByDefault() {
    String catalogName = "mycatalog";
    Catalog fetchedCatalog =
        createCatalogWithDefaultAndNamedAwsConfig(
            catalogName, "arn:aws:iam::123456789012:role/named-role", "named-external-id");

    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/path/to/data"),
            fetchedCatalog.getStorageConfigInfo(),
            List.of(
                AwsStorageConfigInfo.builder()
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                    .setAllowedLocations(List.of("s3://named-bucket/path/"))
                    .setRoleArn("arn:aws:iam::999999999999:role/named-role")
                    .setExternalId("named-external-id")
                    .setStorageName("hot")
                    .build()));
    assertThatThrownBy(
            () ->
                services
                    .catalogsApi()
                    .updateCatalog(
                        catalogName,
                        updateRequest,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageStartingWith("Cannot modify AWS account ID");
  }

  @Test
  public void testUpdateCatalogNamedConfigExternalIdBlockedByDefault() {
    String catalogName = "mycatalog";
    Catalog fetchedCatalog =
        createCatalogWithDefaultAndNamedAwsConfig(
            catalogName, "arn:aws:iam::123456789012:role/named-role", "named-external-id");

    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/path/to/data"),
            fetchedCatalog.getStorageConfigInfo(),
            List.of(
                AwsStorageConfigInfo.builder()
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                    .setAllowedLocations(List.of("s3://named-bucket/path/"))
                    .setRoleArn("arn:aws:iam::123456789012:role/named-role")
                    .setExternalId("different-external-id")
                    .setStorageName("hot")
                    .build()));
    assertThatThrownBy(
            () ->
                services
                    .catalogsApi()
                    .updateCatalog(
                        catalogName,
                        updateRequest,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageStartingWith("Cannot modify ExternalId");
  }

  @Test
  public void testUpdateCatalogNamedConfigAzureTenantIdBlockedByDefault() {
    String catalogName = "mycatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder()
                    .setRoleArn("arn:aws:iam::123456789012:role/my-role")
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                    .setAllowedLocations(List.of("s3://bucket/path/to/data"))
                    .build())
            .setStorageConfigInfos(
                List.of(
                    AzureStorageConfigInfo.builder()
                        .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
                        .setTenantId("tenant-id")
                        .setAllowedLocations(List.of("abfs://container@acct.dfs.core.windows.net/"))
                        .setStorageName("archive")
                        .build()))
            .build();
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    Catalog fetchedCatalog;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(catalogName, services.realmContext(), services.securityContext())) {
      fetchedCatalog = (Catalog) response.getEntity();
    }

    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/path/to/data"),
            fetchedCatalog.getStorageConfigInfo(),
            List.of(
                AzureStorageConfigInfo.builder()
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
                    .setTenantId("different-tenant-id")
                    .setAllowedLocations(List.of("abfs://container@acct.dfs.core.windows.net/"))
                    .setStorageName("archive")
                    .build()));
    assertThatThrownBy(
            () ->
                services
                    .catalogsApi()
                    .updateCatalog(
                        catalogName,
                        updateRequest,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageStartingWith("Cannot modify TenantId");
  }

  @Test
  public void testUpdateCatalogNamedConfigRoleChangesAllowedWithFeatureFlag() {
    TestServices flagEnabledServices =
        TestServices.builder()
            .config(
                Map.of(
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("S3", "GCS", "AZURE"),
                    "ALLOW_UNRESTRICTED_STORAGE_CONFIG_ROLE_CHANGES",
                    Boolean.TRUE))
            .build();
    String catalogName = "mycatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder()
                    .setRoleArn("arn:aws:iam::123456789012:role/my-role")
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                    .setAllowedLocations(List.of("s3://bucket/path/to/data"))
                    .build())
            .setStorageConfigInfos(
                List.of(
                    AwsStorageConfigInfo.builder()
                        .setRoleArn("arn:aws:iam::123456789012:role/named-role")
                        .setExternalId("named-external-id")
                        .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                        .setAllowedLocations(List.of("s3://named-bucket/path/"))
                        .setStorageName("hot")
                        .build()))
            .build();
    try (Response response =
        flagEnabledServices
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                flagEnabledServices.realmContext(),
                flagEnabledServices.securityContext())) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    Catalog fetchedCatalog;
    try (Response response =
        flagEnabledServices
            .catalogsApi()
            .getCatalog(
                catalogName,
                flagEnabledServices.realmContext(),
                flagEnabledServices.securityContext())) {
      fetchedCatalog = (Catalog) response.getEntity();
    }

    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/path/to/data"),
            fetchedCatalog.getStorageConfigInfo(),
            List.of(
                AwsStorageConfigInfo.builder()
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                    .setAllowedLocations(List.of("s3://named-bucket/path/"))
                    .setRoleArn("arn:aws:iam::999999999999:role/other-role")
                    .setExternalId("different-external-id")
                    .setStorageName("hot")
                    .build()));
    try (Response response =
        flagEnabledServices
            .catalogsApi()
            .updateCatalog(
                catalogName,
                updateRequest,
                flagEnabledServices.realmContext(),
                flagEnabledServices.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testUpdateCatalogAddingNewNamedConfigNotConstrained() {
    String catalogName = "mycatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder()
                    .setRoleArn("arn:aws:iam::123456789012:role/my-role")
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                    .setAllowedLocations(List.of("s3://bucket/path/to/data"))
                    .build())
            .build();
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    Catalog fetchedCatalog;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(catalogName, services.realmContext(), services.securityContext())) {
      fetchedCatalog = (Catalog) response.getEntity();
    }

    // Introducing a brand-new named entry has no prior value to compare against, so it is never
    // a constrained change even though it carries role-identity fields.
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/path/to/data"),
            fetchedCatalog.getStorageConfigInfo(),
            List.of(
                AwsStorageConfigInfo.builder()
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                    .setAllowedLocations(List.of("s3://named-bucket/path/"))
                    .setRoleArn("arn:aws:iam::999999999999:role/named-role")
                    .setExternalId("named-external-id")
                    .setStorageName("hot")
                    .build()));
    try (Response response =
        services
            .catalogsApi()
            .updateCatalog(
                catalogName, updateRequest, services.realmContext(), services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testUpdateCatalogRemovingNamedConfigNotConstrained() {
    String catalogName = "mycatalog";
    Catalog fetchedCatalog =
        createCatalogWithDefaultAndNamedAwsConfig(
            catalogName, "arn:aws:iam::123456789012:role/named-role", "named-external-id");

    // Supplying an empty array removes the named entry; removal is never a constrained change.
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/path/to/data"),
            fetchedCatalog.getStorageConfigInfo(),
            List.of());
    try (Response response =
        services
            .catalogsApi()
            .updateCatalog(
                catalogName, updateRequest, services.realmContext(), services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    }

    try (Response response =
        services
            .catalogsApi()
            .getCatalog(catalogName, services.realmContext(), services.securityContext())) {
      Catalog updatedCatalog = (Catalog) response.getEntity();
      assertThat(updatedCatalog.getStorageConfigInfos()).isNull();
    }
  }

  @Test
  public void testUpdateCatalogMixedValidAndInvalidNamedConfigsRejectedWhole() {
    String catalogName = "mycatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder()
                    .setRoleArn("arn:aws:iam::123456789012:role/my-role")
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                    .setAllowedLocations(List.of("s3://bucket/path/to/data"))
                    .build())
            .setStorageConfigInfos(
                List.of(
                    AwsStorageConfigInfo.builder()
                        .setRoleArn("arn:aws:iam::123456789012:role/valid-role")
                        .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                        .setAllowedLocations(List.of("s3://valid-bucket/path/"))
                        .setStorageName("valid")
                        .build(),
                    AwsStorageConfigInfo.builder()
                        .setRoleArn("arn:aws:iam::123456789012:role/blocked-role")
                        .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                        .setAllowedLocations(List.of("s3://blocked-bucket/path/"))
                        .setStorageName("blocked")
                        .build()))
            .build();
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    Catalog fetchedCatalog;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(catalogName, services.realmContext(), services.securityContext())) {
      fetchedCatalog = (Catalog) response.getEntity();
    }

    // "valid" changes only its allowed locations (unconstrained); "blocked" changes its AWS
    // account ID (constrained). The whole request must be rejected and nothing persisted.
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/path/to/data"),
            fetchedCatalog.getStorageConfigInfo(),
            List.of(
                AwsStorageConfigInfo.builder()
                    .setRoleArn("arn:aws:iam::123456789012:role/valid-role")
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                    .setAllowedLocations(List.of("s3://valid-bucket/path/", "s3://valid-bucket2/"))
                    .setStorageName("valid")
                    .build(),
                AwsStorageConfigInfo.builder()
                    .setRoleArn("arn:aws:iam::999999999999:role/blocked-role")
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                    .setAllowedLocations(List.of("s3://blocked-bucket/path/"))
                    .setStorageName("blocked")
                    .build()));
    assertThatThrownBy(
            () ->
                services
                    .catalogsApi()
                    .updateCatalog(
                        catalogName,
                        updateRequest,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageStartingWith("Cannot modify AWS account ID");

    // Verify nothing was persisted: the stored catalog still has the original "valid" locations.
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(catalogName, services.realmContext(), services.securityContext())) {
      Catalog unchangedCatalog = (Catalog) response.getEntity();
      StorageConfigInfo validConfig =
          unchangedCatalog.getStorageConfigInfos().stream()
              .filter(c -> "valid".equals(c.getStorageName()))
              .findFirst()
              .orElseThrow();
      assertThat(validConfig.getAllowedLocations()).containsExactly("s3://valid-bucket/path/");
    }
  }
}
