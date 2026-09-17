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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
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
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
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
            fileStorage);

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
                .build());
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
        ReservedProperties.NONE,
        services.vendingMechanisms());
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
                .build());
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
                .build());
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
                .build());
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
                .build());
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

  @Test
  public void anEmptyMechanismIsReadBackAsEmpty() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://bucket/path/to/data"))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("mechanism-default")
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
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
    try (Response response =
        services
            .catalogsApi()
            .getCatalog("mechanism-default", services.realmContext(), services.securityContext())) {
      Catalog fetched = (Catalog) response.getEntity();
      assertThat(
              ((AwsStorageConfigInfo) fetched.getStorageConfigInfo())
                  .getCredentialVendingMechanism())
          .isNull();
    }
  }

  private static final String TEST_MECHANISM = "TEST_MECHANISM";

  private static TestServices mechanismServices(
      List<String> mechanisms, boolean unrestrictedChanges) {
    return TestServices.builder()
        .config(
            Map.of(
                "SUPPORTED_CATALOG_STORAGE_TYPES",
                List.of("S3", "GCS", "AZURE"),
                "SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS",
                mechanisms,
                "ALLOW_UNRESTRICTED_STORAGE_CONFIG_ROLE_CHANGES",
                unrestrictedChanges))
        .additionalVendingMechanisms(Map.of(TEST_MECHANISM, TestServices.fakeMechanism()))
        .build();
  }

  private static AwsStorageConfigInfo.Builder secondMechanismConfig() {
    return AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
        .setCredentialVendingMechanism(TEST_MECHANISM)
        .setAllowedLocations(List.of("s3://second-bucket/base/"));
  }

  private static AwsStorageConfigInfo emptyMechanismConfig() {
    return AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
        .setRoleArn("arn:aws:iam::123456789012:role/my-role")
        .setAllowedLocations(List.of("s3://second-bucket/base/"))
        .build();
  }

  private static Catalog catalogNamed(String name, StorageConfigInfo storageConfig) {
    return PolarisCatalog.builder()
        .setType(Catalog.TypeEnum.INTERNAL)
        .setName(name)
        .setProperties(new CatalogProperties("s3://second-bucket/base/" + name))
        .setStorageConfigInfo(storageConfig)
        .build();
  }

  private static Response create(TestServices svc, Catalog catalog) {
    return svc.catalogsApi()
        .createCatalog(
            new CreateCatalogRequest(catalog), svc.realmContext(), svc.securityContext());
  }

  private static Catalog fetch(TestServices svc, String name) {
    try (Response response =
        svc.catalogsApi().getCatalog(name, svc.realmContext(), svc.securityContext())) {
      return (Catalog) response.getEntity();
    }
  }

  @Test
  public void testASecondMechanismIsRejectedByTheDefaultAllowlist() {
    TestServices defaults = mechanismServices(List.of("STS"), false);
    assertThatThrownBy(
            () -> create(defaults, catalogNamed("second-off", secondMechanismConfig().build())))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "S3 credential vending mechanism " + TEST_MECHANISM + " is not enabled in this realm");
  }

  @Test
  public void anExplicitStsIsRejectedWhenTheRealmListsOnlyASecondMechanism() {
    TestServices secondOnly = mechanismServices(List.of(TEST_MECHANISM), false);
    AwsStorageConfigInfo sts =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setAllowedLocations(List.of("s3://second-bucket/base/"))
            .setCredentialVendingMechanism("STS")
            .build();
    assertThatThrownBy(() -> create(secondOnly, catalogNamed("sts-off", sts)))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential vending mechanism STS is not enabled in this realm");
  }

  @Test
  public void anEmptyMechanismIsAcceptedWhenTheRealmListsOnlyASecondMechanism() {
    TestServices secondOnly = mechanismServices(List.of(TEST_MECHANISM), false);
    AwsStorageConfigInfo empty =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setAllowedLocations(List.of("s3://second-bucket/base/"))
            .build();
    try (Response response = create(secondOnly, catalogNamed("empty-allowed", empty))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  @Test
  public void testDisallowedMechanismIsRejectedOnUpdateToo() {
    TestServices stsOnlyUnrestricted = mechanismServices(List.of("STS"), true);
    AwsStorageConfigInfo emptyMechanism =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setAllowedLocations(List.of("s3://second-bucket/base/"))
            .build();
    try (Response response =
        create(stsOnlyUnrestricted, catalogNamed("empty-stay", emptyMechanism))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
    Catalog fetched = fetch(stsOnlyUnrestricted, "empty-stay");
    UpdateCatalogRequest toSecond =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(),
            Map.of("default-base-location", "s3://second-bucket/base/empty-stay"),
            secondMechanismConfig().build());
    assertThatThrownBy(
            () ->
                stsOnlyUnrestricted
                    .catalogsApi()
                    .updateCatalog(
                        "empty-stay",
                        toSecond,
                        stsOnlyUnrestricted.realmContext(),
                        stsOnlyUnrestricted.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "S3 credential vending mechanism " + TEST_MECHANISM + " is not enabled in this realm");
  }

  @Test
  public void theLiteralDefaultIsReservedAtCreateAndUpdate() {
    TestServices svc = mechanismServices(List.of("STS"), true);
    String reserved =
        "S3 credential vending mechanism DEFAULT is reserved; leave the field empty to use the"
            + " server default";
    AwsStorageConfigInfo namesDefault =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setCredentialVendingMechanism("DEFAULT")
            .setAllowedLocations(List.of("s3://second-bucket/base/"))
            .build();
    assertThatThrownBy(() -> create(svc, catalogNamed("names-default", namesDefault)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(reserved);

    AwsStorageConfigInfo empty =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://second-bucket/base/"))
            .build();
    try (Response response = create(svc, catalogNamed("stays-empty", empty))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
    Catalog fetched = fetch(svc, "stays-empty");
    UpdateCatalogRequest toDefault =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(),
            Map.of("default-base-location", "s3://second-bucket/base/stays-empty"),
            namesDefault);
    assertThatThrownBy(
            () ->
                svc.catalogsApi()
                    .updateCatalog(
                        "stays-empty", toDefault, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(reserved);
  }

  @Test
  public void anEmptyMechanismCanBeUpdatedToAnExplicitSts() {
    TestServices svc = mechanismServices(List.of("STS"), false);
    AwsStorageConfigInfo empty =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://second-bucket/base/"))
            .build();
    AwsStorageConfigInfo explicitSts =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setCredentialVendingMechanism("STS")
            .setAllowedLocations(List.of("s3://second-bucket/base/"))
            .build();
    try (Response response = create(svc, catalogNamed("empty-then-sts", empty))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
    Catalog fetched = fetch(svc, "empty-then-sts");
    UpdateCatalogRequest toSts =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(),
            Map.of("default-base-location", "s3://second-bucket/base/empty-then-sts"),
            explicitSts);
    try (Response response =
        svc.catalogsApi()
            .updateCatalog("empty-then-sts", toSts, svc.realmContext(), svc.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    assertThat(
            ((AwsStorageConfigInfo) fetch(svc, "empty-then-sts").getStorageConfigInfo())
                .getCredentialVendingMechanism())
        .isEqualTo("STS");
  }

  @Test
  public void testASecondMechanismCatalogIsCreatedAndReadBack() {
    TestServices enabled = mechanismServices(List.of("STS", TEST_MECHANISM), false);
    try (Response response =
        create(enabled, catalogNamed("second-on", secondMechanismConfig().build()))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
    AwsStorageConfigInfo fetched =
        (AwsStorageConfigInfo) fetch(enabled, "second-on").getStorageConfigInfo();
    assertThat(fetched.getCredentialVendingMechanism()).isEqualTo(TEST_MECHANISM);
  }

  @Test
  public void changingTheMechanismIsAcceptedAndValidatedByTheNewMechanism() {
    EndpointRequiringMechanism mechanism = new EndpointRequiringMechanism();
    TestServices svc =
        TestServices.builder()
            .config(
                Map.of(
                    "SUPPORTED_CATALOG_STORAGE_TYPES", List.of("S3"),
                    "SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS", List.of("STS", TEST_MECHANISM)))
            .additionalVendingMechanisms(Map.of(TEST_MECHANISM, mechanism))
            .build();
    AwsStorageConfigInfo sts =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setCredentialVendingMechanism("STS")
            .setAllowedLocations(List.of("s3://second-bucket/base/"))
            .build();
    try (Response response = create(svc, catalogNamed("mechanism-change", sts))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
    Catalog fetched = fetch(svc, "mechanism-change");
    UpdateCatalogRequest toTestMechanism =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(),
            Map.of("default-base-location", "s3://second-bucket/base/mechanism-change"),
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setCredentialVendingMechanism(TEST_MECHANISM)
                .setAllowedLocations(List.of("s3://second-bucket/base/"))
                .setEndpoint("https://s3.example.test")
                .build());
    try (Response response =
        svc.catalogsApi()
            .updateCatalog(
                "mechanism-change", toTestMechanism, svc.realmContext(), svc.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    assertThat(
            ((AwsStorageConfigInfo) fetch(svc, "mechanism-change").getStorageConfigInfo())
                .getCredentialVendingMechanism())
        .isEqualTo(TEST_MECHANISM);
    assertThat(mechanism.currents).hasSize(1);
    assertThat(mechanism.currents.get(0).getCredentialVendingMechanism()).isEqualTo("STS");
    assertThat(mechanism.updateds).hasSize(1);
    assertThat(mechanism.updateds.get(0).getCredentialVendingMechanism()).isEqualTo(TEST_MECHANISM);
  }

  @Test
  public void emptyMechanismCatalogEndpointStaysMutable() {
    // The mechanism freeze never touches the endpoint of a catalog with an empty mechanism.
    TestServices svc = mechanismServices(List.of("STS"), false);
    AwsStorageConfigInfo emptyMechanism =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setAllowedLocations(List.of("s3://second-bucket/base/"))
            .setEndpoint("https://s3.example.com:1234")
            .setPathStyleAccess(true)
            .build();
    try (Response response = create(svc, catalogNamed("empty-mutable", emptyMechanism))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
    Catalog fetched = fetch(svc, "empty-mutable");
    UpdateCatalogRequest updateEndpoint =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(),
            Map.of("default-base-location", "s3://second-bucket/base/empty-mutable"),
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setRoleArn("arn:aws:iam::123456789012:role/my-role")
                .setAllowedLocations(List.of("s3://second-bucket/base/"))
                .setEndpoint("https://s3.other.example.com:1234")
                .setPathStyleAccess(true)
                .build());
    try (Response response =
        svc.catalogsApi()
            .updateCatalog(
                "empty-mutable", updateEndpoint, svc.realmContext(), svc.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    AwsStorageConfigInfo updated =
        (AwsStorageConfigInfo) fetch(svc, "empty-mutable").getStorageConfigInfo();
    assertThat(updated.getEndpoint()).isEqualTo("https://s3.other.example.com:1234");
  }

  @Test
  public void creatingACatalogWithAnAllowlistedButUninstalledMechanismIsRefused() {
    TestServices svc = mechanismServices(List.of("STS", "UNINSTALLED_MECHANISM"), false);
    AwsStorageConfigInfo uninstalled =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setCredentialVendingMechanism("UNINSTALLED_MECHANISM")
            .setAllowedLocations(List.of("s3://second-bucket/base/"))
            .build();
    assertThatThrownBy(() -> create(svc, catalogNamed("never-usable", uninstalled)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "S3 credential vending mechanism UNINSTALLED_MECHANISM is not available in this server");
  }

  @Test
  public void updatingACatalogToAnAllowlistedButUninstalledMechanismIsRefused() {
    TestServices svc = mechanismServices(List.of("STS", "UNINSTALLED_MECHANISM"), true);
    try (Response response = create(svc, catalogNamed("empty-stays", emptyMechanismConfig()))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
    Catalog fetched = fetch(svc, "empty-stays");
    UpdateCatalogRequest toUninstalled =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(),
            Map.of("default-base-location", "s3://second-bucket/base/empty-stays"),
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setCredentialVendingMechanism("UNINSTALLED_MECHANISM")
                .setAllowedLocations(List.of("s3://second-bucket/base/"))
                .build());
    assertThatThrownBy(
            () ->
                svc.catalogsApi()
                    .updateCatalog(
                        "empty-stays", toUninstalled, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "S3 credential vending mechanism UNINSTALLED_MECHANISM is not available in this server");
  }

  /** A mechanism that refuses any config without an endpoint, and records what it was given. */
  private static final class EndpointRequiringMechanism implements S3CredentialVendingMechanism {
    final List<AwsStorageConfigurationInfo> currents = new ArrayList<>();
    final List<AwsStorageConfigurationInfo> updateds = new ArrayList<>();

    @Override
    public PolarisStorageIntegration integrationFor(AwsStorageConfigurationInfo storageConfig) {
      return TestServices.fakeMechanism().integrationFor(storageConfig);
    }

    @Override
    public void validate(AwsStorageConfigurationInfo current, AwsStorageConfigurationInfo updated) {
      currents.add(current);
      updateds.add(updated);
      if (updated.getEndpoint() == null) {
        throw new IllegalArgumentException("endpoint is required for the TEST_MECHANISM mechanism");
      }
    }
  }

  @Test
  public void theMechanismValidatesTheConfigAtCreateAndAtUpdate() {
    EndpointRequiringMechanism mechanism = new EndpointRequiringMechanism();
    TestServices svc =
        TestServices.builder()
            .config(
                Map.of(
                    "SUPPORTED_CATALOG_STORAGE_TYPES", List.of("S3"),
                    "SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS", List.of("STS", TEST_MECHANISM)))
            .additionalVendingMechanisms(Map.of(TEST_MECHANISM, mechanism))
            .build();
    AwsStorageConfigInfo.Builder base =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setCredentialVendingMechanism(TEST_MECHANISM)
            .setAllowedLocations(List.of("s3://second-bucket/base/"));

    assertThatThrownBy(() -> create(svc, catalogNamed("needs-endpoint", base.build())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("endpoint is required for the TEST_MECHANISM mechanism");
    assertThat(mechanism.currents).containsExactly((AwsStorageConfigurationInfo) null);

    try (Response response =
        create(
            svc,
            catalogNamed("needs-endpoint", base.setEndpoint("https://s3.example.test").build()))) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
    assertThat(mechanism.currents).hasSize(2);
    assertThat(mechanism.updateds.get(1).getEndpoint()).isEqualTo("https://s3.example.test");

    Catalog fetched = fetch(svc, "needs-endpoint");
    UpdateCatalogRequest dropEndpoint =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(),
            Map.of("default-base-location", "s3://second-bucket/base/needs-endpoint"),
            base.setEndpoint(null).build());
    assertThatThrownBy(
            () ->
                svc.catalogsApi()
                    .updateCatalog(
                        "needs-endpoint", dropEndpoint, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("endpoint is required for the TEST_MECHANISM mechanism");
    assertThat(mechanism.currents).hasSize(3);
    assertThat(mechanism.currents.get(2)).isNotNull();
    assertThat(mechanism.currents.get(2).getEndpoint()).isEqualTo("https://s3.example.test");
  }
}
