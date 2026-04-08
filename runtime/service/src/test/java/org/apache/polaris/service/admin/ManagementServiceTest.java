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
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"));
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
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
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
            null,
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
                .setRoleArn("arn:aws:iam::123456789012:role/my-role")
                .setEndpoint("http://example.com")
                .build(),
            null,
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
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
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
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
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
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            new PrincipalEntity.Builder()
                .setName(PolarisEntityConstants.getRootPrincipalName())
                .build(),
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
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
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
                .setRoleArn("arn:aws:iam::999999999999:role/other-role")
                .build(),
            null,
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
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
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
                .setRoleArn("arn:aws:iam::123456789012:role/other-role")
                .build(),
            null,
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
  public void testCreateCatalogWithLabels() {
    AwsStorageConfigInfo awsConfig =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/label-role")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://labels-bucket/"))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("labels-catalog")
            .setProperties(new CatalogProperties("s3://labels-bucket/data"))
            .setStorageConfigInfo(awsConfig)
            .setLabels(Map.of("env", "prod", "team", "platform"))
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
            .getCatalog("labels-catalog", services.realmContext(), services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog fetched = (Catalog) response.getEntity();
      assertThat(fetched.getLabels())
          .containsEntry("env", "prod")
          .containsEntry("team", "platform");
    }
  }

  @Test
  public void testUpdateCatalogChangeExternalIdBlockedByDefault() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("my-external-id")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
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
                .setRoleArn("arn:aws:iam::123456789012:role/my-role")
                .setExternalId("different-external-id")
                .build(),
            null,
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
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
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
                .setRoleArn("arn:aws:iam::999999999999:role/other-role")
                .setExternalId("different-external-id")
                .build(),
            null,
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
  public void testUpdateCatalogLabelsPreservedWhenOmitted() {
    AwsStorageConfigInfo awsConfig =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/label-update-role")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://label-update-bucket/"))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("label-update-catalog")
            .setProperties(new CatalogProperties("s3://label-update-bucket/data"))
            .setStorageConfigInfo(awsConfig)
            .setLabels(Map.of("env", "staging"))
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

    Catalog fetched;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(
                "label-update-catalog", services.realmContext(), services.securityContext())) {
      fetched = (Catalog) response.getEntity();
    }

    // Update with labels omitted (null) — existing labels must be preserved
    UpdateCatalogRequest updateNoLabels =
        new UpdateCatalogRequest(fetched.getEntityVersion(), null, null, null, null);
    try (Response response =
        services
            .catalogsApi()
            .updateCatalog(
                "label-update-catalog",
                updateNoLabels,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog updated = (Catalog) response.getEntity();
      assertThat(updated.getLabels()).containsEntry("env", "staging");
    }
  }

  @Test
  public void testUpdateCatalogLabelsReplaced() {
    AwsStorageConfigInfo awsConfig =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/label-replace-role")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://label-replace-bucket/"))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("label-replace-catalog")
            .setProperties(new CatalogProperties("s3://label-replace-bucket/data"))
            .setStorageConfigInfo(awsConfig)
            .setLabels(Map.of("env", "dev", "old-key", "old-val"))
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

    Catalog fetched;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(
                "label-replace-catalog", services.realmContext(), services.securityContext())) {
      fetched = (Catalog) response.getEntity();
    }

    // Update with explicit new labels — old labels are fully replaced
    UpdateCatalogRequest updateWithLabels =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(), null, null, Map.of("env", "prod"), null);
    try (Response response =
        services
            .catalogsApi()
            .updateCatalog(
                "label-replace-catalog",
                updateWithLabels,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog updated = (Catalog) response.getEntity();
      assertThat(updated.getLabels()).containsEntry("env", "prod").doesNotContainKey("old-key");
    }
  }

  @Test
  public void testUpdateCatalogLabelsClearedWithClearLabelsFlag() {
    AwsStorageConfigInfo awsConfig =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/label-clear-role")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://label-clear-bucket/"))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("label-clear-catalog")
            .setProperties(new CatalogProperties("s3://label-clear-bucket/data"))
            .setStorageConfigInfo(awsConfig)
            .setLabels(Map.of("env", "prod", "team", "platform"))
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

    Catalog fetched;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(
                "label-clear-catalog", services.realmContext(), services.securityContext())) {
      fetched = (Catalog) response.getEntity();
    }

    // Clear labels with clearLabels=true
    UpdateCatalogRequest clearRequest =
        new UpdateCatalogRequest(fetched.getEntityVersion(), null, null, null, true);
    try (Response response =
        services
            .catalogsApi()
            .updateCatalog(
                "label-clear-catalog",
                clearRequest,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog updated = (Catalog) response.getEntity();
      assertThat(updated.getLabels()).isEmpty();
    }
  }

  @Test
  public void testUpdateCatalogClearLabelsAndLabelsMutuallyExclusive() {
    PolarisMetaStoreManager metaStoreManager = services.metaStoreManager();
    PolarisCallContext callContext = services.newCallContext();
    PolarisAdminService polarisAdminService =
        setupPolarisAdminService(metaStoreManager, callContext);

    AwsStorageConfigInfo awsConfig =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/mutex-role")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://mutex-bucket/"))
            .build();
    polarisAdminService.createCatalog(
        new CreateCatalogRequest(
            PolarisCatalog.builder()
                .setType(Catalog.TypeEnum.INTERNAL)
                .setName("mutex-catalog")
                .setProperties(new CatalogProperties("s3://mutex-bucket/data"))
                .setStorageConfigInfo(awsConfig)
                .setLabels(Map.of("env", "prod"))
                .build()));

    int version = polarisAdminService.getCatalog("mutex-catalog").getEntityVersion();
    UpdateCatalogRequest conflictRequest =
        new UpdateCatalogRequest(version, null, null, Map.of("env", "staging"), true);

    assertThatThrownBy(() -> polarisAdminService.updateCatalog("mutex-catalog", conflictRequest))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("mutually exclusive");
  }

  @Test
  public void testListCatalogsWithLabelFilter() {
    PolarisMetaStoreManager metaStoreManager = services.metaStoreManager();
    PolarisCallContext callContext = services.newCallContext();
    PolarisAdminService polarisAdminService =
        setupPolarisAdminService(metaStoreManager, callContext);

    AwsStorageConfigInfo prodConfig =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/filter-role")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://prod-filter-bucket/"))
            .build();

    AwsStorageConfigInfo devConfig =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/filter-role")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://dev-filter-bucket/"))
            .build();

    // Create catalog with prod label
    Catalog prodCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("prod-catalog")
            .setProperties(new CatalogProperties("s3://prod-filter-bucket/data"))
            .setStorageConfigInfo(prodConfig)
            .setLabels(Map.of("env", "prod"))
            .build();
    try (Response r =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(prodCatalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(r).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Create catalog with dev label
    Catalog devCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("dev-catalog")
            .setProperties(new CatalogProperties("s3://dev-filter-bucket/data"))
            .setStorageConfigInfo(devConfig)
            .setLabels(Map.of("env", "dev"))
            .build();
    try (Response r =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(devCatalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(r).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Filter by env=prod — should only return prod-catalog
    List<Catalog> prodOnly = polarisAdminService.listCatalogs(Map.of("env", "prod"));
    assertThat(prodOnly).extracting(Catalog::getName).containsExactly("prod-catalog");

    // No filter — should return both
    List<Catalog> all = polarisAdminService.listCatalogs(Map.of());
    assertThat(all)
        .extracting(Catalog::getName)
        .containsExactlyInAnyOrder("prod-catalog", "dev-catalog");
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
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
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
}
