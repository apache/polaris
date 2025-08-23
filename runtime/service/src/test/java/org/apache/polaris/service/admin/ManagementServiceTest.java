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
import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.secrets.UnsafeInMemorySecretsManager;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.config.ReservedProperties;
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
            .config(Map.of("SUPPORTED_CATALOG_STORAGE_TYPES", List.of("S3", "GCS", "AZURE")))
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
  }

  private PolarisMetaStoreManager setupMetaStoreManager() {
    MetaStoreManagerFactory metaStoreManagerFactory = services.metaStoreManagerFactory();
    RealmContext realmContext = services.realmContext();
    return metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
  }

  private PolarisAdminService setupPolarisAdminService(
      PolarisMetaStoreManager metaStoreManager, PolarisCallContext callContext) {
    return new PolarisAdminService(
        callContext,
        services.resolutionManifestFactory(),
        metaStoreManager,
        new UnsafeInMemorySecretsManager(),
        new SecurityContext() {
          @Override
          public Principal getUserPrincipal() {
            return PolarisPrincipal.of(
                new PrincipalEntity.Builder()
                    .setName(PolarisEntityConstants.getRootPrincipalName())
                    .build(),
                Set.of(PolarisEntityConstants.getNameOfPrincipalServiceAdminRole()));
          }

          @Override
          public boolean isUserInRole(String role) {
            return true;
          }

          @Override
          public boolean isSecure() {
            return false;
          }

          @Override
          public String getAuthenticationScheme() {
            return "";
          }
        },
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
    PolarisMetaStoreManager metaStoreManager = setupMetaStoreManager();
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
    PolarisMetaStoreManager metaStoreManager = setupMetaStoreManager();
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
  public void testCreateCatalogReturnErrorOnFailure() {
    PolarisMetaStoreManager metaStoreManager = Mockito.spy(setupMetaStoreManager());
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
