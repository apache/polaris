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
package org.apache.polaris.service.quarkus.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ManagementServiceTest {
  private TestServices services;
  private PolarisCallContext polarisCallContext;

  private static final String DEFAULT_CATALOG_NAME = "mycatalog";
  private static final String S3_BASE_LOCATION = "s3://bucket/path/to/data";
  private static final String S3_IAM_ROLE_ARN = "arn:aws:iam::123456789012:role/my-role";
  private static final String S3_IAM_USER_ARN = "arn:aws:iam::123456789012:user/my-user";
  private static final String S3_EXTERNAL_ID = "externalId";
  private static final String AZ_BASE_LOCATION =
      "abfss://mycontainer@mystorageaccount.dfs.core.windows.net/my-table";
  private static final String AZ_TENANT_ID = "12345678-90ab-cdef-1234-567890abcdef";
  private static final String AZ_CONSENT_URL = "https://login.microsoftonline.com/xxx";
  private static final String AZ_MULTI_TENANT_APP_NAME = "myapp";
  private static final String GCP_BASE_LOCATION = "gs://bucket/path/to/data";
  private static final String GCP_SERVICE_ACCOUNT =
      "my-service-account@my-project.iam.gserviceaccount.com";

  @BeforeEach
  public void setup() {
    // Used to build a `CallContext` which then gets fed into the real `TestServices`
    // TestServices fakeServices =
    //     TestServices.builder()
    //         .config(Map.of("SUPPORTED_CATALOG_STORAGE_TYPES", List.of("S3", "GCS", "AZURE")))
    //         .build();
    // PolarisCallContext polarisCallContext =
    //     new PolarisCallContext(
    //         fakeServices
    //             .metaStoreManagerFactory()
    //             .getOrCreateSessionSupplier(fakeServices.realmContext())
    //             .get(),
    //         fakeServices.polarisDiagnostics(),
    //         fakeServices.configurationStore(),
    //         Mockito.mock(Clock.class));
    // CallContext.setCurrentContext(CallContext.of(fakeServices.realmContext(),
    // polarisCallContext));
    services =
        TestServices.builder()
            .config(Map.of("SUPPORTED_CATALOG_STORAGE_TYPES", List.of("S3", "GCS", "AZURE")))
            .build();
    this.polarisCallContext =
        new PolarisCallContext(
            services.metaStoreSession(),
            services.polarisDiagnostics(),
            services.configurationStore(),
            Mockito.mock());
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

  @Test
  public void testUpdateCatalogWithServiceProvidedAwsStorageConfig() {
    // Create an AWS storage configuration with externalId and user arn, we can't use
    // AwsStorageConfigInfo to create the catalog since these properties should be provided
    // by polaris service and will be ignored
    AwsStorageConfigurationInfo awsStorageConfigurationInfo =
        new AwsStorageConfigurationInfo(
            PolarisStorageConfigurationInfo.StorageType.S3,
            List.of(S3_BASE_LOCATION),
            S3_IAM_ROLE_ARN,
            S3_EXTERNAL_ID,
            null);
    awsStorageConfigurationInfo.setUserARN(S3_IAM_USER_ARN);

    createPolarisCatalogEntity(DEFAULT_CATALOG_NAME, S3_BASE_LOCATION, awsStorageConfigurationInfo);

    // 200 successful GET after creation
    Catalog fetchedCatalog;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(
                DEFAULT_CATALOG_NAME, services.realmContext(), services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = (Catalog) response.getEntity();
      System.out.println(fetchedCatalog);

      assertThat(fetchedCatalog.getName()).isEqualTo(DEFAULT_CATALOG_NAME);
      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, S3_BASE_LOCATION));
      assertThat(fetchedCatalog.getEntityVersion()).isGreaterThan(0);
    }

    // Update the catalog without specifying the external id, should inherit the existing value
    AwsStorageConfigInfo updatedAwsStorageModel =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn(S3_IAM_ROLE_ARN)
            .setAllowedLocations(List.of(S3_BASE_LOCATION))
            .build();
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, S3_BASE_LOCATION),
            updatedAwsStorageModel);
    try (Response response =
        services
            .catalogsApi()
            .updateCatalog(
                DEFAULT_CATALOG_NAME,
                updateRequest,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog updatedCatalog = (Catalog) response.getEntity();
      AwsStorageConfigInfo awsStorageConfigInfo =
          (AwsStorageConfigInfo) updatedCatalog.getStorageConfigInfo();
      assertThat(awsStorageConfigInfo.getUserArn()).isEqualTo(S3_IAM_USER_ARN);
      assertThat(awsStorageConfigInfo.getExternalId()).isEqualTo(S3_EXTERNAL_ID);
    }
  }

  @Test
  public void testUpdateCatalogWithServiceProvidedAzureStorageConfig() {
    // Create an Azure storage configuration with consent url and multi tenant app name, we can't
    // use Azure StorageConfigInfo to create the catalog since these properties should be provided
    // by polaris service and will be ignored
    AzureStorageConfigurationInfo azureStorageConfigurationInfo =
        new AzureStorageConfigurationInfo(List.of(AZ_BASE_LOCATION), AZ_TENANT_ID);
    azureStorageConfigurationInfo.setConsentUrl(AZ_CONSENT_URL);
    azureStorageConfigurationInfo.setMultiTenantAppName(AZ_MULTI_TENANT_APP_NAME);

    createPolarisCatalogEntity(
        DEFAULT_CATALOG_NAME, AZ_BASE_LOCATION, azureStorageConfigurationInfo);

    // 200 successful GET after creation
    Catalog fetchedCatalog;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(
                DEFAULT_CATALOG_NAME, services.realmContext(), services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = (Catalog) response.getEntity();
      System.out.println(fetchedCatalog);

      assertThat(fetchedCatalog.getName()).isEqualTo(DEFAULT_CATALOG_NAME);
      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, AZ_BASE_LOCATION));
      assertThat(fetchedCatalog.getEntityVersion()).isGreaterThan(0);
    }

    // Update the catalog without specifying the external id, should inherit the existing value
    AzureStorageConfigInfo updatedAzureStorageModel =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setTenantId(AZ_TENANT_ID)
            .setAllowedLocations(List.of(AZ_BASE_LOCATION))
            .build();
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, AZ_BASE_LOCATION),
            updatedAzureStorageModel);
    try (Response response =
        services
            .catalogsApi()
            .updateCatalog(
                DEFAULT_CATALOG_NAME,
                updateRequest,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog updatedCatalog = (Catalog) response.getEntity();
      AzureStorageConfigInfo azureStorageConfigInfo =
          (AzureStorageConfigInfo) updatedCatalog.getStorageConfigInfo();
      assertThat(azureStorageConfigInfo.getConsentUrl()).isEqualTo(AZ_CONSENT_URL);
      assertThat(azureStorageConfigInfo.getMultiTenantAppName())
          .isEqualTo(AZ_MULTI_TENANT_APP_NAME);
    }
  }

  @Test
  public void testUpdateCatalogWithServiceProvidedGcpStorageConfig() {
    // Create an GCP storage configuration with service name, we can't
    // use Azure StorageConfigInfo to create the catalog since these properties should be provided
    // by polaris service and will be ignored
    GcpStorageConfigurationInfo gcpStorageConfigurationInfo =
        new GcpStorageConfigurationInfo(List.of());
    gcpStorageConfigurationInfo.setGcpServiceAccount(GCP_SERVICE_ACCOUNT);

    createPolarisCatalogEntity(
        DEFAULT_CATALOG_NAME, GCP_BASE_LOCATION, gcpStorageConfigurationInfo);

    // 200 successful GET after creation
    Catalog fetchedCatalog;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(
                DEFAULT_CATALOG_NAME, services.realmContext(), services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = (Catalog) response.getEntity();
      System.out.println(fetchedCatalog);

      assertThat(fetchedCatalog.getName()).isEqualTo(DEFAULT_CATALOG_NAME);
      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, GCP_BASE_LOCATION));
      assertThat(fetchedCatalog.getEntityVersion()).isGreaterThan(0);
    }

    // Update the catalog without specifying the external id, should inherit the existing value
    GcpStorageConfigInfo updatedGcpStorageModel =
        GcpStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setAllowedLocations(List.of(GCP_BASE_LOCATION))
            .build();
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, GCP_BASE_LOCATION),
            updatedGcpStorageModel);
    try (Response response =
        services
            .catalogsApi()
            .updateCatalog(
                DEFAULT_CATALOG_NAME,
                updateRequest,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog updatedCatalog = (Catalog) response.getEntity();
      GcpStorageConfigInfo gcpStorageConfigInfo =
          (GcpStorageConfigInfo) updatedCatalog.getStorageConfigInfo();
      assertThat(gcpStorageConfigInfo.getGcsServiceAccount()).isEqualTo(GCP_SERVICE_ACCOUNT);
    }
  }

  private PolarisBaseEntity createPolarisCatalogEntity(
      String catalogName,
      String defaultBaseLocation,
      PolarisStorageConfigurationInfo storageConfigInfo) {
    CatalogEntity catalogEntity =
        new CatalogEntity.Builder()
            .setName(catalogName)
            .setId(services.metaStoreManager().generateNewEntityId(polarisCallContext).getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .setCatalogType(Catalog.TypeEnum.INTERNAL.name())
            .setProperties(Map.of(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, defaultBaseLocation))
            .setInternalProperties(
                Map.of(
                    PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                    storageConfigInfo.serialize()))
            .build();
    return services
        .metaStoreManager()
        .createCatalog(polarisCallContext, catalogEntity, List.of())
        .getCatalog();
  }
}
