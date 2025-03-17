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
import org.apache.iceberg.exceptions.BadRequestException;
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
    Catalog fetchedCatalog = getPolariCatalogEntity(DEFAULT_CATALOG_NAME, S3_BASE_LOCATION);
    AwsStorageConfigInfo awsStorageConfigInfo =
        (AwsStorageConfigInfo) fetchedCatalog.getStorageConfigInfo();
    assertThat(awsStorageConfigInfo.getUserArn()).isEqualTo(S3_IAM_USER_ARN);
    assertThat(awsStorageConfigInfo.getExternalId()).isEqualTo(S3_EXTERNAL_ID);

    // Case 1: Update the catalog by providing a user arn
    AwsStorageConfigInfo updatedAwsStorageModel =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(S3_BASE_LOCATION))
            .setRoleArn(S3_IAM_ROLE_ARN)
            .setUserArn(S3_IAM_USER_ARN + "-new")
            .build();
    updatePolarisCatalogEntity(
        DEFAULT_CATALOG_NAME, S3_BASE_LOCATION, updatedAwsStorageModel, "Cannot modify userARN in storage config");
    awsStorageConfigInfo = (AwsStorageConfigInfo) fetchedCatalog.getStorageConfigInfo();
    assertThat(awsStorageConfigInfo.getUserArn()).isEqualTo(S3_IAM_USER_ARN);
    assertThat(awsStorageConfigInfo.getExternalId()).isEqualTo(S3_EXTERNAL_ID);

    // Case 2: Update the catalog by providing an external id
    updatedAwsStorageModel =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(S3_BASE_LOCATION))
            .setRoleArn(S3_IAM_ROLE_ARN)
            .setExternalId(S3_EXTERNAL_ID + "-new")
            .build();
    updatePolarisCatalogEntity(
        DEFAULT_CATALOG_NAME,
        S3_BASE_LOCATION,
        updatedAwsStorageModel,
        "Cannot modify ExternalId in storage config");

    // Case 3: Update the catalog without the user arn and external id, should inherit existing
    // values
    updatedAwsStorageModel =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(S3_BASE_LOCATION))
            .setRoleArn(S3_IAM_ROLE_ARN)
            .build();
    Catalog updatedCatalog =
        updatePolarisCatalogEntity(
            DEFAULT_CATALOG_NAME, S3_BASE_LOCATION, updatedAwsStorageModel, null);
    AwsStorageConfigInfo updatedAwsStorageConfigInfo =
        (AwsStorageConfigInfo) updatedCatalog.getStorageConfigInfo();
    assertThat(updatedAwsStorageConfigInfo.getUserArn()).isEqualTo(S3_IAM_USER_ARN);
    assertThat(updatedAwsStorageConfigInfo.getExternalId()).isEqualTo(S3_EXTERNAL_ID);
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
    Catalog fetchedCatalog = getPolariCatalogEntity(DEFAULT_CATALOG_NAME, AZ_BASE_LOCATION);
    AzureStorageConfigInfo azureStorageConfigInfo =
        (AzureStorageConfigInfo) fetchedCatalog.getStorageConfigInfo();
    assertThat(azureStorageConfigInfo.getConsentUrl()).isEqualTo(AZ_CONSENT_URL);
    assertThat(azureStorageConfigInfo.getMultiTenantAppName()).isEqualTo(AZ_MULTI_TENANT_APP_NAME);

    // Case 1: Update the catalog by specifying the consent url
    AzureStorageConfigInfo updatedAzureStorageModel =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setAllowedLocations(List.of(AZ_BASE_LOCATION))
            .setTenantId(AZ_TENANT_ID)
            .setConsentUrl(AZ_CONSENT_URL + "-new")
            .build();
    updatePolarisCatalogEntity(
        DEFAULT_CATALOG_NAME,
        AZ_BASE_LOCATION,
        updatedAzureStorageModel,
        "Cannot modify consentUrl in storage config");

    // Case 2: Update the catalog by specifying the multi tenant app name
    updatedAzureStorageModel =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setAllowedLocations(List.of(AZ_BASE_LOCATION))
            .setTenantId(AZ_TENANT_ID)
            .setMultiTenantAppName(AZ_MULTI_TENANT_APP_NAME + "-new")
            .build();
    updatePolarisCatalogEntity(
        DEFAULT_CATALOG_NAME,
        AZ_BASE_LOCATION,
        updatedAzureStorageModel,
        "Cannot modify multiTenantAppName in storage config");

    // Case 3: Update the catalog without specifying the consent url and multi tenant app name,
    // should inherit existing values
    updatedAzureStorageModel =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setAllowedLocations(List.of(AZ_BASE_LOCATION))
            .setTenantId(AZ_TENANT_ID)
            .build();
    Catalog updatedCatalog =
        updatePolarisCatalogEntity(
            DEFAULT_CATALOG_NAME, AZ_BASE_LOCATION, updatedAzureStorageModel, null);
    AzureStorageConfigInfo updatedAzureStorageConfigInfo =
        (AzureStorageConfigInfo) updatedCatalog.getStorageConfigInfo();
    assertThat(updatedAzureStorageConfigInfo.getConsentUrl()).isEqualTo(AZ_CONSENT_URL);
    assertThat(updatedAzureStorageConfigInfo.getMultiTenantAppName())
        .isEqualTo(AZ_MULTI_TENANT_APP_NAME);
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
    Catalog fetchedCatalog = getPolariCatalogEntity(DEFAULT_CATALOG_NAME, GCP_BASE_LOCATION);
    GcpStorageConfigInfo gcpStorageConfigInfo =
        (GcpStorageConfigInfo) fetchedCatalog.getStorageConfigInfo();
    assertThat(gcpStorageConfigInfo.getGcsServiceAccount()).isEqualTo(GCP_SERVICE_ACCOUNT);

    // Case 1: Update the catalog by providing a service account
    GcpStorageConfigInfo updatedGcpStorageModel =
        GcpStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setAllowedLocations(List.of(GCP_BASE_LOCATION))
            .setGcsServiceAccount(GCP_SERVICE_ACCOUNT + "-new")
            .build();
    updatePolarisCatalogEntity(
        DEFAULT_CATALOG_NAME,
        GCP_BASE_LOCATION,
        updatedGcpStorageModel,
        "Cannot modify gcpServiceAccount in storage config");

    // Case 2: Update the catalog without specifying the service account, should inherit the
    // existing value
    updatedGcpStorageModel =
        GcpStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setAllowedLocations(List.of(GCP_BASE_LOCATION))
            .build();
    Catalog updatedCatalog =
        updatePolarisCatalogEntity(
            DEFAULT_CATALOG_NAME, GCP_BASE_LOCATION, updatedGcpStorageModel, null);
    gcpStorageConfigInfo = (GcpStorageConfigInfo) updatedCatalog.getStorageConfigInfo();
    assertThat(gcpStorageConfigInfo.getGcsServiceAccount()).isEqualTo(GCP_SERVICE_ACCOUNT);
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

  private Catalog getPolariCatalogEntity(String catalogName, String defaultBaseLocation) {
    Catalog fetchedCatalog;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(
                DEFAULT_CATALOG_NAME, services.realmContext(), services.securityContext())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = (Catalog) response.getEntity();

      assertThat(fetchedCatalog.getName()).isEqualTo(catalogName);
      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, defaultBaseLocation));
      assertThat(fetchedCatalog.getEntityVersion()).isGreaterThan(0);
    }
    return fetchedCatalog;
  }

  private Catalog updatePolarisCatalogEntity(
      String catalogName,
      String defaultBaseLocation,
      StorageConfigInfo updatedStorageConfigModel,
      String errorMessage) {
    Catalog fetchedCatalog = getPolariCatalogEntity(catalogName, defaultBaseLocation);
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, defaultBaseLocation),
            updatedStorageConfigModel);

    if (errorMessage != null) {
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
          .hasMessageContaining(errorMessage);

    } else {
      try (Response response =
          services
              .catalogsApi()
              .updateCatalog(
                  catalogName,
                  updateRequest,
                  services.realmContext(),
                  services.securityContext())) {
        assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
        return (Catalog) response.getEntity();
      }
    }
    return null;
  }
}
