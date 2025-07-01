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
package org.apache.polaris.service.it.test;

import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.apache.polaris.service.it.test.PolarisApplicationIntegrationTest.PRINCIPAL_ROLE_ALL;
import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CatalogRoles;
import org.apache.polaris.core.admin.model.Catalogs;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.CreateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.CreatePrincipalRequest;
import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantCatalogRoleRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.PrincipalRoles;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.Principals;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * @implSpec @implSpec This test expects the server to be configured with the following features
 *     configured:
 *     <ul>
 *       <li>{@link
 *           org.apache.polaris.core.config.FeatureConfiguration#ALLOW_OVERLAPPING_CATALOG_URLS}:
 *           {@code true}
 *       <li>{@link
 *           org.apache.polaris.core.config.FeatureConfiguration#ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING}:
 *           {@code true}
 *     </ul>
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisManagementServiceIntegrationTest {
  private static final int MAX_IDENTIFIER_LENGTH = 256;
  private static final String ISSUER_KEY = "polaris";
  private static final String CLAIM_KEY_ACTIVE = "active";
  private static final String CLAIM_KEY_CLIENT_ID = "client_id";
  private static final String CLAIM_KEY_PRINCIPAL_ID = "principalId";
  private static final String CLAIM_KEY_SCOPE = "scope";

  private static PolarisClient client;
  private static ManagementApi managementApi;
  private static CatalogApi catalogApi;
  private static ClientCredentials rootCredentials;

  @BeforeAll
  public static void setup(PolarisApiEndpoints endpoints, ClientCredentials credentials) {
    client = polarisClient(endpoints);
    managementApi = client.managementApi(credentials);
    catalogApi = client.catalogApi(credentials);
    rootCredentials = credentials;
  }

  @AfterAll
  public static void close() throws Exception {
    client.close();
  }

  @AfterEach
  public void tearDown() {
    client.cleanUp(rootCredentials);
  }

  @Test
  public void testCatalogSerializing() throws IOException {
    CatalogProperties props = new CatalogProperties("s3://my-old-bucket/path/to/data");
    props.put("prop1", "propval");
    PolarisCatalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("my_catalog")
            .setProperties(props)
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder()
                    .setRoleArn("arn:aws:iam::123456789012:role/my-role")
                    .setExternalId("externalId")
                    .setUserArn("userArn")
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                    .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
                    .build())
            .build();

    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(catalog);
    System.out.println(json);
    Catalog deserialized = mapper.readValue(json, Catalog.class);
    assertThat(deserialized).isInstanceOf(PolarisCatalog.class);
  }

  @Test
  public void testListCatalogs() {
    try (Response response = managementApi.request("v1/catalogs").get()) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(Catalogs.class))
          .returns(
              List.of(),
              l ->
                  l.getCatalogs().stream()
                      .filter(c -> !c.getName().equalsIgnoreCase("ROOT"))
                      .toList());
    }
  }

  @Test
  public void testListCatalogsUnauthorized() {
    PrincipalWithCredentials principal =
        managementApi.createPrincipal(client.newEntityName("a_new_user"));
    try (Response response = client.managementApi(principal).request("v1/catalogs").get()) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreateCatalog() {
    try (Response response =
        managementApi
            .request("v1/catalogs")
            .post(
                Entity.json(
                    "{\"catalog\":{\"type\":\"INTERNAL\",\"name\":\"my-catalog\",\"properties\":{\"default-base-location\":\"s3://my-bucket/path/to/data\"},\"storageConfigInfo\":{\"storageType\":\"S3\",\"roleArn\":\"arn:aws:iam::123456789012:role/my-role\",\"externalId\":\"externalId\",\"userArn\":\"userArn\",\"allowedLocations\":[\"s3://my-old-bucket/path/to/data\"]}}}"))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete
    try (Response response = managementApi.request("v1/catalogs/my-catalog").delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreateCatalogWithInvalidName() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build();

    String goodName = RandomStringUtils.random(MAX_IDENTIFIER_LENGTH, true, true);

    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(goodName)
            .setProperties(new CatalogProperties("s3://my-bucket/path/to/data"))
            .setStorageConfigInfo(awsConfigModel)
            .build();
    try (Response response = managementApi.request("v1/catalogs").post(Entity.json(catalog))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    String longInvalidName = RandomStringUtils.random(MAX_IDENTIFIER_LENGTH + 1, true, true);
    List<String> invalidCatalogNames =
        Arrays.asList(
            longInvalidName,
            "",
            "system$catalog1",
            "SYSTEM$TestCatalog",
            "System$test_catalog",
            "  SysTeM$ test catalog");

    for (String invalidCatalogName : invalidCatalogNames) {
      catalog =
          PolarisCatalog.builder()
              .setType(Catalog.TypeEnum.INTERNAL)
              .setName(invalidCatalogName)
              .setProperties(new CatalogProperties("s3://my-bucket/path/to/data"))
              .setStorageConfigInfo(awsConfigModel)
              .build();

      try (Response response = managementApi.request("v1/catalogs").post(Entity.json(catalog))) {
        assertThat(response)
            .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
        assertThat(response.hasEntity()).isTrue();
        ErrorResponse errorResponse = response.readEntity(ErrorResponse.class);
        assertThat(errorResponse.message()).contains("Invalid value:");
      }
    }
  }

  @Test
  public void testCreateCatalogWithAzureStorageConfig() {
    AzureStorageConfigInfo azureConfigInfo =
        AzureStorageConfigInfo.builder()
            .setConsentUrl("https://consent.url")
            .setMultiTenantAppName("myappname")
            .setTenantId("tenantId")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("my-catalog")
            .setProperties(
                new CatalogProperties(
                    "abfss://polaris@polarisdev.dfs.core.windows.net/path/to/my/data/"))
            .setStorageConfigInfo(azureConfigInfo)
            .build();
    try (Response response =
        managementApi.request("v1/catalogs").post(Entity.json(new CreateCatalogRequest(catalog)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    try (Response response = managementApi.request("v1/catalogs/my-catalog").get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog catResponse = response.readEntity(Catalog.class);
      assertThat(catResponse.getStorageConfigInfo())
          .isInstanceOf(AzureStorageConfigInfo.class)
          .hasFieldOrPropertyWithValue("consentUrl", "https://consent.url")
          .hasFieldOrPropertyWithValue("multiTenantAppName", "myappname")
          .hasFieldOrPropertyWithValue(
              "allowedLocations",
              List.of("abfss://polaris@polarisdev.dfs.core.windows.net/path/to/my/data/"));
    }
  }

  @Test
  public void testCreateCatalogWithGcpStorageConfig() {
    GcpStorageConfigInfo gcpConfigModel =
        GcpStorageConfigInfo.builder()
            .setGcsServiceAccount("my-sa")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(client.newEntityName("my-catalog"))
            .setProperties(new CatalogProperties("gs://my-bucket/path/to/data"))
            .setStorageConfigInfo(gcpConfigModel)
            .build();

    managementApi.createCatalog(catalog);

    try (Response response =
        managementApi.request("v1/catalogs/{cat}", Map.of("cat", catalog.getName())).get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog catResponse = response.readEntity(Catalog.class);
      assertThat(catResponse.getStorageConfigInfo())
          .isInstanceOf(GcpStorageConfigInfo.class)
          .hasFieldOrPropertyWithValue("gcsServiceAccount", "my-sa")
          .hasFieldOrPropertyWithValue("allowedLocations", List.of("gs://my-bucket/path/to/data"));
    }
  }

  @Test
  public void testCreateCatalogWithNullBaseLocation() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode storageConfig = mapper.valueToTree(awsConfigModel);
    ObjectNode catalogNode = mapper.createObjectNode();
    catalogNode.set("storageConfigInfo", storageConfig);
    catalogNode.put("name", "my-catalog");
    catalogNode.put("type", "INTERNAL");
    catalogNode.set("properties", mapper.createObjectNode());
    ObjectNode requestNode = mapper.createObjectNode();
    requestNode.set("catalog", catalogNode);
    try (Response response = managementApi.request("v1/catalogs").post(Entity.json(requestNode))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreateCatalogWithoutProperties() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode storageConfig = mapper.valueToTree(awsConfigModel);
    ObjectNode catalogNode = mapper.createObjectNode();
    catalogNode.set("storageConfigInfo", storageConfig);
    catalogNode.put("name", "my-catalog");
    catalogNode.put("type", "INTERNAL");
    ObjectNode requestNode = mapper.createObjectNode();
    requestNode.set("catalog", catalogNode);

    try (Response response = managementApi.request("v1/catalogs").post(Entity.json(requestNode))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertThat(error)
          .isNotNull()
          .returns(
              "Invalid value: createCatalog.arg0.catalog.properties: must not be null",
              ErrorResponse::message);
    }
  }

  @Test
  public void testCreateCatalogWithoutStorageConfig() {
    String catalogString =
        "{\"catalog\": {\"type\":\"INTERNAL\",\"name\":\"my-catalog\",\"properties\":{\"default-base-location\":\"s3://my-bucket/path/to/data\"}}}";
    try (Response response =
        managementApi.request("v1/catalogs").post(Entity.json(catalogString))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertThat(error)
          .isNotNull()
          .returns(
              "Invalid value: createCatalog.arg0.catalog.storageConfigInfo: must not be null",
              ErrorResponse::message);
    }
  }

  @Test
  public void testCreateCatalogWithUnparsableJson() {
    String catalogString = "{\"catalog\": {{\"bad data}";
    try (Response response =
        managementApi.request("v1/catalogs").post(Entity.json(catalogString))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertThat(error).isNotNull().extracting(ErrorResponse::message).isNotNull();
    }
  }

  @Test
  public void testUpdateCatalogWithoutDefaultBaseLocationInUpdate() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build();
    String catalogName =
        client.newEntityName("testUpdateCatalogWithoutDefaultBaseLocationInUpdate");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://bucket/path/to/data"))
            .setStorageConfigInfo(awsConfigModel)
            .build();
    try (Response response =
        managementApi.request("v1/catalogs").post(Entity.json(new CreateCatalogRequest(catalog)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    Catalog fetchedCatalog;
    try (Response response = managementApi.request("v1/catalogs/" + catalogName).get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getName()).isEqualTo(catalogName);
      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://bucket/path/to/data"));
      assertThat(fetchedCatalog.getEntityVersion()).isGreaterThan(0);
    }

    // Create an UpdateCatalogRequest that only sets a new property foo=bar but omits
    // default-base-location.
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(), Map.of("foo", "bar"), null /* storageConfigIno */);

    // Successfully update
    Catalog updatedCatalog;
    try (Response response =
        managementApi.request("v1/catalogs/" + catalogName).put(Entity.json(updateRequest))) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      updatedCatalog = response.readEntity(Catalog.class);

      assertThat(updatedCatalog.getName()).isEqualTo(catalogName);
      // Check that default-base-location is preserved in addition to adding the new property
      assertThat(updatedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://bucket/path/to/data", "foo", "bar"));
      assertThat(updatedCatalog.getEntityVersion()).isGreaterThan(0);
    }
  }

  @Test
  public void testCreateExternalCatalog() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build();
    String catalogName = client.newEntityName("my-external-catalog");
    Catalog catalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://my-bucket/path/to/data"))
            .setStorageConfigInfo(awsConfigModel)
            .build();
    managementApi.createCatalog(catalog);

    try (Response response = managementApi.request("v1/catalogs/" + catalogName).get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog fetchedCatalog = response.readEntity(Catalog.class);
      assertThat(fetchedCatalog)
          .isNotNull()
          .isInstanceOf(ExternalCatalog.class)
          .asInstanceOf(InstanceOfAssertFactories.type(ExternalCatalog.class))
          .extracting(ExternalCatalog::getStorageConfigInfo)
          .isNotNull()
          .isInstanceOf(AwsStorageConfigInfo.class)
          .asInstanceOf(InstanceOfAssertFactories.type(AwsStorageConfigInfo.class))
          .returns("arn:aws:iam::123456789012:role/my-role", AwsStorageConfigInfo::getRoleArn);
    }

    managementApi.deleteCatalog(catalogName);
  }

  @Test
  public void testCreateCatalogWithoutDefaultLocation() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode storageConfig = mapper.valueToTree(awsConfigModel);
    ObjectNode catalogNode = mapper.createObjectNode();
    catalogNode.set("storageConfigInfo", storageConfig);
    catalogNode.put("name", "my-catalog");
    catalogNode.put("type", "INTERNAL");
    ObjectNode properties = mapper.createObjectNode();
    properties.set("default-base-location", mapper.nullNode());
    catalogNode.set("properties", properties);
    ObjectNode requestNode = mapper.createObjectNode();
    requestNode.set("catalog", catalogNode);

    try (Response response = managementApi.request("v1/catalogs").post(Entity.json(requestNode))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void serialization() {
    CatalogProperties properties = new CatalogProperties("s3://my-bucket/path/to/data");
    ObjectMapper mapper = new ObjectMapper();
    CatalogProperties translated = mapper.convertValue(properties, CatalogProperties.class);
    assertThat(translated.toMap())
        .containsEntry("default-base-location", "s3://my-bucket/path/to/data");
  }

  @Test
  public void testCreateAndUpdateAzureCatalog() {
    StorageConfigInfo storageConfig =
        new AzureStorageConfigInfo("azure:tenantid:12345", StorageConfigInfo.StorageTypeEnum.AZURE);
    String catalogName = client.newEntityName("myazurecatalog");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(storageConfig)
            .setProperties(new CatalogProperties("abfss://container1@acct1.dfs.core.windows.net/"))
            .build();

    // 200 Successful create
    managementApi.createCatalog(catalog);

    // 200 successful GET after creation
    Catalog fetchedCatalog;
    try (Response response = managementApi.request("v1/catalogs/" + catalogName).get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getName()).isEqualTo(catalogName);
      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(
              Map.of("default-base-location", "abfss://container1@acct1.dfs.core.windows.net/"));
      assertThat(fetchedCatalog.getEntityVersion()).isGreaterThan(0);
    }

    StorageConfigInfo modifiedStorageConfig =
        new AzureStorageConfigInfo("azure:tenantid:22222", StorageConfigInfo.StorageTypeEnum.AZURE);
    UpdateCatalogRequest badUpdateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "abfss://newcontainer@acct1.dfs.core.windows.net/"),
            modifiedStorageConfig);
    try (Response response =
        managementApi.request("v1/catalogs/" + catalogName).put(Entity.json(badUpdateRequest))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertThat(error)
          .isNotNull()
          .extracting(ErrorResponse::message)
          .asString()
          .startsWith("Cannot modify");
    }

    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "abfss://newcontainer@acct1.dfs.core.windows.net/"),
            storageConfig);

    // 200 successful update
    try (Response response =
        managementApi.request("v1/catalogs/" + catalogName).put(Entity.json(updateRequest))) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(
              Map.of("default-base-location", "abfss://newcontainer@acct1.dfs.core.windows.net/"));
    }

    // 204 Successful delete
    try (Response response = managementApi.request("v1/catalogs/" + catalogName).delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreateListUpdateAndDeleteCatalog() {
    StorageConfigInfo storageConfig =
        new AwsStorageConfigInfo(
            "arn:aws:iam::123456789011:role/role1", StorageConfigInfo.StorageTypeEnum.S3);
    String catalogName = client.newEntityName("mycatalog");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(storageConfig)
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();

    managementApi.createCatalog(catalog);

    // Second attempt to create the same entity should fail with CONFLICT.
    try (Response response =
        managementApi.request("v1/catalogs").post(Entity.json(new CreateCatalogRequest(catalog)))) {
      assertThat(response).returns(Response.Status.CONFLICT.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    Catalog fetchedCatalog;
    try (Response response = managementApi.request("v1/catalogs/" + catalogName).get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getName()).isEqualTo(catalogName);
      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://bucket1/"));
      assertThat(fetchedCatalog.getEntityVersion()).isGreaterThan(0);
    }

    // Should list the catalog.
    try (Response response = managementApi.request("v1/catalogs").get()) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(Catalogs.class))
          .extracting(Catalogs::getCatalogs)
          .asInstanceOf(InstanceOfAssertFactories.list(Catalog.class))
          .filteredOn(cat -> !cat.getName().equalsIgnoreCase("ROOT"))
          .satisfiesExactly(cat -> assertThat(cat).returns(catalogName, Catalog::getName));
    }

    // Reject update of fields that can't be currently updated
    StorageConfigInfo invalidModifiedStorageConfig =
        new AwsStorageConfigInfo(
            "arn:aws:iam::123456789012:role/newrole", StorageConfigInfo.StorageTypeEnum.S3);
    UpdateCatalogRequest badUpdateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://newbucket/"),
            invalidModifiedStorageConfig);
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}", Map.of("cat", catalogName))
            .put(Entity.json(badUpdateRequest))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertThat(error)
          .isNotNull()
          .extracting(ErrorResponse::message)
          .asString()
          .startsWith("Cannot modify");
    }

    // Allow update of fields that are supported (e.g. new default-base-location and role ARN when
    // AWS
    // account IDs are same)
    StorageConfigInfo validModifiedStorageConfig =
        new AwsStorageConfigInfo(
            "arn:aws:iam::123456789011:role/newrole", StorageConfigInfo.StorageTypeEnum.S3);
    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://newbucket/"),
            validModifiedStorageConfig);

    // 200 successful update
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}", Map.of("cat", catalogName))
            .put(Entity.json(updateRequest))) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://newbucket/"));
      assertThat(fetchedCatalog.getStorageConfigInfo())
          .isInstanceOf(AwsStorageConfigInfo.class)
          .hasFieldOrPropertyWithValue("roleArn", "arn:aws:iam::123456789011:role/newrole");
    }

    // 200 GET after update should show new properties
    try (Response response =
        managementApi.request("v1/catalogs/{cat}", Map.of("cat", catalogName)).get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://newbucket/"));
    }

    // 204 Successful delete
    try (Response response =
        managementApi.request("v1/catalogs/{cat}", Map.of("cat", catalogName)).delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // NOT_FOUND after deletion
    try (Response response =
        managementApi.request("v1/catalogs/{cat}", Map.of("cat", catalogName)).get()) {
      assertThat(response).returns(Response.Status.NOT_FOUND.getStatusCode(), Response::getStatus);
    }

    // Empty list
    try (Response response = managementApi.request("v1/catalogs").get()) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(Catalogs.class))
          .returns(
              List.of(),
              c ->
                  c.getCatalogs().stream()
                      .filter(cat -> !cat.getName().equalsIgnoreCase("ROOT"))
                      .toList());
    }
  }

  @Test
  public void testGetCatalogNotFound() {
    // there's no catalog yet. Expect 404
    try (Response response = managementApi.request("v1/catalogs/mycatalog").get()) {
      assertThat(response).returns(Response.Status.NOT_FOUND.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testGetCatalogInvalidName() {
    String longInvalidName = RandomStringUtils.random(MAX_IDENTIFIER_LENGTH + 1, true, true);
    List<String> invalidCatalogNames =
        Arrays.asList(
            longInvalidName,
            "system$catalog1",
            "SYSTEM$TestCatalog",
            "System$test_catalog",
            "  SysTeM$ test catalog");

    for (String invalidCatalogName : invalidCatalogNames) {
      // there's no catalog yet. Expect 404
      try (Response response = managementApi.request("v1/catalogs/" + invalidCatalogName).get()) {
        assertThat(response)
            .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
        assertThat(response.hasEntity()).isTrue();
        ErrorResponse errorResponse = response.readEntity(ErrorResponse.class);
        assertThat(errorResponse.message()).contains("Invalid value:");
      }
    }
  }

  @Test
  public void testCatalogRoleInvalidName() {
    String catalogName = client.newEntityName("mycatalog1");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://required/base/location"))
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .build();
    managementApi.createCatalog(catalog);

    String longInvalidName = RandomStringUtils.random(MAX_IDENTIFIER_LENGTH + 1, true, true);
    List<String> invalidCatalogRoleNames =
        Arrays.asList(
            longInvalidName,
            "system$catalog1",
            "SYSTEM$TestCatalog",
            "System$test_catalog",
            "  SysTeM$ test catalog");

    for (String invalidCatalogRoleName : invalidCatalogRoleNames) {
      try (Response response =
          managementApi
              .request("v1/catalogs/mycatalog1/catalog-roles/" + invalidCatalogRoleName)
              .get()) {

        assertThat(response)
            .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
        assertThat(response.hasEntity()).isTrue();
        ErrorResponse errorResponse = response.readEntity(ErrorResponse.class);
        assertThat(errorResponse.message()).contains("Invalid value:");
      }
    }
  }

  @Test
  public void testListPrincipalsUnauthorized() {
    PrincipalWithCredentials principal =
        managementApi.createPrincipal(client.newEntityName("new_admin"));
    try (Response response = client.managementApi(principal).request("v1/principals").get()) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreatePrincipalAndRotateCredentials() {
    Principal principal =
        Principal.builder()
            .setName(client.newEntityName("myprincipal"))
            .setProperties(Map.of("custom-tag", "foo"))
            .build();

    PrincipalWithCredentials creds =
        managementApi.createPrincipal(new CreatePrincipalRequest(principal, true));
    assertThat(creds.getCredentials().getClientId()).isEqualTo(creds.getPrincipal().getClientId());

    // Now rotate the credentials. First, if we try to just use the adminToken to rotate the
    // newly created principal's credentials, we should fail; rotateCredentials is only
    // a "self" privilege that even admins can't inherit.
    try (Response response =
        managementApi
            .request("v1/principals/{p}/rotate", Map.of("p", principal.getName()))
            .post(Entity.json(""))) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }

    String oldUserToken = client.obtainToken(creds);

    // Any call should initially fail with error indicating that rotation is needed.
    try (Response response =
        client
            .managementApi(oldUserToken)
            .request("v1/principals/{p}", Map.of("p", principal.getName()))
            .get()) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertThat(error)
          .isNotNull()
          .extracting(ErrorResponse::message)
          .asString()
          .contains("PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE");
    }

    // Now try to rotate using the principal's token.
    PrincipalWithCredentials newCreds;
    try (Response response =
        client
            .managementApi(oldUserToken)
            .request("v1/principals/{p}/rotate", Map.of("p", principal.getName()))
            .post(Entity.json(""))) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      newCreds = response.readEntity(PrincipalWithCredentials.class);
    }
    assertThat(newCreds.getCredentials().getClientId())
        .isEqualTo(newCreds.getPrincipal().getClientId());

    // ClientId shouldn't change
    assertThat(newCreds.getCredentials().getClientId())
        .isEqualTo(creds.getCredentials().getClientId());
    assertThat(newCreds.getCredentials().getClientSecret())
        .isNotEqualTo(creds.getCredentials().getClientSecret());

    // TODO: Test the validity of the old secret for getting tokens, here and then after a second
    // rotation that makes the old secret fall off retention.
  }

  @Test
  public void testCreateFederatedPrincipalRoleSucceeds() {
    // Create a federated Principal Role
    PrincipalRole federatedPrincipalRole =
        new PrincipalRole(
            client.newEntityName("federatedRole"),
            true,
            Map.of(),
            Instant.now().toEpochMilli(),
            Instant.now().toEpochMilli(),
            1);

    // Attempt to create the federated Principal using the managementApi
    try (Response createResponse =
        managementApi
            .request("v1/principal-roles")
            .post(Entity.json(new CreatePrincipalRoleRequest(federatedPrincipalRole)))) {
      assertThat(createResponse).returns(CREATED.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreateListUpdateAndDeletePrincipal() {
    Principal principal =
        Principal.builder()
            .setName(client.newEntityName("myprincipal"))
            .setProperties(Map.of("custom-tag", "foo"))
            .build();
    managementApi.createPrincipal(new CreatePrincipalRequest(principal, null));

    // Second attempt to create the same entity should fail with CONFLICT.
    try (Response response =
        managementApi
            .request("v1/principals")
            .post(Entity.json(new CreatePrincipalRequest(principal, false)))) {
      assertThat(response).returns(Response.Status.CONFLICT.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    Principal fetchedPrincipal;
    try (Response response =
        managementApi.request("v1/principals/{p}", Map.of("p", principal.getName())).get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedPrincipal = response.readEntity(Principal.class);

      assertThat(fetchedPrincipal.getName()).isEqualTo(principal.getName());
      assertThat(fetchedPrincipal.getProperties()).isEqualTo(Map.of("custom-tag", "foo"));
      assertThat(fetchedPrincipal.getEntityVersion()).isGreaterThan(0);
    }

    // Should list the principal.
    try (Response response = managementApi.request("v1/principals").get()) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(Principals.class))
          .extracting(Principals::getPrincipals)
          .asInstanceOf(InstanceOfAssertFactories.list(Principal.class))
          .anySatisfy(pr -> assertThat(pr).returns(principal.getName(), Principal::getName));
    }

    UpdatePrincipalRequest updateRequest =
        new UpdatePrincipalRequest(
            fetchedPrincipal.getEntityVersion(), Map.of("custom-tag", "updated"));

    // 200 successful update
    try (Response response =
        managementApi
            .request("v1/principals/{p}", Map.of("p", principal.getName()))
            .put(Entity.json(updateRequest))) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedPrincipal = response.readEntity(Principal.class);

      assertThat(fetchedPrincipal.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 200 GET after update should show new properties
    try (Response response =
        managementApi.request("v1/principals/{p}", Map.of("p", principal.getName())).get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedPrincipal = response.readEntity(Principal.class);

      assertThat(fetchedPrincipal.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    managementApi.deletePrincipal(principal);

    // NOT_FOUND after deletion
    try (Response response =
        managementApi
            .request("v1/principals/{prince}", Map.of("prince", principal.getName()))
            .get()) {
      assertThat(response).returns(Response.Status.NOT_FOUND.getStatusCode(), Response::getStatus);
    }

    // Empty list
    try (Response response = managementApi.request("v1/principals").get()) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(Principals.class))
          .extracting(Principals::getPrincipals)
          .asInstanceOf(InstanceOfAssertFactories.list(Principal.class))
          .noneSatisfy(pr -> assertThat(pr).returns(principal.getName(), Principal::getName));
    }
  }

  @Test
  public void testCreatePrincipalWithInvalidName() {
    String goodName = RandomStringUtils.random(MAX_IDENTIFIER_LENGTH, true, true);
    Principal principal =
        Principal.builder()
            .setName(goodName)
            .setProperties(Map.of("custom-tag", "good_principal"))
            .build();
    managementApi.createPrincipal(new CreatePrincipalRequest(principal, null));

    String longInvalidName = RandomStringUtils.random(MAX_IDENTIFIER_LENGTH + 1, true, true);
    List<String> invalidPrincipalNames =
        Arrays.asList(
            longInvalidName,
            "",
            "system$principal1",
            "SYSTEM$TestPrincipal",
            "System$test_principal",
            "  SysTeM$ principal");

    for (String invalidPrincipalName : invalidPrincipalNames) {
      principal =
          Principal.builder()
              .setName(invalidPrincipalName)
              .setProperties(Map.of("custom-tag", "bad_principal"))
              .build();

      try (Response response =
          managementApi
              .request("v1/principals")
              .post(Entity.json(new CreatePrincipalRequest(principal, false)))) {
        assertThat(response)
            .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
        assertThat(response.hasEntity()).isTrue();
        ErrorResponse errorResponse = response.readEntity(ErrorResponse.class);
        assertThat(errorResponse.message()).contains("Invalid value:");
      }
    }
  }

  @Test
  public void testGetPrincipalWithInvalidName() {
    String longInvalidName = RandomStringUtils.random(MAX_IDENTIFIER_LENGTH + 1, true, true);
    List<String> invalidPrincipalNames =
        Arrays.asList(
            longInvalidName,
            "system$principal1",
            "SYSTEM$TestPrincipal",
            "System$test_principal",
            "  SysTeM$ principal");

    for (String invalidPrincipalName : invalidPrincipalNames) {
      try (Response response =
          managementApi.request("v1/principals/" + invalidPrincipalName).get()) {
        assertThat(response)
            .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
        assertThat(response.hasEntity()).isTrue();
        ErrorResponse errorResponse = response.readEntity(ErrorResponse.class);
        assertThat(errorResponse.message()).contains("Invalid value:");
      }
    }
  }

  @Test
  public void testCreateListUpdateAndDeletePrincipalRole() {
    PrincipalRole principalRole =
        new PrincipalRole(
            client.newEntityName("myprincipalrole"), false, Map.of("custom-tag", "foo"), 0L, 0L, 1);
    managementApi.createPrincipalRole(principalRole);

    // Second attempt to create the same entity should fail with CONFLICT.
    try (Response response =
        managementApi
            .request("v1/principal-roles")
            .post(Entity.json(new CreatePrincipalRoleRequest(principalRole)))) {

      assertThat(response).returns(Response.Status.CONFLICT.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    PrincipalRole fetchedPrincipalRole;
    try (Response response =
        managementApi
            .request("v1/principal-roles/{pr}", Map.of("pr", principalRole.getName()))
            .get()) {

      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedPrincipalRole = response.readEntity(PrincipalRole.class);

      assertThat(fetchedPrincipalRole.getName()).isEqualTo(principalRole.getName());
      assertThat(fetchedPrincipalRole.getProperties()).isEqualTo(Map.of("custom-tag", "foo"));
      assertThat(fetchedPrincipalRole.getEntityVersion()).isGreaterThan(0);
    }

    // Should list the principalRole.
    try (Response response = managementApi.request("v1/principal-roles").get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .extracting(PrincipalRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(PrincipalRole.class))
          .anySatisfy(
              pr -> assertThat(pr).returns(principalRole.getName(), PrincipalRole::getName));
    }

    UpdatePrincipalRoleRequest updateRequest =
        new UpdatePrincipalRoleRequest(
            fetchedPrincipalRole.getEntityVersion(), Map.of("custom-tag", "updated"));

    // 200 successful update
    try (Response response =
        managementApi
            .request("v1/principal-roles/{pr}", Map.of("pr", principalRole.getName()))
            .put(Entity.json(updateRequest))) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedPrincipalRole = response.readEntity(PrincipalRole.class);

      assertThat(fetchedPrincipalRole.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 200 GET after update should show new properties
    try (Response response =
        managementApi
            .request("v1/principal-roles/{pr}", Map.of("pr", principalRole.getName()))
            .get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedPrincipalRole = response.readEntity(PrincipalRole.class);

      assertThat(fetchedPrincipalRole.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    managementApi.deletePrincipalRole(principalRole);

    // NOT_FOUND after deletion
    try (Response response =
        managementApi
            .request("v1/principal-roles/{pr}", Map.of("pr", principalRole.getName()))
            .get()) {
      assertThat(response).returns(Response.Status.NOT_FOUND.getStatusCode(), Response::getStatus);
    }

    // Empty list
    try (Response response = managementApi.request("v1/principal-roles").get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .extracting(PrincipalRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(PrincipalRole.class))
          .noneSatisfy(
              pr -> assertThat(pr).returns(principalRole.getName(), PrincipalRole::getName));
    }
  }

  @Test
  public void testCreatePrincipalRoleInvalidName() {
    String goodName = RandomStringUtils.random(MAX_IDENTIFIER_LENGTH, true, true);
    PrincipalRole principalRole =
        new PrincipalRole(goodName, false, Map.of("custom-tag", "good_principal_role"), 0L, 0L, 1);
    managementApi.createPrincipalRole(principalRole);

    String longInvalidName = RandomStringUtils.random(MAX_IDENTIFIER_LENGTH + 1, true, true);
    List<String> invalidPrincipalRoleNames =
        Arrays.asList(
            longInvalidName,
            "",
            "system$principalrole1",
            "SYSTEM$TestPrincipalRole",
            "System$test_principal_role",
            "  SysTeM$ principal role");

    for (String invalidPrincipalRoleName : invalidPrincipalRoleNames) {
      principalRole =
          new PrincipalRole(
              invalidPrincipalRoleName,
              false,
              Map.of("custom-tag", "bad_principal_role"),
              0L,
              0L,
              1);

      try (Response response =
          managementApi
              .request("v1/principal-roles")
              .post(Entity.json(new CreatePrincipalRoleRequest(principalRole)))) {
        assertThat(response)
            .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
        assertThat(response.hasEntity()).isTrue();
        ErrorResponse errorResponse = response.readEntity(ErrorResponse.class);
        assertThat(errorResponse.message()).contains("Invalid value:");
      }
    }
  }

  @Test
  public void testGetPrincipalRoleInvalidName() {
    String longInvalidName = RandomStringUtils.random(MAX_IDENTIFIER_LENGTH + 1, true, true);
    List<String> invalidPrincipalRoleNames =
        Arrays.asList(
            longInvalidName,
            "system$principalrole1",
            "SYSTEM$TestPrincipalRole",
            "System$test_principal_role",
            "  SysTeM$ principal role");

    for (String invalidPrincipalRoleName : invalidPrincipalRoleNames) {
      try (Response response =
          managementApi.request("v1/principal-roles/" + invalidPrincipalRoleName).get()) {
        assertThat(response)
            .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
        assertThat(response.hasEntity()).isTrue();
        ErrorResponse errorResponse = response.readEntity(ErrorResponse.class);
        assertThat(errorResponse.message()).contains("Invalid value:");
      }
    }
  }

  @Test
  public void testCreateListUpdateAndDeleteCatalogRole() {
    String catalogName = client.newEntityName("mycatalog1");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://required/base/location"))
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .build();
    managementApi.createCatalog(catalog);

    String catalogName2 = client.newEntityName("mycatalog2");
    Catalog catalog2 =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName2)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://required/base/other_location"))
            .build();
    managementApi.createCatalog(catalog2);

    CatalogRole catalogRole =
        new CatalogRole("mycatalogrole", Map.of("custom-tag", "foo"), 0L, 0L, 1);
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles", Map.of("cat", catalogName))
            .post(Entity.json(new CreateCatalogRoleRequest(catalogRole)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Second attempt to create the same entity should fail with CONFLICT.
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles", Map.of("cat", catalogName))
            .post(Entity.json(new CreateCatalogRoleRequest(catalogRole)))) {

      assertThat(response).returns(Response.Status.CONFLICT.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    CatalogRole fetchedCatalogRole;
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles/mycatalogrole", Map.of("cat", catalogName))
            .get()) {

      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalogRole = response.readEntity(CatalogRole.class);

      assertThat(fetchedCatalogRole.getName()).isEqualTo("mycatalogrole");
      assertThat(fetchedCatalogRole.getProperties()).isEqualTo(Map.of("custom-tag", "foo"));
      assertThat(fetchedCatalogRole.getEntityVersion()).isGreaterThan(0);
    }

    // Should list the catalogRole.
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles", Map.of("cat", catalogName))
            .get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .extracting(CatalogRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(CatalogRole.class))
          .anySatisfy(cr -> assertThat(cr).returns("mycatalogrole", CatalogRole::getName));
    }

    // Empty list if listing in catalog2
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles", Map.of("cat", catalogName2))
            .get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .extracting(CatalogRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(CatalogRole.class))
          .satisfiesExactly(
              cr ->
                  assertThat(cr)
                      .returns(
                          PolarisEntityConstants.getNameOfCatalogAdminRole(),
                          CatalogRole::getName));
    }

    UpdateCatalogRoleRequest updateRequest =
        new UpdateCatalogRoleRequest(
            fetchedCatalogRole.getEntityVersion(), Map.of("custom-tag", "updated"));

    // 200 successful update
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles/mycatalogrole", Map.of("cat", catalogName))
            .put(Entity.json(updateRequest))) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalogRole = response.readEntity(CatalogRole.class);

      assertThat(fetchedCatalogRole.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 200 GET after update should show new properties
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles/mycatalogrole", Map.of("cat", catalogName))
            .get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalogRole = response.readEntity(CatalogRole.class);

      assertThat(fetchedCatalogRole.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 204 Successful delete
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles/mycatalogrole", Map.of("cat", catalogName))
            .delete()) {

      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // NOT_FOUND after deletion
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles/mycatalogrole", Map.of("cat", catalogName))
            .get()) {

      assertThat(response).returns(Response.Status.NOT_FOUND.getStatusCode(), Response::getStatus);
    }

    // Empty list
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles", Map.of("cat", catalogName))
            .get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .extracting(CatalogRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(CatalogRole.class))
          .noneSatisfy(cr -> assertThat(cr).returns("mycatalogrole", CatalogRole::getName));
    }

    // 204 Successful delete mycatalog
    try (Response response =
        managementApi.request("v1/catalogs/{cat}", Map.of("cat", catalogName)).delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete mycatalog2
    try (Response response =
        managementApi.request("v1/catalogs/{cat}", Map.of("cat", catalogName2)).delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testAssignListAndRevokePrincipalRoles() {
    // Create two Principals
    Principal principal1 = new Principal(client.newEntityName("myprincipal1"));
    try (Response response =
        managementApi
            .request("v1/principals")
            .post(Entity.json(new CreatePrincipalRequest(principal1, false)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    Principal principal2 = new Principal(client.newEntityName("myprincipal2"));
    try (Response response =
        managementApi
            .request("v1/principals")
            .post(Entity.json(new CreatePrincipalRequest(principal2, false)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // One PrincipalRole
    PrincipalRole principalRole = new PrincipalRole(client.newEntityName("myprincipalrole"));
    managementApi.createPrincipalRole(principalRole);

    // Assign the role to myprincipal1
    try (Response response =
        managementApi
            .request(
                "v1/principals/{prince}/principal-roles", Map.of("prince", principal1.getName()))
            .put(Entity.json(principalRole))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Should list myprincipalrole
    try (Response response =
        managementApi
            .request(
                "v1/principals/{prince}/principal-roles", Map.of("prince", principal1.getName()))
            .get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .extracting(PrincipalRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(PrincipalRole.class))
          .hasSize(1)
          .satisfiesExactly(
              pr -> assertThat(pr).returns(principalRole.getName(), PrincipalRole::getName));
    }

    // Should list myprincipal1 if listing assignees of myprincipalrole
    try (Response response =
        managementApi
            .request("v1/principal-roles/{pr}/principals", Map.of("pr", principalRole.getName()))
            .get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(Principals.class))
          .extracting(Principals::getPrincipals)
          .asInstanceOf(InstanceOfAssertFactories.list(Principal.class))
          .hasSize(1)
          .satisfiesExactly(pr -> assertThat(pr).returns(principal1.getName(), Principal::getName));
    }

    // Empty list if listing in principal2
    try (Response response =
        managementApi
            .request(
                "v1/principals/{prince}/principal-roles", Map.of("prince", principal2.getName()))
            .get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .returns(List.of(), PrincipalRoles::getRoles);
    }

    // 204 Successful revoke
    try (Response response =
        managementApi
            .request(
                "v1/principals/{prince}/principal-roles/{pr}",
                Map.of("prince", principal1.getName(), "pr", principalRole.getName()))
            .delete()) {

      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // Empty list
    try (Response response =
        managementApi
            .request(
                "v1/principals/{prince}/principal-roles", Map.of("prince", principal1.getName()))
            .get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .returns(List.of(), PrincipalRoles::getRoles);
    }
    try (Response response =
        managementApi
            .request("v1/principal-roles/{pr}/principals", Map.of("pr", principalRole.getName()))
            .get()) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(Principals.class))
          .returns(List.of(), Principals::getPrincipals);
    }

    // 204 Successful delete myprincipal1
    try (Response response =
        managementApi
            .request("v1/principals/{prince}", Map.of("prince", principal1.getName()))
            .delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete myprincipal2
    try (Response response =
        managementApi
            .request("v1/principals/{prince}", Map.of("prince", principal2.getName()))
            .delete()) {

      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete myprincipalrole
    try (Response response =
        managementApi
            .request("v1/principal-roles/{pr}", Map.of("pr", principalRole.getName()))
            .delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testAssignListAndRevokeCatalogRoles() {
    // Create two PrincipalRoles
    PrincipalRole principalRole1 = new PrincipalRole(client.newEntityName("mypr1"));
    managementApi.createPrincipalRole(principalRole1);

    PrincipalRole principalRole2 = new PrincipalRole(client.newEntityName("mypr2"));
    managementApi.createPrincipalRole(principalRole2);

    // One CatalogRole
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(client.newEntityName("mycatalog"))
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    managementApi.createCatalog(catalog);

    CatalogRole catalogRole = new CatalogRole("mycr");
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles", Map.of("cat", catalog.getName()))
            .post(Entity.json(new CreateCatalogRoleRequest(catalogRole)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Create another one in a different catalog.
    Catalog otherCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(client.newEntityName("othercatalog"))
            .setProperties(new CatalogProperties("s3://path/to/data"))
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .build();
    managementApi.createCatalog(otherCatalog);

    CatalogRole otherCatalogRole = new CatalogRole("myothercr");
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles", Map.of("cat", otherCatalog.getName()))
            .post(Entity.json(new CreateCatalogRoleRequest(otherCatalogRole)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Assign both the roles to mypr1
    try (Response response =
        managementApi
            .request(
                "v1/principal-roles/{pr}/catalog-roles/{cat}",
                Map.of("pr", principalRole1.getName(), "cat", catalog.getName()))
            .put(Entity.json(new GrantCatalogRoleRequest(catalogRole)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    try (Response response =
        managementApi
            .request(
                "v1/principal-roles/{pr}/catalog-roles/{cat}",
                Map.of("pr", principalRole1.getName(), "cat", otherCatalog.getName()))
            .put(Entity.json(new GrantCatalogRoleRequest(otherCatalogRole)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Should list only mycr
    try (Response response =
        managementApi
            .request(
                "v1/principal-roles/{pr}/catalog-roles/{cat}",
                Map.of("pr", principalRole1.getName(), "cat", catalog.getName()))
            .get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .extracting(CatalogRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(CatalogRole.class))
          .hasSize(1)
          .satisfiesExactly(cr -> assertThat(cr).returns("mycr", CatalogRole::getName));
    }

    // Should list mypr1 if listing assignees of mycr
    try (Response response =
        managementApi
            .request(
                "v1/catalogs/{cat}/catalog-roles/mycr/principal-roles",
                Map.of("cat", catalog.getName()))
            .get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .extracting(PrincipalRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(PrincipalRole.class))
          .hasSize(1)
          .satisfiesExactly(
              pr -> assertThat(pr).returns(principalRole1.getName(), PrincipalRole::getName));
    }

    // Empty list if listing in principalRole2
    try (Response response =
        managementApi
            .request(
                "v1/principal-roles/{pr}/catalog-roles/{cat}",
                Map.of("pr", principalRole2.getName(), "cat", catalog.getName()))
            .get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .returns(List.of(), CatalogRoles::getRoles);
    }

    // 204 Successful revoke
    try (Response response =
        managementApi
            .request(
                "v1/principal-roles/{pr}/catalog-roles/{cat}/mycr",
                Map.of("pr", principalRole1.getName(), "cat", catalog.getName()))
            .delete()) {

      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // Empty list
    try (Response response =
        managementApi
            .request(
                "v1/principal-roles/{pr}/catalog-roles/{cat}",
                Map.of("pr", principalRole1.getName(), "cat", catalog.getName()))
            .get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .returns(List.of(), CatalogRoles::getRoles);
    }
    try (Response response =
        managementApi
            .request(
                "v1/catalogs/{cat}/catalog-roles/mycr/principal-roles",
                Map.of("cat", catalog.getName()))
            .get()) {

      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .returns(List.of(), PrincipalRoles::getRoles);
    }

    managementApi.deletePrincipalRole(principalRole1);
    managementApi.deletePrincipalRole(principalRole2);

    managementApi.deleteCatalogRole(catalog.getName(), "mycr");
    managementApi.deleteCatalog(catalog.getName());

    managementApi.deleteCatalogRole(otherCatalog.getName(), otherCatalogRole.getName());
    managementApi.deleteCatalog(otherCatalog.getName());
  }

  @Test
  public void testCatalogAdminGrantAndRevokeCatalogRoles() {
    // Create a PrincipalRole and a new catalog. Grant the catalog_admin role to the new principal
    // role
    String principalRoleName = client.newEntityName("mypr33");
    managementApi.createPrincipalRole(principalRoleName);

    String catalogName = client.newEntityName("myuniquetestcatalog");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    managementApi.createCatalog(catalog);

    CatalogRole catalogAdminRole = managementApi.getCatalogRole(catalogName, "catalog_admin");
    managementApi.grantCatalogRoleToPrincipalRole(principalRoleName, catalogName, catalogAdminRole);

    PrincipalWithCredentials catalogAdminPrincipal =
        managementApi.createPrincipal(client.newEntityName("principal1"));

    managementApi.assignPrincipalRole(
        catalogAdminPrincipal.getPrincipal().getName(), principalRoleName);

    String catalogAdminToken = client.obtainToken(catalogAdminPrincipal);

    // Create a second principal role. Use the catalog admin principal to list principal roles and
    // grant a catalog role to the new principal role
    String principalRoleName2 = "mypr2";
    PrincipalRole principalRole2 = new PrincipalRole(principalRoleName2);
    managementApi.createPrincipalRole(principalRole2);

    // create a catalog role and grant it manage_content privilege
    String catalogRoleName = "mycr1";
    client.managementApi(catalogAdminToken).createCatalogRole(catalogName, catalogRoleName);

    CatalogPrivilege privilege = CatalogPrivilege.CATALOG_MANAGE_CONTENT;
    client
        .managementApi(catalogAdminToken)
        .addGrant(
            catalogName,
            catalogRoleName,
            new CatalogGrant(privilege, GrantResource.TypeEnum.CATALOG));

    // The catalog admin can grant the new catalog role to the mypr2 principal role
    client
        .managementApi(catalogAdminToken)
        .grantCatalogRoleToPrincipalRole(
            principalRoleName2, catalogName, new CatalogRole(catalogRoleName));

    // But the catalog admin cannot revoke the role because it requires
    // PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE
    try (Response response =
        client
            .managementApi(catalogAdminToken)
            .request(
                "v1/principal-roles/"
                    + principalRoleName
                    + "/catalog-roles/"
                    + catalogName
                    + "/"
                    + catalogRoleName)
            .delete()) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }

    // The service admin can revoke the role because it has the
    // PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE privilege
    try (Response response =
        managementApi
            .request(
                "v1/principal-roles/"
                    + principalRoleName
                    + "/catalog-roles/"
                    + catalogName
                    + "/"
                    + catalogRoleName)
            .delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testServiceAdminCanTransferCatalogAdmin() {
    // Create a PrincipalRole and a new catalog. Grant the catalog_admin role to the new principal
    // role
    String principalRoleName = client.newEntityName("mypr33");
    PrincipalRole principalRole1 = new PrincipalRole(principalRoleName);
    managementApi.createPrincipalRole(principalRole1);

    String catalogName = client.newEntityName("myothertestcatalog");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    managementApi.createCatalog(catalog);

    CatalogRole catalogAdminRole = managementApi.getCatalogRole(catalogName, "catalog_admin");
    managementApi.grantCatalogRoleToPrincipalRole(principalRoleName, catalogName, catalogAdminRole);

    PrincipalWithCredentials catalogAdminPrincipal =
        managementApi.createPrincipal(client.newEntityName("principal1"));

    managementApi.assignPrincipalRole(
        catalogAdminPrincipal.getPrincipal().getName(), principalRole1.getName());

    String catalogAdminToken = client.obtainToken(catalogAdminPrincipal);

    // service_admin revokes the catalog_admin privilege from its principal role
    try {
      try (Response response =
          managementApi
              .request(
                  "v1/principal-roles/service_admin/catalog-roles/"
                      + catalogName
                      + "/catalog_admin")
              .delete()) {
        assertThat(response)
            .returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
      }

      // the service_admin can not revoke the catalog_admin privilege from the new principal role
      try (Response response =
          client
              .managementApi(catalogAdminToken)
              .request(
                  "v1/principal-roles/"
                      + principalRoleName
                      + "/catalog-roles/"
                      + catalogName
                      + "/catalog_admin")
              .delete()) {
        assertThat(response)
            .returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
      }
    } finally {
      // grant the admin role back to service_admin so that cleanup can happen
      client
          .managementApi(catalogAdminToken)
          .grantCatalogRoleToPrincipalRole("service_admin", catalogName, catalogAdminRole);
    }
  }

  @Test
  public void testCatalogAdminGrantAndRevokeCatalogRolesFromWrongCatalog() {
    // Create a PrincipalRole and a new catalog. Grant the catalog_admin role to the new principal
    // role
    String principalRoleName = client.newEntityName("mypr33");
    PrincipalRole principalRole1 = new PrincipalRole(principalRoleName);
    managementApi.createPrincipalRole(principalRole1);

    // create a catalog
    String catalogName = client.newEntityName("mytestcatalog");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    managementApi.createCatalog(catalog);

    // create a second catalog
    String catalogName2 = client.newEntityName("anothercatalog");
    Catalog catalog2 =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName2)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    managementApi.createCatalog(catalog2);

    // create a catalog role *in the second catalog* and grant it manage_content privilege
    String catalogRoleName = "mycr1";
    managementApi.createCatalogRole(catalogName2, catalogRoleName);

    // Get the catalog admin role from the *first* catalog and grant that role to the principal role
    CatalogRole catalogAdminRole = managementApi.getCatalogRole(catalogName, "catalog_admin");
    managementApi.grantCatalogRoleToPrincipalRole(principalRoleName, catalogName, catalogAdminRole);

    // Create a principal and grant the principal role to it
    PrincipalWithCredentials catalogAdminPrincipal =
        managementApi.createPrincipal(client.newEntityName("principal1"));
    managementApi.assignPrincipalRole(
        catalogAdminPrincipal.getPrincipal().getName(), principalRole1.getName());

    String catalogAdminToken = client.obtainToken(catalogAdminPrincipal);

    // Create a second principal role.
    String principalRoleName2 = client.newEntityName("mypr2");
    PrincipalRole principalRole2 = new PrincipalRole(principalRoleName2);
    managementApi.createPrincipalRole(principalRole2);

    // The catalog admin cannot grant the new catalog role to the mypr2 principal role because the
    // catalog role is in the wrong catalog
    try (Response response =
        client
            .managementApi(catalogAdminToken)
            .request("v1/principal-roles/" + principalRoleName + "/catalog-roles/" + catalogName2)
            .put(Entity.json(new GrantCatalogRoleRequest(new CatalogRole(catalogRoleName))))) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testTableManageAccessCanGrantAndRevokeFromCatalogRoles() {
    // Create a PrincipalRole and a new catalog.
    String principalRoleName = client.newEntityName("mypr33");
    PrincipalRole principalRole1 = new PrincipalRole(principalRoleName);
    managementApi.createPrincipalRole(principalRole1);

    // create a catalog
    String catalogName = client.newEntityName("mytablemanagecatalog");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    managementApi.createCatalog(catalog);

    // create a valid target CatalogRole in this catalog
    managementApi.createCatalogRole(catalogName, "target_catalog_role");

    // create a second catalog
    String catalogName2 = client.newEntityName("anothertablemanagecatalog");
    Catalog catalog2 =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName2)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    managementApi.createCatalog(catalog2);

    // create an *invalid* target CatalogRole in second catalog
    managementApi.createCatalogRole(catalogName2, "invalid_target_catalog_role");

    // create the namespace "c" in *both* namespaces
    String namespaceName = "c";
    catalogApi.createNamespace(catalogName, namespaceName);
    catalogApi.createNamespace(catalogName2, namespaceName);

    // create a catalog role *in the first catalog* and grant it manage_content privilege at the
    // namespace level
    // grant that role to the PrincipalRole
    String catalogRoleName = "ns_manage_access_role";
    managementApi.createCatalogRole(catalogName, catalogRoleName);
    managementApi.addGrant(
        catalogName,
        catalogRoleName,
        new NamespaceGrant(
            List.of(namespaceName),
            NamespacePrivilege.CATALOG_MANAGE_ACCESS,
            GrantResource.TypeEnum.NAMESPACE));

    managementApi.grantCatalogRoleToPrincipalRole(
        principalRoleName, catalogName, new CatalogRole(catalogRoleName));

    // Create a principal and grant the principal role to it
    PrincipalWithCredentials catalogAdminPrincipal =
        managementApi.createPrincipal(client.newEntityName("ns_manage_access_user"));
    managementApi.assignPrincipalRole(
        catalogAdminPrincipal.getPrincipal().getName(), principalRole1.getName());

    String manageAccessUserToken = client.obtainToken(catalogAdminPrincipal);

    // Use the ns_manage_access_user to grant TABLE_CREATE access to the target catalog role
    // This works because the user has CATALOG_MANAGE_ACCESS within the namespace and the target
    // catalog role is in
    // the same catalog
    client
        .managementApi(manageAccessUserToken)
        .addGrant(
            catalogName,
            "target_catalog_role",
            new NamespaceGrant(
                List.of(namespaceName),
                NamespacePrivilege.TABLE_CREATE,
                GrantResource.TypeEnum.NAMESPACE));

    // Even though the ns_manage_access_role can grant privileges to the catalog role, it cannot
    // grant the target
    // catalog role to the mypr2 principal role because it doesn't have privilege to manage grants
    // on the catalog role
    // as a securable
    try (Response response =
        client
            .managementApi(manageAccessUserToken)
            .request("v1/principal-roles/" + principalRoleName + "/catalog-roles/" + catalogName)
            .put(
                Entity.json(new GrantCatalogRoleRequest(new CatalogRole("target_catalog_role"))))) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }

    // The user cannot grant catalog-level privileges to the catalog role
    try (Response response =
        client
            .managementApi(manageAccessUserToken)
            .request(
                "v1/catalogs/{cat}/catalog-roles/{role}/grants",
                Map.of("cat", catalogName, "role", "target_catalog_role"))
            .put(
                Entity.json(
                    new CatalogGrant(
                        CatalogPrivilege.TABLE_CREATE, GrantResource.TypeEnum.CATALOG)))) {
      assertThat(response).returns(FORBIDDEN.getStatusCode(), Response::getStatus);
    }

    // even though the namespace "c" exists in both catalogs, the ns_manage_access_role can only
    // grant privileges for
    // the namespace in its own catalog
    try (Response response =
        client
            .managementApi(manageAccessUserToken)
            .request(
                "v1/catalogs/{cat}/catalog-roles/{role}/grants",
                Map.of("cat", catalogName2, "role", "invalid_target_catalog_role"))
            .put(
                Entity.json(
                    new NamespaceGrant(
                        List.of(namespaceName),
                        NamespacePrivilege.TABLE_CREATE,
                        GrantResource.TypeEnum.NAMESPACE)))) {
      assertThat(response).returns(FORBIDDEN.getStatusCode(), Response::getStatus);
    }

    // nor can it grant privileges to the catalog role in the second catalog
    try (Response response =
        client
            .managementApi(manageAccessUserToken)
            .request(
                "v1/catalogs/{cat}/catalog-roles/{role}/grants",
                Map.of("cat", catalogName2, "role", "invalid_target_catalog_role"))
            .put(
                Entity.json(
                    new CatalogGrant(
                        CatalogPrivilege.TABLE_CREATE, GrantResource.TypeEnum.CATALOG)))) {
      assertThat(response).returns(FORBIDDEN.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testTokenExpiry() {
    // TokenExpiredException - if the token has expired.
    String newToken =
        defaultJwt()
            .withExpiresAt(Instant.now().plus(1, ChronoUnit.SECONDS))
            .sign(Algorithm.HMAC256("polaris"));
    Awaitility.await("expected list of records should be produced")
        .atMost(Duration.ofSeconds(20))
        .pollDelay(Duration.ofSeconds(1))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              try (Response response =
                  client.managementApi(newToken).request("v1/principals").get()) {
                assertThat(response)
                    .returns(Response.Status.UNAUTHORIZED.getStatusCode(), Response::getStatus);
              }
            });
  }

  @Test
  public void testTokenInactive() {
    // InvalidClaimException - if a claim contained a different value than the expected one.
    String newToken =
        defaultJwt().withClaim(CLAIM_KEY_ACTIVE, false).sign(Algorithm.HMAC256("polaris"));
    try (Response response = client.managementApi(newToken).request("v1/principals").get()) {
      assertThat(response)
          .returns(Response.Status.UNAUTHORIZED.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testTokenInvalidSignature() {
    // SignatureVerificationException - if the signature is invalid.
    String newToken = defaultJwt().sign(Algorithm.HMAC256("invalid_secret"));
    try (Response response = client.managementApi(newToken).request("v1/principals").get()) {
      assertThat(response)
          .returns(Response.Status.UNAUTHORIZED.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testTokenInvalidPrincipalId() {
    String newToken =
        defaultJwt().withClaim(CLAIM_KEY_PRINCIPAL_ID, 0).sign(Algorithm.HMAC256("polaris"));
    try (Response response = client.managementApi(newToken).request("v1/principals").get()) {
      assertThat(response)
          .returns(Response.Status.UNAUTHORIZED.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testNamespaceExistsStatus() {
    // create a catalog
    String catalogName = client.newEntityName("mytablemanagecatalog");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    managementApi.createCatalog(catalog);

    // create a namespace
    String namespaceName = "c";
    catalogApi.createNamespace(catalogName, namespaceName);

    // check if a namespace existed
    try (Response response =
        catalogApi.request("v1/" + catalogName + "/namespaces/" + namespaceName).head()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testDropNamespaceStatus() {
    // create a catalog
    String catalogName = client.newEntityName("mytablemanagecatalog");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    managementApi.createCatalog(catalog);

    // create a namespace
    String namespaceName = "c";
    catalogApi.createNamespace(catalogName, namespaceName);

    // drop a namespace
    try (Response response =
        catalogApi.request("v1/" + catalogName + "/namespaces/" + namespaceName).delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreateAndUpdateCatalogRoleWithReservedProperties() {
    String catalogName = client.newEntityName("mycatalog1");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("s3://required/base/location"))
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .build();
    managementApi.createCatalog(catalog);

    CatalogRole badCatalogRole =
        new CatalogRole("mycatalogrole", Map.of("polaris.reserved", "foo"), 0L, 0L, 1);
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles", Map.of("cat", catalogName))
            .post(Entity.json(new CreateCatalogRoleRequest(badCatalogRole)))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
    }

    CatalogRole okCatalogRole = new CatalogRole("mycatalogrole", Map.of("foo", "bar"), 0L, 0L, 1);
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles", Map.of("cat", catalogName))
            .post(Entity.json(new CreateCatalogRoleRequest(okCatalogRole)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    UpdateCatalogRoleRequest updateRequest =
        new UpdateCatalogRoleRequest(
            okCatalogRole.getEntityVersion(), Map.of("polaris.reserved", "true"));
    try (Response response =
        managementApi
            .request("v1/catalogs/{cat}/catalog-roles/mycatalogrole", Map.of("cat", catalogName))
            .put(Entity.json(updateRequest))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreateAndUpdatePrincipalRoleWithReservedProperties() {
    String principal = "testCreateAndUpdatePrincipalRoleWithReservedProperties";
    managementApi.createPrincipal(principal);

    PrincipalRole badPrincipalRole =
        new PrincipalRole(
            client.newEntityName("myprincipalrole"),
            false,
            Map.of("polaris.reserved", "foo"),
            0L,
            0L,
            1);
    try (Response response =
        managementApi
            .request("v1/principal-roles")
            .post(Entity.json(new CreatePrincipalRoleRequest(badPrincipalRole)))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
    }

    PrincipalRole goodPrincipalRole =
        new PrincipalRole(
            client.newEntityName("myprincipalrole"),
            false,
            Map.of("not.reserved", "foo"),
            0L,
            0L,
            1);
    try (Response response =
        managementApi
            .request("v1/principal-roles")
            .post(Entity.json(new CreatePrincipalRoleRequest(goodPrincipalRole)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    UpdatePrincipalRoleRequest badUpdate =
        new UpdatePrincipalRoleRequest(
            goodPrincipalRole.getEntityVersion(), ImmutableMap.of("polaris.reserved", "true"));
    try (Response response =
        managementApi
            .request("v1/principal-roles/{pr}", Map.of("pr", goodPrincipalRole.getName()))
            .put(Entity.json(badUpdate))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
    }

    managementApi.deletePrincipalRole(goodPrincipalRole);
    managementApi.deletePrincipal(principal);
  }

  @Test
  public void testCreateAndUpdatePrincipalWithReservedProperties() {
    String principal = "testCreateAndUpdatePrincipalWithReservedProperties";

    Principal badPrincipal =
        new Principal(
            principal, "clientId", ImmutableMap.of("polaris.reserved", "true"), 0L, 0L, 1);
    try (Response response =
        managementApi
            .request("v1/principals")
            .post(Entity.json(new CreatePrincipalRequest(badPrincipal, false)))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
    }

    Principal goodPrincipal =
        new Principal(principal, "clientId", ImmutableMap.of("not.reserved", "true"), 0L, 0L, 1);
    try (Response response =
        managementApi
            .request("v1/principals")
            .post(Entity.json(new CreatePrincipalRequest(goodPrincipal, false)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    UpdatePrincipalRequest badUpdate =
        new UpdatePrincipalRequest(
            goodPrincipal.getEntityVersion(), ImmutableMap.of("polaris.reserved", "true"));
    try (Response response =
        managementApi
            .request("v1/principals/{p}", Map.of("p", goodPrincipal.getName()))
            .put(Entity.json(badUpdate))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
    }

    managementApi.deletePrincipal(principal);
  }

  public static JWTCreator.Builder defaultJwt() {
    Instant now = Instant.now();
    return JWT.create()
        .withIssuer(ISSUER_KEY)
        .withSubject(String.valueOf(1))
        .withIssuedAt(now)
        .withExpiresAt(now.plus(10, ChronoUnit.SECONDS))
        .withJWTId(UUID.randomUUID().toString())
        .withClaim(CLAIM_KEY_ACTIVE, true)
        .withClaim(CLAIM_KEY_CLIENT_ID, rootCredentials.clientId())
        .withClaim(CLAIM_KEY_PRINCIPAL_ID, 1)
        .withClaim(CLAIM_KEY_SCOPE, PRINCIPAL_ROLE_ALL);
  }
}
