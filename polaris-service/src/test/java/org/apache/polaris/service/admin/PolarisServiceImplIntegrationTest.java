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

import static io.dropwizard.jackson.Jackson.newObjectMapper;
import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CatalogRoles;
import org.apache.polaris.core.admin.model.Catalogs;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantPrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.PrincipalRoles;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.PrincipalWithCredentialsCredentials;
import org.apache.polaris.core.admin.model.Principals;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.auth.TokenUtils;
import org.apache.polaris.service.catalog.PolarisTestClient;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisRealm;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

@ExtendWith({DropwizardExtensionsSupport.class, PolarisConnectionExtension.class})
public class PolarisServiceImplIntegrationTest {
  private static final int MAX_IDENTIFIER_LENGTH = 256;

  // TODO: Add a test-only hook that fully clobbers all persistence state so we can have a fresh
  // slate on every test case; otherwise, leftover state from one test from failures will interfere
  // with other test cases.
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          ConfigOverride.config(
              "server.applicationConnectors[0].port",
              "0"), // Bind to random port to support parallelism
          ConfigOverride.config("server.adminConnectors[0].port", "0"),

          // disallow FILE urls for the sake of tests below
          ConfigOverride.config(
              "featureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES", "S3,GCS,AZURE"),
          ConfigOverride.config("gcp_credentials.access_token", "abc"),
          ConfigOverride.config("gcp_credentials.expires_in", "12345"));
  private static PolarisTestClient userClient;
  private static String userToken;
  private static String realm;

  @BeforeAll
  public static void setup(
      PolarisConnectionExtension.PolarisToken adminToken, @PolarisRealm String polarisRealm)
      throws IOException {
    userClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), adminToken.token(), polarisRealm);
    userToken = adminToken.token();
    realm = polarisRealm;

    // Set up test location
    PolarisConnectionExtension.createTestDir(realm);
  }

  @AfterEach
  public void tearDown() {
    try (Response response = userClient.listCatalogs()) {
      response
          .readEntity(Catalogs.class)
          .getCatalogs()
          .forEach(
              catalog -> {
                // clean up the catalog before we try to drop it

                // delete all the namespaces
                try (Response res = userClient.listNamespaces(catalog.getName())) {
                  if (res.getStatus() != Response.Status.OK.getStatusCode()) {
                    LoggerFactory.getLogger(getClass())
                        .warn(
                            "Unable to list namespaces in catalog {}: {}",
                            catalog.getName(),
                            res.readEntity(String.class));
                  } else {
                    res.readEntity(ListNamespacesResponse.class)
                        .namespaces()
                        .forEach(
                            namespace ->
                                userClient
                                    .deleteNamespace(
                                        catalog.getName(), RESTUtil.encodeNamespace(namespace))
                                    .close());
                  }
                }

                // delete all the catalog roles except catalog_admin
                try (Response res = userClient.listCatalogRoles(catalog.getName())) {
                  if (res.getStatus() != Response.Status.OK.getStatusCode()) {
                    LoggerFactory.getLogger(getClass())
                        .warn(
                            "Unable to list catalog roles for catalog {}: {}",
                            catalog.getName(),
                            res.readEntity(String.class));
                    return;
                  }
                  res.readEntity(CatalogRoles.class).getRoles().stream()
                      .filter(cr -> !cr.getName().equals("catalog_admin"))
                      .forEach(
                          cr ->
                              userClient
                                  .deleteCatalogRole(catalog.getName(), cr.getName())
                                  .close());
                }

                Response deleteResponse = userClient.deleteCatalog(catalog.getName());
                if (deleteResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                  LoggerFactory.getLogger(getClass())
                      .warn(
                          "Unable to delete catalog {}: {}",
                          catalog.getName(),
                          deleteResponse.readEntity(String.class));
                }
                deleteResponse.close();
              });
    }
    try (Response response = userClient.listPrincipals()) {
      response.readEntity(Principals.class).getPrincipals().stream()
          .filter(
              principal ->
                  !principal.getName().equals(PolarisEntityConstants.getRootPrincipalName()))
          .forEach(
              principal -> {
                userClient.deletePrincipal(principal.getName()).close();
              });
    }
    try (Response response = userClient.listPrincipalRoles()) {
      response.readEntity(PrincipalRoles.class).getRoles().stream()
          .filter(
              principalRole ->
                  !principalRole
                      .getName()
                      .equals(PolarisEntityConstants.getNameOfPrincipalServiceAdminRole()))
          .forEach(
              principalRole -> userClient.deletePrincipalRole(principalRole.getName()).close());
    }
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
    try (Response response = userClient.listCatalogs()) {
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
    Principal principal = new Principal("a_new_user");
    String newToken = null;
    try (Response response = userClient.createPrincipal(principal)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
      PrincipalWithCredentials creds = response.readEntity(PrincipalWithCredentials.class);
      newToken =
          TokenUtils.getTokenFromSecrets(
              EXT.client(),
              EXT.getLocalPort(),
              creds.getCredentials().getClientId(),
              creds.getCredentials().getClientSecret(),
              realm);
    }
    PolarisTestClient newClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), newToken, realm);
    try (Response response = newClient.listCatalogs()) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreateCatalog() {
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(
                Entity.json(
                    "{\"catalog\":{\"type\":\"INTERNAL\",\"name\":\"my-catalog\",\"properties\":{\"default-base-location\":\"s3://my-bucket/path/to/data\"},\"storageConfigInfo\":{\"storageType\":\"S3\",\"roleArn\":\"arn:aws:iam::123456789012:role/my-role\",\"externalId\":\"externalId\",\"userArn\":\"userArn\",\"allowedLocations\":[\"s3://my-old-bucket/path/to/data\"]}}}"))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete
    try (Response response = userClient.deleteCatalog("my-catalog")) {
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

    ObjectMapper mapper = newObjectMapper();

    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(goodName)
            .setProperties(new CatalogProperties("s3://my-bucket/path/to/data"))
            .setStorageConfigInfo(awsConfigModel)
            .build();
    try (Response response = userClient.createCatalog(catalog)) {
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

      try (Response response = userClient.createCatalog(catalog)) {
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
    try (Response response = userClient.createCatalog(catalog)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    try (Response response = userClient.getCatalog("my-catalog")) {
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
            .setName("my-catalog")
            .setProperties(new CatalogProperties("gs://my-bucket/path/to/data"))
            .setStorageConfigInfo(gcpConfigModel)
            .build();
    try (Response response = userClient.createCatalog(catalog)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    try (Response response = userClient.getCatalog("my-catalog")) {
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(requestNode))) {
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

    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs", userToken)
            .post(Entity.json(requestNode))) {
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
  public void testCreateCatalogWithoutStorageConfig() throws JsonProcessingException {
    String catalogString =
        "{\"catalog\": {\"type\":\"INTERNAL\",\"name\":\"my-catalog\",\"properties\":{\"default-base-location\":\"s3://my-bucket/path/to/data\"}}}";
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs", userToken)
            .post(Entity.json(catalogString))) {
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
  public void testCreateCatalogWithUnparsableJson() throws JsonProcessingException {
    String catalogString = "{\"catalog\": {{\"bad data}";
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs", userToken)
            .post(Entity.json(catalogString))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertThat(error)
          .isNotNull()
          .extracting(ErrorResponse::message)
          .asString()
          .startsWith("Invalid JSON: Unexpected character");
    }
  }

  @Test
  public void testCreateCatalogWithDisallowedStorageConfig() throws JsonProcessingException {
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
    try (Response response = userClient.createCatalog(catalog)) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertThat(error)
          .isNotNull()
          .returns("Unsupported storage type: FILE", ErrorResponse::message);
    }
  }

  @Test
  public void testUpdateCatalogWithDisallowedStorageConfig() throws JsonProcessingException {
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
    try (Response response = userClient.createCatalog(catalog)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    Catalog fetchedCatalog = null;
    try (Response response = userClient.getCatalog(catalogName)) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

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
    try (Response response = userClient.updateCatalog(catalogName, updateRequest)) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
      ErrorResponse error = response.readEntity(ErrorResponse.class);

      assertThat(error).returns("Unsupported storage type: FILE", ErrorResponse::message);
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
    String catalogName = "my-external-catalog";
    String remoteUrl = "http://localhost:8080";
    Catalog catalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(catalogName)
            .setRemoteUrl(remoteUrl)
            .setProperties(new CatalogProperties("s3://my-bucket/path/to/data"))
            .setStorageConfigInfo(awsConfigModel)
            .build();
    createCatalog(catalog);

    try (Response response = userClient.getCatalog(catalogName)) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog fetchedCatalog = response.readEntity(Catalog.class);
      assertThat(fetchedCatalog)
          .isNotNull()
          .isInstanceOf(ExternalCatalog.class)
          .asInstanceOf(InstanceOfAssertFactories.type(ExternalCatalog.class))
          .returns(remoteUrl, ExternalCatalog::getRemoteUrl)
          .extracting(ExternalCatalog::getStorageConfigInfo)
          .isNotNull()
          .isInstanceOf(AwsStorageConfigInfo.class)
          .asInstanceOf(InstanceOfAssertFactories.type(AwsStorageConfigInfo.class))
          .returns("arn:aws:iam::123456789012:role/my-role", AwsStorageConfigInfo::getRoleArn);
    }

    // 204 Successful delete
    try (Response response = userClient.deleteCatalog(catalogName)) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
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

    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(requestNode))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void serialization() throws JsonProcessingException {
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
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("myazurecatalog")
            .setStorageConfigInfo(storageConfig)
            .setProperties(new CatalogProperties("abfss://container1@acct1.dfs.core.windows.net/"))
            .build();

    // 200 Successful create
    createCatalog(catalog);

    // 200 successful GET after creation
    Catalog fetchedCatalog = null;
    try (Response response = userClient.getCatalog("myazurecatalog")) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getName()).isEqualTo("myazurecatalog");
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
    try (Response response = userClient.updateCatalog("myazurecatalog", badUpdateRequest)) {
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
    try (Response response = userClient.updateCatalog("myazurecatalog", updateRequest)) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(
              Map.of("default-base-location", "abfss://newcontainer@acct1.dfs.core.windows.net/"));
    }

    // 204 Successful delete
    try (Response response = userClient.deleteCatalog("myazurecatalog")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreateListUpdateAndDeleteCatalog() {
    StorageConfigInfo storageConfig =
        new AwsStorageConfigInfo(
            "arn:aws:iam::123456789011:role/role1", StorageConfigInfo.StorageTypeEnum.S3);
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("mycatalog")
            .setStorageConfigInfo(storageConfig)
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();

    createCatalog(catalog);

    // Second attempt to create the same entity should fail with CONFLICT.
    try (Response response = userClient.createCatalog(catalog)) {
      assertThat(response).returns(Response.Status.CONFLICT.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    Catalog fetchedCatalog = null;
    try (Response response = userClient.getCatalog("mycatalog")) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getName()).isEqualTo("mycatalog");
      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://bucket1/"));
      assertThat(fetchedCatalog.getEntityVersion()).isGreaterThan(0);
    }

    // Should list the catalog.
    try (Response response = userClient.listCatalogs()) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(Catalogs.class))
          .extracting(Catalogs::getCatalogs)
          .asInstanceOf(InstanceOfAssertFactories.list(Catalog.class))
          .filteredOn(cat -> !cat.getName().equalsIgnoreCase("ROOT"))
          .satisfiesExactly(cat -> assertThat(cat).returns("mycatalog", Catalog::getName));
    }

    // Reject update of fields that can't be currently updated
    StorageConfigInfo modifiedStorageConfig =
        new AwsStorageConfigInfo(
            "arn:aws:iam::123456789011:role/newrole", StorageConfigInfo.StorageTypeEnum.S3);
    UpdateCatalogRequest badUpdateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://newbucket/"),
            modifiedStorageConfig);
    try (Response response = userClient.updateCatalog("mycatalog", badUpdateRequest)) {
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
            Map.of("default-base-location", "s3://newbucket/"),
            storageConfig);

    // 200 successful update
    try (Response response = userClient.updateCatalog("mycatalog", updateRequest)) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://newbucket/"));
    }

    // 200 GET after update should show new properties
    try (Response response = userClient.getCatalog("mycatalog")) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://newbucket/"));
    }

    // 204 Successful delete
    try (Response response = userClient.deleteCatalog("mycatalog")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // NOT_FOUND after deletion
    try (Response response = userClient.deleteCatalog("mycatalog")) {
      assertThat(response).returns(Response.Status.NOT_FOUND.getStatusCode(), Response::getStatus);
    }

    // Empty list
    try (Response response = userClient.listCatalogs()) {
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

  private static Invocation.Builder newRequest(String url, String token) {
    return EXT.client()
        .target(String.format(url, EXT.getLocalPort()))
        .request("application/json")
        .header("Authorization", "Bearer " + token)
        .header(REALM_PROPERTY_KEY, realm);
  }

  private static Invocation.Builder newRequest(String url) {
    return newRequest(url, userToken);
  }

  @Test
  public void testGetCatalogNotFound() {
    // there's no catalog yet. Expect 404
    try (Response response = userClient.getCatalog("mycatalog")) {
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
      try (Response response = userClient.getCatalog(invalidCatalogName)) {
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
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("mycatalog1")
            .setProperties(new CatalogProperties("s3://required/base/location"))
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .build();
    createCatalog(catalog);

    String longInvalidName = RandomStringUtils.random(MAX_IDENTIFIER_LENGTH + 1, true, true);
    List<String> invalidCatalogRoleNames =
        Arrays.asList(
            longInvalidName,
            "system$catalog1",
            "SYSTEM$TestCatalog",
            "System$test_catalog",
            "  SysTeM$ test catalog");

    for (String invalidCatalogRoleName : invalidCatalogRoleNames) {
      try (Response response = userClient.getCatalogRole("mycatalog1", invalidCatalogRoleName)) {
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
    Principal principal = new Principal("new_admin");
    String newToken = null;
    try (Response response = userClient.createPrincipal(principal)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
      PrincipalWithCredentials creds = response.readEntity(PrincipalWithCredentials.class);
      newToken =
          TokenUtils.getTokenFromSecrets(
              EXT.client(),
              EXT.getLocalPort(),
              creds.getCredentials().getClientId(),
              creds.getCredentials().getClientSecret(),
              realm);
    }
    PolarisTestClient newClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), newToken, realm);
    try (Response response = newClient.listPrincipals()) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreatePrincipalAndRotateCredentials() {
    Principal principal =
        Principal.builder()
            .setName("myprincipal")
            .setProperties(Map.of("custom-tag", "foo"))
            .build();

    PrincipalWithCredentialsCredentials creds = null;
    Principal returnedPrincipal = null;
    try (Response response = userClient.createPrincipal(principal, true)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
      PrincipalWithCredentials parsed = response.readEntity(PrincipalWithCredentials.class);
      creds = parsed.getCredentials();
      returnedPrincipal = parsed.getPrincipal();
    }
    assertThat(creds.getClientId()).isEqualTo(returnedPrincipal.getClientId());

    String oldClientId = creds.getClientId();
    String oldSecret = creds.getClientSecret();

    // Now rotate the credentials. First, if we try to just use the adminToken to rotate the
    // newly created principal's credentials, we should fail; rotateCredentials is only
    // a "self" privilege that even admins can't inherit.
    try (Response response = userClient.rotateCredential("myprincipal")) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }

    // Get a fresh token associate with the principal itself.
    String newPrincipalToken =
        TokenUtils.getTokenFromSecrets(
            EXT.client(), EXT.getLocalPort(), oldClientId, oldSecret, realm);

    // Any call should initially fail with error indicating that rotation is needed.
    PolarisTestClient newPrincipalClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), newPrincipalToken, realm);
    try (Response response = newPrincipalClient.getPrincipal("myprincipal")) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertThat(error)
          .isNotNull()
          .extracting(ErrorResponse::message)
          .asString()
          .contains("PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE");
    }

    // Now try to rotate using the principal's token.
    try (Response response = newPrincipalClient.rotateCredential("myprincipal")) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      PrincipalWithCredentials parsed = response.readEntity(PrincipalWithCredentials.class);
      creds = parsed.getCredentials();
      returnedPrincipal = parsed.getPrincipal();
    }
    assertThat(creds.getClientId()).isEqualTo(returnedPrincipal.getClientId());

    // ClientId shouldn't change
    assertThat(creds.getClientId()).isEqualTo(oldClientId);
    assertThat(creds.getClientSecret()).isNotEqualTo(oldSecret);

    // TODO: Test the validity of the old secret for getting tokens, here and then after a second
    // rotation that makes the old secret fall off retention.
  }

  @Test
  public void testCreateListUpdateAndDeletePrincipal() {
    Principal principal =
        Principal.builder()
            .setName("myprincipal")
            .setProperties(Map.of("custom-tag", "foo"))
            .build();
    try (Response response = userClient.createPrincipal(principal, null)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Second attempt to create the same entity should fail with CONFLICT.
    try (Response response = userClient.createPrincipal(principal, false)) {
      assertThat(response).returns(Response.Status.CONFLICT.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    Principal fetchedPrincipal = null;
    try (Response response = userClient.getPrincipal("myprincipal")) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedPrincipal = response.readEntity(Principal.class);

      assertThat(fetchedPrincipal.getName()).isEqualTo("myprincipal");
      assertThat(fetchedPrincipal.getProperties()).isEqualTo(Map.of("custom-tag", "foo"));
      assertThat(fetchedPrincipal.getEntityVersion()).isGreaterThan(0);
    }

    // Should list the principal.
    try (Response response = userClient.listPrincipals()) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(Principals.class))
          .extracting(Principals::getPrincipals)
          .asInstanceOf(InstanceOfAssertFactories.list(Principal.class))
          .anySatisfy(pr -> assertThat(pr).returns("myprincipal", Principal::getName));
    }

    UpdatePrincipalRequest updateRequest =
        new UpdatePrincipalRequest(
            fetchedPrincipal.getEntityVersion(), Map.of("custom-tag", "updated"));

    // 200 successful update
    try (Response response = userClient.updatePrincipal("myprincipal", updateRequest)) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedPrincipal = response.readEntity(Principal.class);

      assertThat(fetchedPrincipal.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 200 GET after update should show new properties
    try (Response response = userClient.getPrincipal("myprincipal")) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedPrincipal = response.readEntity(Principal.class);

      assertThat(fetchedPrincipal.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 204 Successful delete
    try (Response response = userClient.deletePrincipal("myprincipal")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // NOT_FOUND after deletion
    try (Response response = userClient.deletePrincipal("myprincipal")) {
      assertThat(response).returns(Response.Status.NOT_FOUND.getStatusCode(), Response::getStatus);
    }

    // Empty list
    try (Response response = userClient.listPrincipals()) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(Principals.class))
          .extracting(Principals::getPrincipals)
          .asInstanceOf(InstanceOfAssertFactories.list(Principal.class))
          .noneSatisfy(pr -> assertThat(pr).returns("myprincipal", Principal::getName));
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
    try (Response response = userClient.createPrincipal(principal, null)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

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

      try (Response response = userClient.createPrincipal(principal, false)) {
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
      try (Response response = userClient.getPrincipal(invalidPrincipalName)) {
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
        new PrincipalRole("myprincipalrole", Map.of("custom-tag", "foo"), 0L, 0L, 1);
    createPrincipalRole(principalRole);

    // Second attempt to create the same entity should fail with CONFLICT.
    try (Response response = userClient.createPrincipalRole(principalRole)) {
      assertThat(response).returns(Response.Status.CONFLICT.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    PrincipalRole fetchedPrincipalRole = null;
    try (Response response = userClient.getPrincipalRole("myprincipalrole")) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedPrincipalRole = response.readEntity(PrincipalRole.class);

      assertThat(fetchedPrincipalRole.getName()).isEqualTo("myprincipalrole");
      assertThat(fetchedPrincipalRole.getProperties()).isEqualTo(Map.of("custom-tag", "foo"));
      assertThat(fetchedPrincipalRole.getEntityVersion()).isGreaterThan(0);
    }

    // Should list the principalRole.
    try (Response response = userClient.listPrincipalRoles()) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .extracting(PrincipalRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(PrincipalRole.class))
          .anySatisfy(pr -> assertThat(pr).returns("myprincipalrole", PrincipalRole::getName));
    }

    UpdatePrincipalRoleRequest updateRequest =
        new UpdatePrincipalRoleRequest(
            fetchedPrincipalRole.getEntityVersion(), Map.of("custom-tag", "updated"));

    // 200 successful update
    try (Response response = userClient.updatePrincipalRole("myprincipalrole", updateRequest)) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedPrincipalRole = response.readEntity(PrincipalRole.class);

      assertThat(fetchedPrincipalRole.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 200 GET after update should show new properties
    try (Response response = userClient.getPrincipalRole("myprincipalrole")) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedPrincipalRole = response.readEntity(PrincipalRole.class);

      assertThat(fetchedPrincipalRole.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 204 Successful delete
    try (Response response = userClient.deletePrincipalRole("myprincipalrole")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // NOT_FOUND after deletion
    try (Response response = userClient.deletePrincipalRole("myprincipalrole")) {
      assertThat(response).returns(Response.Status.NOT_FOUND.getStatusCode(), Response::getStatus);
    }

    // Empty list
    try (Response response = userClient.listPrincipalRoles()) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .extracting(PrincipalRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(PrincipalRole.class))
          .noneSatisfy(pr -> assertThat(pr).returns("myprincipalrole", PrincipalRole::getName));
    }
  }

  @Test
  public void testCreatePrincipalRoleInvalidName() {
    String goodName = RandomStringUtils.random(MAX_IDENTIFIER_LENGTH, true, true);
    PrincipalRole principalRole =
        new PrincipalRole(goodName, Map.of("custom-tag", "good_principal_role"), 0L, 0L, 1);
    createPrincipalRole(principalRole);

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
              invalidPrincipalRoleName, Map.of("custom-tag", "bad_principal_role"), 0L, 0L, 1);

      try (Response response = userClient.createPrincipalRole(principalRole)) {
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
      try (Response response = userClient.getPrincipalRole(invalidPrincipalRoleName)) {
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
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("mycatalog1")
            .setProperties(new CatalogProperties("s3://required/base/location"))
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .build();
    createCatalog(catalog);

    Catalog catalog2 =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("mycatalog2")
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://required/base/other_location"))
            .build();
    createCatalog(catalog2);

    CatalogRole catalogRole =
        new CatalogRole("mycatalogrole", Map.of("custom-tag", "foo"), 0L, 0L, 1);
    try (Response response = userClient.createCatalogRole("mycatalog1", catalogRole)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Second attempt to create the same entity should fail with CONFLICT.
    try (Response response = userClient.createCatalogRole("mycatalog1", catalogRole)) {
      assertThat(response).returns(Response.Status.CONFLICT.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    CatalogRole fetchedCatalogRole = null;
    try (Response response = userClient.getCatalogRole("mycatalog1", "mycatalogrole")) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalogRole = response.readEntity(CatalogRole.class);

      assertThat(fetchedCatalogRole.getName()).isEqualTo("mycatalogrole");
      assertThat(fetchedCatalogRole.getProperties()).isEqualTo(Map.of("custom-tag", "foo"));
      assertThat(fetchedCatalogRole.getEntityVersion()).isGreaterThan(0);
    }

    // Should list the catalogRole.
    try (Response response = userClient.listCatalogRoles("mycatalog1")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .extracting(CatalogRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(CatalogRole.class))
          .anySatisfy(cr -> assertThat(cr).returns("mycatalogrole", CatalogRole::getName));
    }

    // Empty list if listing in catalog2
    try (Response response = userClient.listCatalogRoles("mycatalog2")) {
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
        userClient.updateCatalogRole("mycatalog1", "mycatalogrole", updateRequest)) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalogRole = response.readEntity(CatalogRole.class);

      assertThat(fetchedCatalogRole.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 200 GET after update should show new properties
    try (Response response = userClient.getCatalogRole("mycatalog1", "mycatalogrole")) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      fetchedCatalogRole = response.readEntity(CatalogRole.class);

      assertThat(fetchedCatalogRole.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 204 Successful delete
    try (Response response = userClient.deleteCatalogRole("mycatalog1", "mycatalogrole")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // NOT_FOUND after deletion
    try (Response response = userClient.deleteCatalogRole("mycatalog1", "mycatalogrole")) {
      assertThat(response).returns(Response.Status.NOT_FOUND.getStatusCode(), Response::getStatus);
    }

    // Empty list
    try (Response response = userClient.listCatalogRoles("mycatalog1")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .extracting(CatalogRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(CatalogRole.class))
          .noneSatisfy(cr -> assertThat(cr).returns("mycatalogrole", CatalogRole::getName));
    }

    // 204 Successful delete mycatalog
    try (Response response = userClient.deleteCatalog("mycatalog1")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete mycatalog2
    try (Response response = userClient.deleteCatalog("mycatalog2")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testAssignListAndRevokePrincipalRoles() {
    // Create two Principals
    Principal principal1 = new Principal("myprincipal1");
    try (Response response = userClient.createPrincipal(principal1, false)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    Principal principal2 = new Principal("myprincipal2");
    try (Response response = userClient.createPrincipal(principal2, false)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // One PrincipalRole
    PrincipalRole principalRole = new PrincipalRole("myprincipalrole");
    createPrincipalRole(principalRole);

    // Assign the role to myprincipal1
    try (Response response = userClient.assignPrincipalRole("myprincipal1", principalRole)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Should list myprincipalrole
    try (Response response = userClient.listPrincipalRoles("myprincipal1")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .extracting(PrincipalRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(PrincipalRole.class))
          .hasSize(1)
          .satisfiesExactly(
              pr -> assertThat(pr).returns("myprincipalrole", PrincipalRole::getName));
    }

    // Should list myprincipal1 if listing assignees of myprincipalrole
    try (Response response = userClient.listPrincipals("myprincipalrole")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(Principals.class))
          .extracting(Principals::getPrincipals)
          .asInstanceOf(InstanceOfAssertFactories.list(Principal.class))
          .hasSize(1)
          .satisfiesExactly(pr -> assertThat(pr).returns("myprincipal1", Principal::getName));
    }

    // Empty list if listing in principal2
    try (Response response = userClient.listPrincipalRoles("myprincipal2")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .returns(List.of(), PrincipalRoles::getRoles);
    }

    // 204 Successful revoke
    try (Response response = userClient.revokePrincipalRole("myprincipal1", "myprincipalrole")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // Empty list
    try (Response response = userClient.listPrincipalRoles("myprincipal1")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .returns(List.of(), PrincipalRoles::getRoles);
    }
    try (Response response = userClient.listPrincipals("myprincipalrole")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(Principals.class))
          .returns(List.of(), Principals::getPrincipals);
    }

    // 204 Successful delete myprincipal1
    try (Response response = userClient.deletePrincipal("myprincipal1")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete myprincipal2
    try (Response response = userClient.deletePrincipal("myprincipal2")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete myprincipalrole
    try (Response response = userClient.deletePrincipalRole("myprincipalrole")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testAssignListAndRevokeCatalogRoles() {
    // Create two PrincipalRoles
    PrincipalRole principalRole1 = new PrincipalRole("mypr1");
    createPrincipalRole(principalRole1);

    PrincipalRole principalRole2 = new PrincipalRole("mypr2");
    createPrincipalRole(principalRole2);

    // One CatalogRole
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("mycatalog")
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    createCatalog(catalog);

    CatalogRole catalogRole = new CatalogRole("mycr");
    try (Response response = userClient.createCatalogRole("mycatalog", catalogRole)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Create another one in a different catalog.
    Catalog otherCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("othercatalog")
            .setProperties(new CatalogProperties("s3://path/to/data"))
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .build();
    createCatalog(otherCatalog);

    CatalogRole otherCatalogRole = new CatalogRole("myothercr");
    try (Response response = userClient.createCatalogRole("othercatalog", otherCatalogRole)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Assign both the roles to mypr1
    try (Response response =
        userClient.grantCatalogRoleToPrincipalRole("mypr1", "mycatalog", catalogRole)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    try (Response response =
        userClient.grantCatalogRoleToPrincipalRole("mypr1", "othercatalog", otherCatalogRole)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Should list only mycr
    try (Response response = userClient.listCatalogRoles("mypr1", "mycatalog")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .extracting(CatalogRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(CatalogRole.class))
          .hasSize(1)
          .satisfiesExactly(cr -> assertThat(cr).returns("mycr", CatalogRole::getName));
    }

    // Should list mypr1 if listing assignees of mycr
    try (Response response = userClient.listPrincipalRoles("mycatalog", "mycr")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .extracting(PrincipalRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(PrincipalRole.class))
          .hasSize(1)
          .satisfiesExactly(pr -> assertThat(pr).returns("mypr1", PrincipalRole::getName));
    }

    // Empty list if listing in principalRole2
    try (Response response = userClient.listCatalogRoles("mypr2", "mycatalog")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .returns(List.of(), CatalogRoles::getRoles);
    }

    // 204 Successful revoke
    try (Response response = userClient.revokeCatalogRole("mypr1", "mycatalog", "mycr")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // Empty list
    try (Response response = userClient.listCatalogRoles("mypr1", "mycatalog")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .returns(List.of(), CatalogRoles::getRoles);
    }
    try (Response response = userClient.listPrincipalRoles("mycatalog", "mycr")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .returns(List.of(), PrincipalRoles::getRoles);
    }

    // 204 Successful delete mypr1
    try (Response response = userClient.deletePrincipalRole("mypr1")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete mypr2
    try (Response response = userClient.deletePrincipalRole("mypr2")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete mycr
    try (Response response = userClient.deleteCatalogRole("mycatalog", "mycr")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete mycatalog
    try (Response response = userClient.deleteCatalog("mycatalog")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete myothercr
    try (Response response = userClient.deleteCatalogRole("othercatalog", "myothercr")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete othercatalog
    try (Response response = userClient.deleteCatalog("othercatalog")) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCatalogAdminGrantAndRevokeCatalogRoles() {
    // Create a PrincipalRole and a new catalog. Grant the catalog_admin role to the new principal
    // role
    String principalRoleName = "mypr33";
    PrincipalRole principalRole1 = new PrincipalRole(principalRoleName);
    createPrincipalRole(principalRole1);

    String catalogName = "myuniquetestcatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    createCatalog(catalog);

    CatalogRole catalogAdminRole = readCatalogRole(catalogName, "catalog_admin");
    grantCatalogRoleToPrincipalRole(principalRoleName, catalogName, catalogAdminRole, userToken);

    PrincipalWithCredentials catalogAdminPrincipal = createPrincipal("principal1");

    grantPrincipalRoleToPrincipal(catalogAdminPrincipal.getPrincipal().getName(), principalRole1);

    String catalogAdminToken =
        TokenUtils.getTokenFromSecrets(
            EXT.client(),
            EXT.getLocalPort(),
            catalogAdminPrincipal.getCredentials().getClientId(),
            catalogAdminPrincipal.getCredentials().getClientSecret(),
            realm);

    // Create a second principal role. Use the catalog admin principal to list principal roles and
    // grant a catalog role to the new principal role
    String principalRoleName2 = "mypr2";
    PrincipalRole principalRole2 = new PrincipalRole(principalRoleName2);
    createPrincipalRole(principalRole2);

    // create a catalog role and grant it manage_content privilege
    String catalogRoleName = "mycr1";
    createCatalogRole(catalogName, catalogRoleName, catalogAdminToken);

    CatalogPrivilege privilege = CatalogPrivilege.CATALOG_MANAGE_CONTENT;
    grantPrivilegeToCatalogRole(
        catalogName,
        catalogRoleName,
        new CatalogGrant(privilege, GrantResource.TypeEnum.CATALOG),
        catalogAdminToken,
        Response.Status.CREATED);

    // The catalog admin can grant the new catalog role to the mypr2 principal role
    grantCatalogRoleToPrincipalRole(
        principalRoleName2, catalogName, new CatalogRole(catalogRoleName), catalogAdminToken);

    // But the catalog admin cannot revoke the role because it requires
    // PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE
    PolarisTestClient adminClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), catalogAdminToken, realm);
    try (Response response =
        adminClient.revokeCatalogRole(principalRoleName, catalogName, catalogRoleName)) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }

    // The service admin can revoke the role because it has the
    // PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE privilege
    try (Response response =
        userClient.revokeCatalogRole(principalRoleName, catalogName, catalogRoleName)) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testServiceAdminCanTransferCatalogAdmin() {
    // Create a PrincipalRole and a new catalog. Grant the catalog_admin role to the new principal
    // role
    String principalRoleName = "mypr33";
    PrincipalRole principalRole1 = new PrincipalRole(principalRoleName);
    createPrincipalRole(principalRole1);

    String catalogName = "myothertestcatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    createCatalog(catalog);

    CatalogRole catalogAdminRole = readCatalogRole(catalogName, "catalog_admin");
    grantCatalogRoleToPrincipalRole(principalRoleName, catalogName, catalogAdminRole, userToken);

    PrincipalWithCredentials catalogAdminPrincipal = createPrincipal("principal1");

    grantPrincipalRoleToPrincipal(catalogAdminPrincipal.getPrincipal().getName(), principalRole1);

    String catalogAdminToken =
        TokenUtils.getTokenFromSecrets(
            EXT.client(),
            EXT.getLocalPort(),
            catalogAdminPrincipal.getCredentials().getClientId(),
            catalogAdminPrincipal.getCredentials().getClientSecret(),
            realm);

    // service_admin revokes the catalog_admin privilege from its principal role
    try {
      try (Response response =
          userClient.revokeCatalogRole("service_admin", catalogName, "catalog_admin")) {
        assertThat(response)
            .returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
      }

      // the service_admin can not revoke the catalog_admin privilege from the new principal role
      PolarisTestClient adminClient =
          new PolarisTestClient(EXT.client(), EXT.getLocalPort(), catalogAdminToken, realm);
      try (Response response =
          adminClient.revokeCatalogRole("service_admin", catalogName, "catalog_admin")) {
        assertThat(response)
            .returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
      }
    } finally {
      // grant the admin role back to service_admin so that cleanup can happen
      grantCatalogRoleToPrincipalRole(
          "service_admin", catalogName, catalogAdminRole, catalogAdminToken);
    }
  }

  @Test
  public void testCatalogAdminGrantAndRevokeCatalogRolesFromWrongCatalog() {
    // Create a PrincipalRole and a new catalog. Grant the catalog_admin role to the new principal
    // role
    String principalRoleName = "mypr33";
    PrincipalRole principalRole1 = new PrincipalRole(principalRoleName);
    createPrincipalRole(principalRole1);

    // create a catalog
    String catalogName = "mytestcatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    createCatalog(catalog);

    // create a second catalog
    String catalogName2 = "anothercatalog";
    Catalog catalog2 =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName2)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    createCatalog(catalog2);

    // create a catalog role *in the second catalog* and grant it manage_content privilege
    String catalogRoleName = "mycr1";
    createCatalogRole(catalogName2, catalogRoleName, userToken);

    // Get the catalog admin role from the *first* catalog and grant that role to the principal role
    CatalogRole catalogAdminRole = readCatalogRole(catalogName, "catalog_admin");
    grantCatalogRoleToPrincipalRole(principalRoleName, catalogName, catalogAdminRole, userToken);

    // Create a principal and grant the principal role to it
    PrincipalWithCredentials catalogAdminPrincipal = createPrincipal("principal1");
    grantPrincipalRoleToPrincipal(catalogAdminPrincipal.getPrincipal().getName(), principalRole1);

    String catalogAdminToken =
        TokenUtils.getTokenFromSecrets(
            EXT.client(),
            EXT.getLocalPort(),
            catalogAdminPrincipal.getCredentials().getClientId(),
            catalogAdminPrincipal.getCredentials().getClientSecret(),
            realm);

    // Create a second principal role.
    String principalRoleName2 = "mypr2";
    PrincipalRole principalRole2 = new PrincipalRole(principalRoleName2);
    createPrincipalRole(principalRole2);

    // The catalog admin cannot grant the new catalog role to the mypr2 principal role because the
    // catalog role is in the wrong catalog
    PolarisTestClient adminClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), catalogAdminToken, realm);
    try (Response response =
        adminClient.grantCatalogRoleToPrincipalRole(
            principalRoleName, catalogName2, new CatalogRole(catalogRoleName))) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testTableManageAccessCanGrantAndRevokeFromCatalogRoles() {
    // Create a PrincipalRole and a new catalog.
    String principalRoleName = "mypr33";
    PrincipalRole principalRole1 = new PrincipalRole(principalRoleName);
    createPrincipalRole(principalRole1);

    // create a catalog
    String catalogName = "mytablemanagecatalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    createCatalog(catalog);

    // create a valid target CatalogRole in this catalog
    createCatalogRole(catalogName, "target_catalog_role", userToken);

    // create a second catalog
    String catalogName2 = "anothertablemanagecatalog";
    Catalog catalog2 =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName2)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    createCatalog(catalog2);

    // create an *invalid* target CatalogRole in second catalog
    createCatalogRole(catalogName2, "invalid_target_catalog_role", userToken);

    // create the namespace "c" in *both* namespaces
    String namespaceName = "c";
    createNamespace(catalogName, namespaceName);
    createNamespace(catalogName2, namespaceName);

    // create a catalog role *in the first catalog* and grant it manage_content privilege at the
    // namespace level
    // grant that role to the PrincipalRole
    String catalogRoleName = "ns_manage_access_role";
    createCatalogRole(catalogName, catalogRoleName, userToken);
    grantPrivilegeToCatalogRole(
        catalogName,
        catalogRoleName,
        new NamespaceGrant(
            List.of(namespaceName),
            NamespacePrivilege.CATALOG_MANAGE_ACCESS,
            GrantResource.TypeEnum.NAMESPACE),
        userToken,
        Response.Status.CREATED);

    grantCatalogRoleToPrincipalRole(
        principalRoleName, catalogName, new CatalogRole(catalogRoleName), userToken);

    // Create a principal and grant the principal role to it
    PrincipalWithCredentials catalogAdminPrincipal = createPrincipal("ns_manage_access_user");
    grantPrincipalRoleToPrincipal(catalogAdminPrincipal.getPrincipal().getName(), principalRole1);

    String manageAccessUserToken =
        TokenUtils.getTokenFromSecrets(
            EXT.client(),
            EXT.getLocalPort(),
            catalogAdminPrincipal.getCredentials().getClientId(),
            catalogAdminPrincipal.getCredentials().getClientSecret(),
            realm);

    // Use the ns_manage_access_user to grant TABLE_CREATE access to the target catalog role
    // This works because the user has CATALOG_MANAGE_ACCESS within the namespace and the target
    // catalog role is in
    // the same catalog
    grantPrivilegeToCatalogRole(
        catalogName,
        "target_catalog_role",
        new NamespaceGrant(
            List.of(namespaceName),
            NamespacePrivilege.TABLE_CREATE,
            GrantResource.TypeEnum.NAMESPACE),
        manageAccessUserToken,
        Response.Status.CREATED);

    // Even though the ns_manage_access_role can grant privileges to the catalog role, it cannot
    // grant the target
    // catalog role to the mypr2 principal role because it doesn't have privilege to manage grants
    // on the catalog role
    // as a securable
    PolarisTestClient manageAccessUserClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), manageAccessUserToken, realm);
    try (Response response =
        manageAccessUserClient.grantCatalogRoleToPrincipalRole(
            principalRoleName, catalogName, new CatalogRole("target_catalog_role"))) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }

    // The user cannot grant catalog-level privileges to the catalog role
    grantPrivilegeToCatalogRole(
        catalogName,
        "target_catalog_role",
        new CatalogGrant(CatalogPrivilege.TABLE_CREATE, GrantResource.TypeEnum.CATALOG),
        manageAccessUserToken,
        Response.Status.FORBIDDEN);

    // even though the namespace "c" exists in both catalogs, the ns_manage_access_role can only
    // grant privileges for
    // the namespace in its own catalog
    grantPrivilegeToCatalogRole(
        catalogName2,
        "invalid_target_catalog_role",
        new NamespaceGrant(
            List.of(namespaceName),
            NamespacePrivilege.TABLE_CREATE,
            GrantResource.TypeEnum.NAMESPACE),
        manageAccessUserToken,
        Response.Status.FORBIDDEN);

    // nor can it grant privileges to the catalog role in the second catalog
    grantPrivilegeToCatalogRole(
        catalogName2,
        "invalid_target_catalog_role",
        new CatalogGrant(CatalogPrivilege.TABLE_CREATE, GrantResource.TypeEnum.CATALOG),
        manageAccessUserToken,
        Response.Status.FORBIDDEN);
  }

  private static void createNamespace(String catalogName, String namespaceName) {
    try (Response response = userClient.createNamespace(catalogName, namespaceName)) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    }
  }

  private static void createCatalog(Catalog catalog) {
    try (Response response = userClient.createCatalog(catalog)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  private static void grantPrivilegeToCatalogRole(
      String catalogName,
      String catalogRoleName,
      GrantResource grant,
      String catalogAdminToken,
      Response.Status expectedStatus) {
    PolarisTestClient catalogAdminClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), catalogAdminToken, realm);
    try (Response response =
        catalogAdminClient.grantPrivilegeToCatalogRole(catalogName, catalogRoleName, grant)) {
      assertThat(response).returns(expectedStatus.getStatusCode(), Response::getStatus);
    }
  }

  private static void createCatalogRole(
      String catalogName, String catalogRoleName, String catalogAdminToken) {
    PolarisTestClient adminClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), catalogAdminToken, realm);
    try (Response response =
        adminClient.createCatalogRole(catalogName, new CatalogRole(catalogRoleName))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  private static void grantPrincipalRoleToPrincipal(
      String principalName, PrincipalRole principalRole) {
    try (Response response =
        userClient.grantPrincipalRole(
            principalName, new GrantPrincipalRoleRequest(principalRole))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  private static PrincipalWithCredentials createPrincipal(String principalName) {
    PrincipalWithCredentials catalogAdminPrincipal;
    try (Response response = userClient.createPrincipal(new Principal(principalName), false)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
      catalogAdminPrincipal = response.readEntity(PrincipalWithCredentials.class);
    }
    return catalogAdminPrincipal;
  }

  private static void grantCatalogRoleToPrincipalRole(
      String principalRoleName, String catalogName, CatalogRole catalogRole, String token) {
    PolarisTestClient client =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), token, realm);
    try (Response response =
        client.grantCatalogRoleToPrincipalRole(principalRoleName, catalogName, catalogRole)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  private static CatalogRole readCatalogRole(String catalogName, String roleName) {
    try (Response response = userClient.getCatalogRole(catalogName, roleName)) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      return response.readEntity(CatalogRole.class);
    }
  }

  private static void createPrincipalRole(PrincipalRole principalRole1) {
    try (Response response = userClient.createPrincipalRole(principalRole1)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }
}
