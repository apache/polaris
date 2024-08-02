/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.service.admin;

import static io.dropwizard.jackson.Jackson.newObjectMapper;
import static io.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.polaris.core.admin.model.AwsStorageConfigInfo;
import io.polaris.core.admin.model.AzureStorageConfigInfo;
import io.polaris.core.admin.model.Catalog;
import io.polaris.core.admin.model.CatalogProperties;
import io.polaris.core.admin.model.CatalogRole;
import io.polaris.core.admin.model.CatalogRoles;
import io.polaris.core.admin.model.Catalogs;
import io.polaris.core.admin.model.CreateCatalogRequest;
import io.polaris.core.admin.model.CreateCatalogRoleRequest;
import io.polaris.core.admin.model.CreatePrincipalRequest;
import io.polaris.core.admin.model.CreatePrincipalRoleRequest;
import io.polaris.core.admin.model.ExternalCatalog;
import io.polaris.core.admin.model.FileStorageConfigInfo;
import io.polaris.core.admin.model.GrantCatalogRoleRequest;
import io.polaris.core.admin.model.PolarisCatalog;
import io.polaris.core.admin.model.Principal;
import io.polaris.core.admin.model.PrincipalRole;
import io.polaris.core.admin.model.PrincipalRoles;
import io.polaris.core.admin.model.PrincipalWithCredentials;
import io.polaris.core.admin.model.PrincipalWithCredentialsCredentials;
import io.polaris.core.admin.model.Principals;
import io.polaris.core.admin.model.StorageConfigInfo;
import io.polaris.core.admin.model.UpdateCatalogRequest;
import io.polaris.core.admin.model.UpdateCatalogRoleRequest;
import io.polaris.core.admin.model.UpdatePrincipalRequest;
import io.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import io.polaris.core.entity.PolarisEntityConstants;
import io.polaris.service.PolarisApplication;
import io.polaris.service.auth.TokenUtils;
import io.polaris.service.config.PolarisApplicationConfig;
import io.polaris.service.test.PolarisConnectionExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
              "featureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES", "S3,GCS,AZURE"));
  private static String userToken;
  private static String realm;

  @BeforeAll
  public static void setup(PolarisConnectionExtension.PolarisToken adminToken) {
    userToken = adminToken.token();
    realm = PolarisConnectionExtension.getTestRealm(PolarisServiceImplIntegrationTest.class);
  }

  @AfterEach
  public void tearDown() {
    try (Response response = newRequest("http://localhost:%d/api/management/v1/catalogs").get()) {
      response.readEntity(Catalogs.class).getCatalogs().stream()
          .forEach(
              catalog -> {
                try (Response innerResponse =
                    newRequest(
                            "http://localhost:%d/api/management/v1/catalogs/" + catalog.getName())
                        .delete()) {}
              });
    }
    try (Response response = newRequest("http://localhost:%d/api/management/v1/principals").get()) {
      response.readEntity(Principals.class).getPrincipals().stream()
          .filter(
              principal ->
                  !principal.getName().equals(PolarisEntityConstants.getRootPrincipalName()))
          .forEach(
              principal -> {
                try (Response innerResponse =
                    newRequest(
                            "http://localhost:%d/api/management/v1/principals/"
                                + principal.getName())
                        .delete()) {}
              });
    }
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles").get()) {
      response.readEntity(PrincipalRoles.class).getRoles().stream()
          .filter(
              principalRole ->
                  !principalRole
                      .getName()
                      .equals(PolarisEntityConstants.getNameOfPrincipalServiceAdminRole()))
          .forEach(
              principalRole -> {
                try (Response innerResponse =
                    newRequest(
                            "http://localhost:%d/api/management/v1/principal-roles/"
                                + principalRole.getName())
                        .delete()) {}
              });
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
    try (Response response = newRequest("http://localhost:%d/api/management/v1/catalogs").get()) {
      assertThat(response)
          .returns(200, Response::getStatus)
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals")
            .post(Entity.json(principal))) {
      assertThat(response).returns(201, Response::getStatus);
      PrincipalWithCredentials creds = response.readEntity(PrincipalWithCredentials.class);
      newToken =
          TokenUtils.getTokenFromSecrets(
              EXT.client(),
              EXT.getLocalPort(),
              creds.getCredentials().getClientId(),
              creds.getCredentials().getClientSecret(),
              realm);
    }
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs", "BEARER " + newToken).get()) {
      assertThat(response).returns(403, Response::getStatus);
    }
  }

  @Test
  public void testCreateCatalog() {
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("my-catalog")
            .setProperties(new CatalogProperties("s3://my-bucket/path/to/data"))
            .setStorageConfigInfo(awsConfigModel)
            .build();
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(
                Entity.json(
                    "{\"catalog\":{\"type\":\"INTERNAL\",\"name\":\"my-catalog\",\"properties\":{\"default-base-location\":\"s3://my-bucket/path/to/data\"},\"storageConfigInfo\":{\"storageType\":\"S3\",\"roleArn\":\"arn:aws:iam::123456789012:role/my-role\",\"externalId\":\"externalId\",\"userArn\":\"userArn\",\"allowedLocations\":[\"s3://my-old-bucket/path/to/data\"]}}}"))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // 204 Successful delete
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/my-catalog").delete()) {
      assertThat(response).returns(204, Response::getStatus);
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(mapper.writeValueAsString(catalog)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
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

      try (Response response =
          newRequest("http://localhost:%d/api/management/v1/catalogs")
              .post(Entity.json(mapper.writeValueAsString(catalog)))) {
        assertThat(response)
            .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
        assertThat(response.hasEntity()).isTrue();
        ErrorResponse errorResponse = response.readEntity(ErrorResponse.class);
        assertThat(errorResponse.message()).contains("Invalid value:");
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
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
        newRequest("http://localhost:%d/api/management/v1/catalogs", "Bearer " + userToken)
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
        newRequest("http://localhost:%d/api/management/v1/catalogs", "Bearer " + userToken)
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
        newRequest("http://localhost:%d/api/management/v1/catalogs", "Bearer " + userToken)
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs", "Bearer " + userToken)
            .post(Entity.json(new CreateCatalogRequest(catalog)))) {
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs", "Bearer " + userToken)
            .post(Entity.json(new CreateCatalogRequest(catalog)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    Catalog fetchedCatalog = null;
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/catalogs/" + catalogName,
                "Bearer " + userToken)
            .get()) {
      assertThat(response).returns(200, Response::getStatus);
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
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/catalogs/" + catalogName,
                "Bearer " + userToken)
            .put(Entity.json(updateRequest))) {
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(new CreateCatalogRequest(catalog)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/" + catalogName).get()) {
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/" + catalogName).delete()) {
      assertThat(response).returns(204, Response::getStatus);
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
        new AzureStorageConfigInfo(
            "azure:tenantid:12345",
            AzureStorageConfigInfo.AuthTypeEnum.SAS_TOKEN,
            StorageConfigInfo.StorageTypeEnum.AZURE);
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("myazurecatalog")
            .setStorageConfigInfo(storageConfig)
            .setProperties(new CatalogProperties("abfss://container1@acct1.dfs.core.windows.net/"))
            .build();

    // 200 Successful create
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(new CreateCatalogRequest(catalog)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    Catalog fetchedCatalog = null;
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/myazurecatalog").get()) {
      assertThat(response).returns(200, Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getName()).isEqualTo("myazurecatalog");
      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(
              Map.of("default-base-location", "abfss://container1@acct1.dfs.core.windows.net/"));
      assertThat(fetchedCatalog.getEntityVersion()).isGreaterThan(0);
    }

    StorageConfigInfo modifiedStorageConfig =
        new AzureStorageConfigInfo(
            "azure:tenantid:22222",
            AzureStorageConfigInfo.AuthTypeEnum.SAS_TOKEN,
            StorageConfigInfo.StorageTypeEnum.AZURE);
    UpdateCatalogRequest badUpdateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "abfss://newcontainer@acct1.dfs.core.windows.net/"),
            modifiedStorageConfig);
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/myazurecatalog")
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

    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "abfss://newcontainer@acct1.dfs.core.windows.net/"),
            storageConfig);

    // 200 successful update
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/myazurecatalog")
            .put(Entity.json(updateRequest))) {
      assertThat(response).returns(200, Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(
              Map.of("default-base-location", "abfss://newcontainer@acct1.dfs.core.windows.net/"));
    }

    // 204 Successful delete
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/myazurecatalog").delete()) {
      assertThat(response).returns(204, Response::getStatus);
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

    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(new CreateCatalogRequest(catalog)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Second attempt to create the same entity should fail with CONFLICT.
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(new CreateCatalogRequest(catalog)))) {
      assertThat(response).returns(Response.Status.CONFLICT.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    Catalog fetchedCatalog = null;
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog").get()) {
      assertThat(response).returns(200, Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getName()).isEqualTo("mycatalog");
      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://bucket1/"));
      assertThat(fetchedCatalog.getEntityVersion()).isGreaterThan(0);
    }

    // Should list the catalog.
    try (Response response = newRequest("http://localhost:%d/api/management/v1/catalogs").get()) {
      assertThat(response)
          .returns(200, Response::getStatus)
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog")
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

    UpdateCatalogRequest updateRequest =
        new UpdateCatalogRequest(
            fetchedCatalog.getEntityVersion(),
            Map.of("default-base-location", "s3://newbucket/"),
            storageConfig);

    // 200 successful update
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog")
            .put(Entity.json(updateRequest))) {
      assertThat(response).returns(200, Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://newbucket/"));
    }

    // 200 GET after update should show new properties
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog").get()) {
      assertThat(response).returns(200, Response::getStatus);
      fetchedCatalog = response.readEntity(Catalog.class);

      assertThat(fetchedCatalog.getProperties().toMap())
          .isEqualTo(Map.of("default-base-location", "s3://newbucket/"));
    }

    // 204 Successful delete
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog").delete()) {
      assertThat(response).returns(204, Response::getStatus);
    }

    // NOT_FOUND after deletion
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog").get()) {
      assertThat(response).returns(404, Response::getStatus);
    }

    // Empty list
    try (Response response = newRequest("http://localhost:%d/api/management/v1/catalogs").get()) {
      assertThat(response)
          .returns(200, Response::getStatus)
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
        .header("Authorization", token)
        .header(REALM_PROPERTY_KEY, realm);
  }

  private static Invocation.Builder newRequest(String url) {
    return newRequest(url, "Bearer " + userToken);
  }

  @Test
  public void testGetCatalogNotFound() {
    // there's no catalog yet. Expect 404
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog").get()) {
      assertThat(response).returns(404, Response::getStatus);
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
      try (Response response =
          newRequest("http://localhost:%d/api/management/v1/catalogs/" + invalidCatalogName)
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(new CreateCatalogRequest(catalog)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

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
          newRequest(
                  "http://localhost:%d/api/management/v1/catalogs/mycatalog1/catalog-roles/"
                      + invalidCatalogRoleName)
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
    Principal principal = new Principal("new_admin");
    String newToken = null;
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals")
            .post(Entity.json(principal))) {
      assertThat(response).returns(201, Response::getStatus);
      PrincipalWithCredentials creds = response.readEntity(PrincipalWithCredentials.class);
      newToken =
          TokenUtils.getTokenFromSecrets(
              EXT.client(),
              EXT.getLocalPort(),
              creds.getCredentials().getClientId(),
              creds.getCredentials().getClientSecret(),
              realm);
    }
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals", "Bearer " + newToken)
            .get()) {
      assertThat(response).returns(403, Response::getStatus);
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals")
            .post(Entity.json(new CreatePrincipalRequest(principal, true)))) {
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals/myprincipal/rotate")
            .post(Entity.json(""))) {
      assertThat(response).returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
    }

    // Get a fresh token associate with the principal itself.
    String newPrincipalToken =
        TokenUtils.getTokenFromSecrets(
            EXT.client(), EXT.getLocalPort(), oldClientId, oldSecret, realm);

    // Any call should initially fail with error indicating that rotation is needed.
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/principals/myprincipal",
                "Bearer " + newPrincipalToken)
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
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/principals/myprincipal/rotate",
                "Bearer " + newPrincipalToken)
            .post(Entity.json(""))) {
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals")
            .post(Entity.json(new CreatePrincipalRequest(principal, null)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Second attempt to create the same entity should fail with CONFLICT.
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals")
            .post(Entity.json(new CreatePrincipalRequest(principal, false)))) {
      assertThat(response).returns(Response.Status.CONFLICT.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    Principal fetchedPrincipal = null;
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals/myprincipal").get()) {
      assertThat(response).returns(200, Response::getStatus);
      fetchedPrincipal = response.readEntity(Principal.class);

      assertThat(fetchedPrincipal.getName()).isEqualTo("myprincipal");
      assertThat(fetchedPrincipal.getProperties()).isEqualTo(Map.of("custom-tag", "foo"));
      assertThat(fetchedPrincipal.getEntityVersion()).isGreaterThan(0);
    }

    // Should list the principal.
    try (Response response = newRequest("http://localhost:%d/api/management/v1/principals").get()) {
      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(Principals.class))
          .extracting(Principals::getPrincipals)
          .asInstanceOf(InstanceOfAssertFactories.list(Principal.class))
          .anySatisfy(pr -> assertThat(pr).returns("myprincipal", Principal::getName));
    }

    UpdatePrincipalRequest updateRequest =
        new UpdatePrincipalRequest(
            fetchedPrincipal.getEntityVersion(), Map.of("custom-tag", "updated"));

    // 200 successful update
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals/myprincipal")
            .put(Entity.json(updateRequest))) {
      assertThat(response).returns(200, Response::getStatus);
      fetchedPrincipal = response.readEntity(Principal.class);

      assertThat(fetchedPrincipal.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 200 GET after update should show new properties
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals/myprincipal").get()) {
      assertThat(response).returns(200, Response::getStatus);
      fetchedPrincipal = response.readEntity(Principal.class);

      assertThat(fetchedPrincipal.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 204 Successful delete
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals/myprincipal").delete()) {
      assertThat(response).returns(204, Response::getStatus);
    }

    // NOT_FOUND after deletion
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals/myprincipal").get()) {
      assertThat(response).returns(404, Response::getStatus);
    }

    // Empty list
    try (Response response = newRequest("http://localhost:%d/api/management/v1/principals").get()) {
      assertThat(response)
          .returns(200, Response::getStatus)
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals")
            .post(Entity.json(new CreatePrincipalRequest(principal, null)))) {
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

      try (Response response =
          newRequest("http://localhost:%d/api/management/v1/principals")
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
          newRequest("http://localhost:%d/api/management/v1/principals/" + invalidPrincipalName)
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
  public void testCreateListUpdateAndDeletePrincipalRole() {
    PrincipalRole principalRole =
        new PrincipalRole("myprincipalrole", Map.of("custom-tag", "foo"), 0L, 0L, 1);
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles")
            .post(Entity.json(new CreatePrincipalRoleRequest(principalRole)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Second attempt to create the same entity should fail with CONFLICT.
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles")
            .post(Entity.json(new CreatePrincipalRoleRequest(principalRole)))) {

      assertThat(response).returns(Response.Status.CONFLICT.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    PrincipalRole fetchedPrincipalRole = null;
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles/myprincipalrole").get()) {

      assertThat(response).returns(200, Response::getStatus);
      fetchedPrincipalRole = response.readEntity(PrincipalRole.class);

      assertThat(fetchedPrincipalRole.getName()).isEqualTo("myprincipalrole");
      assertThat(fetchedPrincipalRole.getProperties()).isEqualTo(Map.of("custom-tag", "foo"));
      assertThat(fetchedPrincipalRole.getEntityVersion()).isGreaterThan(0);
    }

    // Should list the principalRole.
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles").get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .extracting(PrincipalRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(PrincipalRole.class))
          .anySatisfy(pr -> assertThat(pr).returns("myprincipalrole", PrincipalRole::getName));
    }

    UpdatePrincipalRoleRequest updateRequest =
        new UpdatePrincipalRoleRequest(
            fetchedPrincipalRole.getEntityVersion(), Map.of("custom-tag", "updated"));

    // 200 successful update
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles/myprincipalrole")
            .put(Entity.json(updateRequest))) {
      assertThat(response).returns(200, Response::getStatus);
      fetchedPrincipalRole = response.readEntity(PrincipalRole.class);

      assertThat(fetchedPrincipalRole.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 200 GET after update should show new properties
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles/myprincipalrole").get()) {
      assertThat(response).returns(200, Response::getStatus);
      fetchedPrincipalRole = response.readEntity(PrincipalRole.class);

      assertThat(fetchedPrincipalRole.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 204 Successful delete
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles/myprincipalrole")
            .delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }

    // NOT_FOUND after deletion
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles/myprincipalrole").get()) {

      assertThat(response).returns(404, Response::getStatus);
    }

    // Empty list
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles").get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles")
            .post(Entity.json(new CreatePrincipalRoleRequest(principalRole)))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

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

      try (Response response =
          newRequest("http://localhost:%d/api/management/v1/principal-roles")
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
          newRequest(
                  "http://localhost:%d/api/management/v1/principal-roles/"
                      + invalidPrincipalRoleName)
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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(new CreateCatalogRequest(catalog)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    Catalog catalog2 =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("mycatalog2")
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://required/base/other_location"))
            .build();
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(new CreateCatalogRequest(catalog2)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    CatalogRole catalogRole =
        new CatalogRole("mycatalogrole", Map.of("custom-tag", "foo"), 0L, 0L, 1);
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog1/catalog-roles")
            .post(Entity.json(new CreateCatalogRoleRequest(catalogRole)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Second attempt to create the same entity should fail with CONFLICT.
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog1/catalog-roles")
            .post(Entity.json(new CreateCatalogRoleRequest(catalogRole)))) {

      assertThat(response).returns(Response.Status.CONFLICT.getStatusCode(), Response::getStatus);
    }

    // 200 successful GET after creation
    CatalogRole fetchedCatalogRole = null;
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/catalogs/mycatalog1/catalog-roles/mycatalogrole")
            .get()) {

      assertThat(response).returns(200, Response::getStatus);
      fetchedCatalogRole = response.readEntity(CatalogRole.class);

      assertThat(fetchedCatalogRole.getName()).isEqualTo("mycatalogrole");
      assertThat(fetchedCatalogRole.getProperties()).isEqualTo(Map.of("custom-tag", "foo"));
      assertThat(fetchedCatalogRole.getEntityVersion()).isGreaterThan(0);
    }

    // Should list the catalogRole.
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog1/catalog-roles")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .extracting(CatalogRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(CatalogRole.class))
          .anySatisfy(cr -> assertThat(cr).returns("mycatalogrole", CatalogRole::getName));
    }

    // Empty list if listing in catalog2
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog2/catalog-roles")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
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
        newRequest(
                "http://localhost:%d/api/management/v1/catalogs/mycatalog1/catalog-roles/mycatalogrole")
            .put(Entity.json(updateRequest))) {
      assertThat(response).returns(200, Response::getStatus);
      fetchedCatalogRole = response.readEntity(CatalogRole.class);

      assertThat(fetchedCatalogRole.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 200 GET after update should show new properties
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/catalogs/mycatalog1/catalog-roles/mycatalogrole")
            .get()) {
      assertThat(response).returns(200, Response::getStatus);
      fetchedCatalogRole = response.readEntity(CatalogRole.class);

      assertThat(fetchedCatalogRole.getProperties()).isEqualTo(Map.of("custom-tag", "updated"));
    }

    // 204 Successful delete
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/catalogs/mycatalog1/catalog-roles/mycatalogrole")
            .delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }

    // NOT_FOUND after deletion
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/catalogs/mycatalog1/catalog-roles/mycatalogrole")
            .get()) {

      assertThat(response).returns(404, Response::getStatus);
    }

    // Empty list
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog1/catalog-roles")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .extracting(CatalogRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(CatalogRole.class))
          .noneSatisfy(cr -> assertThat(cr).returns("mycatalogrole", CatalogRole::getName));
    }

    // 204 Successful delete mycatalog
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog1").delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }

    // 204 Successful delete mycatalog2
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog2").delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }
  }

  @Test
  public void testAssignListAndRevokePrincipalRoles() {
    // Create two Principals
    Principal principal1 = new Principal("myprincipal1");
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals")
            .post(Entity.json(new CreatePrincipalRequest(principal1, false)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    Principal principal2 = new Principal("myprincipal2");
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals")
            .post(Entity.json(new CreatePrincipalRequest(principal2, false)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // One PrincipalRole
    PrincipalRole principalRole = new PrincipalRole("myprincipalrole");
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles")
            .post(Entity.json(new CreatePrincipalRoleRequest(principalRole)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Assign the role to myprincipal1
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals/myprincipal1/principal-roles")
            .put(Entity.json(principalRole))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Should list myprincipalrole
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals/myprincipal1/principal-roles")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .extracting(PrincipalRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(PrincipalRole.class))
          .hasSize(1)
          .satisfiesExactly(
              pr -> assertThat(pr).returns("myprincipalrole", PrincipalRole::getName));
    }

    // Should list myprincipal1 if listing assignees of myprincipalrole
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/principal-roles/myprincipalrole/principals")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(Principals.class))
          .extracting(Principals::getPrincipals)
          .asInstanceOf(InstanceOfAssertFactories.list(Principal.class))
          .hasSize(1)
          .satisfiesExactly(pr -> assertThat(pr).returns("myprincipal1", Principal::getName));
    }

    // Empty list if listing in principal2
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals/myprincipal2/principal-roles")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .returns(List.of(), PrincipalRoles::getRoles);
    }

    // 204 Successful revoke
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/principals/myprincipal1/principal-roles/myprincipalrole")
            .delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }

    // Empty list
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals/myprincipal1/principal-roles")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .returns(List.of(), PrincipalRoles::getRoles);
    }
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/principal-roles/myprincipalrole/principals")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(Principals.class))
          .returns(List.of(), Principals::getPrincipals);
    }

    // 204 Successful delete myprincipal1
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals/myprincipal1").delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }

    // 204 Successful delete myprincipal2
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals/myprincipal2").delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }

    // 204 Successful delete myprincipalrole
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles/myprincipalrole")
            .delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }
  }

  @Test
  public void testAssignListAndRevokeCatalogRoles() {
    // Create two PrincipalRoles
    PrincipalRole principalRole1 = new PrincipalRole("mypr1");
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles")
            .post(Entity.json(new CreatePrincipalRoleRequest(principalRole1)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    PrincipalRole principalRole2 = new PrincipalRole("mypr2");
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles")
            .post(Entity.json(new CreatePrincipalRoleRequest(principalRole2)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(new CreateCatalogRequest(catalog)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    CatalogRole catalogRole = new CatalogRole("mycr");
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog/catalog-roles")
            .post(Entity.json(new CreateCatalogRoleRequest(catalogRole)))) {

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
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(new CreateCatalogRequest(otherCatalog)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    CatalogRole otherCatalogRole = new CatalogRole("myothercr");
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/othercatalog/catalog-roles")
            .post(Entity.json(new CreateCatalogRoleRequest(otherCatalogRole)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Assign both the roles to mypr1
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/principal-roles/mypr1/catalog-roles/mycatalog")
            .put(Entity.json(new GrantCatalogRoleRequest(catalogRole)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/principal-roles/mypr1/catalog-roles/othercatalog")
            .put(Entity.json(new GrantCatalogRoleRequest(otherCatalogRole)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    // Should list only mycr
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/principal-roles/mypr1/catalog-roles/mycatalog")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .extracting(CatalogRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(CatalogRole.class))
          .hasSize(1)
          .satisfiesExactly(cr -> assertThat(cr).returns("mycr", CatalogRole::getName));
    }

    // Should list mypr1 if listing assignees of mycr
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/catalogs/mycatalog/catalog-roles/mycr/principal-roles")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .extracting(PrincipalRoles::getRoles)
          .asInstanceOf(InstanceOfAssertFactories.list(PrincipalRole.class))
          .hasSize(1)
          .satisfiesExactly(pr -> assertThat(pr).returns("mypr1", PrincipalRole::getName));
    }

    // Empty list if listing in principalRole2
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/principal-roles/mypr2/catalog-roles/mycatalog")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .returns(List.of(), CatalogRoles::getRoles);
    }

    // 204 Successful revoke
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/principal-roles/mypr1/catalog-roles/mycatalog/mycr")
            .delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }

    // Empty list
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/principal-roles/mypr1/catalog-roles/mycatalog")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(CatalogRoles.class))
          .returns(List.of(), CatalogRoles::getRoles);
    }
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/catalogs/mycatalog/catalog-roles/mycr/principal-roles")
            .get()) {

      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(PrincipalRoles.class))
          .returns(List.of(), PrincipalRoles::getRoles);
    }

    // 204 Successful delete mypr1
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles/mypr1").delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }

    // 204 Successful delete mypr2
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principal-roles/mypr2").delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }

    // 204 Successful delete mycr
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog/catalog-roles/mycr")
            .delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }

    // 204 Successful delete mycatalog
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/mycatalog").delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }

    // 204 Successful delete myothercr
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/catalogs/othercatalog/catalog-roles/myothercr")
            .delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }

    // 204 Successful delete othercatalog
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs/othercatalog").delete()) {

      assertThat(response).returns(204, Response::getStatus);
    }
  }
}
