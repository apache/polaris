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
package org.apache.polaris.service.storage.gcp;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.gcp.GcpCredentialsStorageIntegration;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GcpCredentialsStorageIntegrationTest {

  private final String gcsServiceKeyJsonFileLocation =
      System.getenv("GOOGLE_APPLICATION_CREDENTIALS");

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSubscope(boolean allowedListAction) throws Exception {
    Assumptions.assumeThat(gcsServiceKeyJsonFileLocation)
        .describedAs("Environment variable GOOGLE_APPLICATION_CREDENTIALS not exits")
        .isNotNull()
        .isNotEmpty();

    List<String> allowedRead =
        Arrays.asList(
            "gs://sfc-dev1-regtest/polaris-test/subscoped-test/read1/",
            "gs://sfc-dev1-regtest/polaris-test/subscoped-test/read2/");
    List<String> allowedWrite =
        Arrays.asList(
            "gs://sfc-dev1-regtest/polaris-test/subscoped-test/write1/",
            "gs://sfc-dev1-regtest/polaris-test/subscoped-test/write2/");
    Storage storageClient = setupStorageClient(allowedRead, allowedWrite, allowedListAction);
    BlobInfo blobInfoGoodWrite =
        createStorageBlob("sfc-dev1-regtest", "polaris-test/subscoped-test/write1/", "file.txt");
    BlobInfo blobInfoBad =
        createStorageBlob("sfc-dev1-regtest", "polaris-test/subscoped-test/write3/", "file.txt");
    BlobInfo blobInfoGoodRead =
        createStorageBlob("sfc-dev1-regtest", "polaris-test/subscoped-test/read1/", "file.txt");
    final byte[] fileContent = "hello-polaris".getBytes(UTF_8);
    // GOOD WRITE
    Assertions.assertThatNoException()
        .isThrownBy(() -> storageClient.create(blobInfoGoodWrite, fileContent));

    // BAD WROTE
    Assertions.assertThatThrownBy(() -> storageClient.create(blobInfoBad, fileContent))
        .isInstanceOf(StorageException.class);

    Assertions.assertThatNoException()
        .isThrownBy(() -> storageClient.get(blobInfoGoodRead.getBlobId()));
    Assertions.assertThatThrownBy(() -> storageClient.get(blobInfoBad.getBlobId()))
        .isInstanceOf(StorageException.class);

    // LIST
    if (allowedListAction) {
      Assertions.assertThatNoException()
          .isThrownBy(
              () ->
                  storageClient.list(
                      "sfc-dev1-regtest",
                      Storage.BlobListOption.prefix("polaris-test/subscoped-test/read1/")));
    } else {
      Assertions.assertThatThrownBy(
              () ->
                  storageClient.list(
                      "sfc-dev1-regtest",
                      Storage.BlobListOption.prefix("polaris-test/subscoped-test/read1/")))
          .isInstanceOf(StorageException.class);
    }
    // DELETE
    List<String> allowedWrite2 =
        Arrays.asList(
            "gs://sfc-dev1-regtest/polaris-test/subscoped-test/write2/",
            "gs://sfc-dev1-regtest/polaris-test/subscoped-test/write3/");
    Storage clientForDelete = setupStorageClient(List.of(), allowedWrite2, allowedListAction);

    // can not delete because it is not in allowed write path for this client
    Assertions.assertThatThrownBy(() -> clientForDelete.delete(blobInfoGoodWrite.getBlobId()))
        .isInstanceOf(StorageException.class);

    // good to delete allowed location
    Assertions.assertThatNoException()
        .isThrownBy(() -> storageClient.delete(blobInfoGoodWrite.getBlobId()));
  }

  private Storage setupStorageClient(
      List<String> allowedReadLoc, List<String> allowedWriteLoc, boolean allowListAction)
      throws IOException {
    Map<StorageAccessProperty, String> credsMap =
        subscopedCredsForOperations(allowedReadLoc, allowedWriteLoc, allowListAction);
    return createStorageClient(credsMap);
  }

  BlobInfo createStorageBlob(String bucket, String prefix, String fileName) {
    BlobId blobId = BlobId.of(bucket, prefix + fileName);
    return BlobInfo.newBuilder(blobId).build();
  }

  private Storage createStorageClient(Map<StorageAccessProperty, String> credsMap) {
    AccessToken accessToken =
        new AccessToken(
            credsMap.get(StorageAccessProperty.GCS_ACCESS_TOKEN),
            new Date(
                Long.parseLong(credsMap.get(StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT))));
    return StorageOptions.newBuilder()
        .setCredentials(GoogleCredentials.create(accessToken))
        .build()
        .getService();
  }

  private Map<StorageAccessProperty, String> subscopedCredsForOperations(
      List<String> allowedReadLoc, List<String> allowedWriteLoc, boolean allowListAction)
      throws IOException {
    List<String> allowedLoc = new ArrayList<>();
    allowedLoc.addAll(allowedReadLoc);
    allowedLoc.addAll(allowedWriteLoc);
    GcpStorageConfigurationInfo gcpConfig = new GcpStorageConfigurationInfo(allowedLoc);
    GcpCredentialsStorageIntegration gcpCredsIntegration =
        new GcpCredentialsStorageIntegration(
            GoogleCredentials.getApplicationDefault(),
            ServiceOptions.getFromServiceLoader(HttpTransportFactory.class, NetHttpTransport::new));
    EnumMap<StorageAccessProperty, String> credsMap =
        gcpCredsIntegration.getSubscopedCreds(
            new PolarisDefaultDiagServiceImpl(),
            gcpConfig,
            allowListAction,
            new HashSet<>(allowedReadLoc),
            new HashSet<>(allowedWriteLoc));
    return credsMap;
  }

  @Test
  public void testGenerateAccessBoundary() throws IOException {
    GcpCredentialsStorageIntegration integration =
        new GcpCredentialsStorageIntegration(
            GoogleCredentials.newBuilder()
                .setAccessToken(
                    new AccessToken(
                        "my_token",
                        new Date(Instant.now().plus(10, ChronoUnit.MINUTES).toEpochMilli())))
                .build(),
            new HttpTransportOptions.DefaultHttpTransportFactory());
    CredentialAccessBoundary credentialAccessBoundary =
        integration.generateAccessBoundaryRules(
            true, Set.of("gs://bucket1/path/to/data"), Set.of("gs://bucket1/path/to/data"));
    assertThat(credentialAccessBoundary).isNotNull();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules =
        mapper.readTree(
            GcpCredentialsStorageIntegrationTest.class.getResource(
                "gcp-testGenerateAccessBoundary.json"));
    assertThat(parsedRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(refRules);
  }

  @Test
  public void testGenerateAccessBoundaryWithMultipleBuckets() throws IOException {
    GcpCredentialsStorageIntegration integration =
        new GcpCredentialsStorageIntegration(
            GoogleCredentials.newBuilder()
                .setAccessToken(
                    new AccessToken(
                        "my_token",
                        new Date(Instant.now().plus(10, ChronoUnit.MINUTES).toEpochMilli())))
                .build(),
            new HttpTransportOptions.DefaultHttpTransportFactory());
    CredentialAccessBoundary credentialAccessBoundary =
        integration.generateAccessBoundaryRules(
            true,
            Set.of(
                "gs://bucket1/normal/path/to/data",
                "gs://bucket1/awesome/path/to/data",
                "gs://bucket2/a/super/path/to/data"),
            Set.of("gs://bucket1/normal/path/to/data"));
    assertThat(credentialAccessBoundary).isNotNull();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules =
        mapper.readTree(
            GcpCredentialsStorageIntegrationTest.class.getResource(
                "gcp-testGenerateAccessBoundaryWithMultipleBuckets.json"));
    assertThat(parsedRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(refRules);
  }

  @Test
  public void testGenerateAccessBoundaryWithoutList() throws IOException {
    GcpCredentialsStorageIntegration integration =
        new GcpCredentialsStorageIntegration(
            GoogleCredentials.newBuilder()
                .setAccessToken(
                    new AccessToken(
                        "my_token",
                        new Date(Instant.now().plus(10, ChronoUnit.MINUTES).toEpochMilli())))
                .build(),
            new HttpTransportOptions.DefaultHttpTransportFactory());
    CredentialAccessBoundary credentialAccessBoundary =
        integration.generateAccessBoundaryRules(
            false,
            Set.of("gs://bucket1/path/to/data", "gs://bucket1/another/path/to/data"),
            Set.of("gs://bucket1/path/to/data"));
    assertThat(credentialAccessBoundary).isNotNull();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules =
        mapper.readTree(
            GcpCredentialsStorageIntegrationTest.class.getResource(
                "gcp-testGenerateAccessBoundaryWithoutList.json"));
    assertThat(parsedRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(refRules);
  }

  @Test
  public void testGenerateAccessBoundaryWithoutWrites() throws IOException {
    GcpCredentialsStorageIntegration integration =
        new GcpCredentialsStorageIntegration(
            GoogleCredentials.newBuilder()
                .setAccessToken(
                    new AccessToken(
                        "my_token",
                        new Date(Instant.now().plus(10, ChronoUnit.MINUTES).toEpochMilli())))
                .build(),
            new HttpTransportOptions.DefaultHttpTransportFactory());
    CredentialAccessBoundary credentialAccessBoundary =
        integration.generateAccessBoundaryRules(
            false,
            Set.of("gs://bucket1/normal/path/to/data", "gs://bucket1/awesome/path/to/data"),
            Set.of());
    assertThat(credentialAccessBoundary).isNotNull();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules =
        mapper.readTree(
            GcpCredentialsStorageIntegrationTest.class.getResource(
                "gcp-testGenerateAccessBoundaryWithoutWrites.json"));
    assertThat(parsedRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(refRules);
  }

  /**
   * Custom comparator as ObjectNodes are compared by field indexes as opposed to field names. They
   * also don't equate a field that is present and set to null with a field that is omitted
   *
   * @param on1
   * @param on2
   * @return
   */
  private boolean recursiveEquals(ContainerNode<?> on1, ContainerNode<?> on2) {
    Set<String> fieldNames = new HashSet<>();
    on1.fieldNames().forEachRemaining(fieldNames::add);
    on2.fieldNames().forEachRemaining(fieldNames::add);
    for (String fieldName : fieldNames) {
      if ((!on1.has(fieldName) || !on2.has(fieldName))) {
        if (isNotNull(on1.get(fieldName)) || isNotNull(on2.get(fieldName))) {
          return false;
        }
      } else {
        JsonNode fieldValue = on1.get(fieldName);
        JsonNode fieldValue2 = on2.get(fieldName);
        if (fieldValue.isContainerNode()) {
          if (!fieldValue2.isContainerNode()
              || !recursiveEquals((ContainerNode<?>) fieldValue, (ContainerNode<?>) fieldValue2)) {
            return false;
          }
        } else if (!fieldValue.equals(fieldValue2)) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean isNotNull(JsonNode node) {
    return node != null && !node.isNull();
  }
}
