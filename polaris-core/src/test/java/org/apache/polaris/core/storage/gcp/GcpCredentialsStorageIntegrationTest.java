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
package org.apache.polaris.core.storage.gcp;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.storage.BaseStorageIntegrationTest;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class GcpCredentialsStorageIntegrationTest extends BaseStorageIntegrationTest {

  private final String gcsServiceKeyJsonFileLocation =
      System.getenv("GOOGLE_APPLICATION_CREDENTIALS");

  private static final String REFRESH_ENDPOINT = "get/credentials";

  private static final ObjectMapper MAPPER =
      JsonMapper.builder()
          .defaultPropertyInclusion(
              JsonInclude.Value.construct(
                  JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL))
          .build();

  private static List<LocationGrant> toGrants(
      Set<String> readLocations, Set<String> listLocations, Set<String> writeLocations) {
    List<LocationGrant> grants = new ArrayList<>();
    if (!readLocations.isEmpty()) {
      grants.add(new LocationGrant(readLocations, Set.of(PolarisStorageActions.READ)));
    }
    if (!listLocations.isEmpty()) {
      grants.add(new LocationGrant(listLocations, Set.of(PolarisStorageActions.LIST)));
    }
    if (!writeLocations.isEmpty()) {
      grants.add(new LocationGrant(writeLocations, Set.of(PolarisStorageActions.WRITE)));
    }
    return grants;
  }

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
    return createStorageClient(
        subscopedCredsForOperations(allowedReadLoc, allowedWriteLoc, allowListAction));
  }

  BlobInfo createStorageBlob(String bucket, String prefix, String fileName) {
    BlobId blobId = BlobId.of(bucket, prefix + fileName);
    return BlobInfo.newBuilder(blobId).build();
  }

  private Storage createStorageClient(StorageAccessConfig storageAccessConfig) {
    AccessToken accessToken =
        new AccessToken(
            storageAccessConfig.get(StorageAccessProperty.GCS_ACCESS_TOKEN),
            new Date(
                Long.parseLong(
                    storageAccessConfig.get(StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT))));
    return StorageOptions.newBuilder()
        .setCredentials(GoogleCredentials.create(accessToken))
        .build()
        .getService();
  }

  private StorageAccessConfig subscopedCredsForOperations(
      List<String> allowedReadLoc, List<String> allowedWriteLoc, boolean allowListAction)
      throws IOException {
    GcpStorageConfigurationInfo gcpConfig =
        GcpStorageConfigurationInfo.builder()
            .addAllAllowedLocations(allowedReadLoc)
            .addAllAllowedLocations(allowedWriteLoc)
            .build();
    GcpCredentialsStorageIntegration gcpCredsIntegration =
        new GcpCredentialsStorageIntegration(
            GoogleCredentials.getApplicationDefault(),
            ServiceOptions.getFromServiceLoader(HttpTransportFactory.class, NetHttpTransport::new),
            gcpConfig,
            EMPTY_REALM_CONFIG);
    return gcpCredsIntegration.getStorageAccessConfig(
        toGrants(
            new HashSet<>(allowedReadLoc),
            allowListAction ? new HashSet<>(allowedReadLoc) : Set.of(),
            new HashSet<>(allowedWriteLoc)),
        Optional.of(REFRESH_ENDPOINT),
        org.apache.polaris.core.storage.CredentialVendingContext.empty());
  }

  private JsonNode readResource(String name) throws IOException {
    try (InputStream in = GcpCredentialsStorageIntegrationTest.class.getResourceAsStream(name)) {
      return MAPPER.readTree(in);
    }
  }

  private Set<JsonNode> canonicalRules(JsonNode rules) {
    Set<JsonNode> canonical = new HashSet<>();
    for (JsonNode rule : rules.path("accessBoundaryRules")) {
      ObjectNode copy = rule.deepCopy();
      JsonNode condition = copy.path("availabilityCondition");
      JsonNode expression = condition.path("expression");
      if (expression.isTextual()) {
        String[] clauses = expression.asText().split(" \\|\\| ");
        Arrays.sort(clauses);
        ((ObjectNode) condition).put("expression", String.join(" || ", clauses));
      }
      canonical.add(copy);
    }
    return canonical;
  }

  @Test
  public void testGenerateAccessBoundary() throws IOException {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            Set.of("gs://bucket1/path/to/data"),
            Set.of("gs://bucket1/path/to/data"),
            Set.of("gs://bucket1/path/to/data"));
    assertThat(credentialAccessBoundary).isNotNull();
    JsonNode parsedRules = MAPPER.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules = readResource("gcp-testGenerateAccessBoundary.json");
    assertThat(canonicalRules(parsedRules)).isEqualTo(canonicalRules(refRules));
  }

  @Test
  public void testGenerateAccessBoundaryWithMultipleBuckets() throws IOException {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            Set.of(
                "gs://bucket1/normal/path/to/data",
                "gs://bucket1/awesome/path/to/data",
                "gs://bucket2/a/super/path/to/data"),
            Set.of(
                "gs://bucket1/normal/path/to/data",
                "gs://bucket1/awesome/path/to/data",
                "gs://bucket2/a/super/path/to/data"),
            Set.of("gs://bucket1/normal/path/to/data"));
    assertThat(credentialAccessBoundary).isNotNull();
    JsonNode parsedRules = MAPPER.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules = readResource("gcp-testGenerateAccessBoundaryWithMultipleBuckets.json");
    assertThat(canonicalRules(parsedRules)).isEqualTo(canonicalRules(refRules));
  }

  @Test
  public void testGenerateAccessBoundaryWithoutList() throws IOException {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            Set.of("gs://bucket1/path/to/data", "gs://bucket1/another/path/to/data"),
            Set.of(),
            Set.of("gs://bucket1/path/to/data"));
    assertThat(credentialAccessBoundary).isNotNull();
    JsonNode parsedRules = MAPPER.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules = readResource("gcp-testGenerateAccessBoundaryWithoutList.json");
    assertThat(canonicalRules(parsedRules)).isEqualTo(canonicalRules(refRules));
  }

  @Test
  public void testGenerateAccessBoundaryWithoutWrites() throws IOException {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            Set.of("gs://bucket1/normal/path/to/data", "gs://bucket1/awesome/path/to/data"),
            Set.of(),
            Set.of());
    assertThat(credentialAccessBoundary).isNotNull();
    JsonNode parsedRules = MAPPER.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules = readResource("gcp-testGenerateAccessBoundaryWithoutWrites.json");
    assertThat(canonicalRules(parsedRules)).isEqualTo(canonicalRules(refRules));
  }

  @Test
  public void testGenerateAccessBoundaryQuotesCelLiteralCharacters() {
    String path = "a'b\"c\\d";
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            Set.of("gs://bucket1/" + path),
            Set.of("gs://bucket1/" + path),
            Set.of("gs://bucket1/" + path));

    JsonNode parsedRules = MAPPER.convertValue(credentialAccessBoundary, JsonNode.class);
    assertThat(parsedRules.path("accessBoundaryRules")).hasSize(2);

    assertThat(expressionAt(parsedRules, 0))
        .isEqualTo(
            "resource.name.startsWith('projects/_/buckets/bucket1/objects/"
                + "a\\'b\\\"c\\\\d"
                + "') || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('"
                + "a\\'b\\\"c\\\\d"
                + "')");
    assertThat(expressionAt(parsedRules, 1))
        .isEqualTo(
            "resource.name.startsWith('projects/_/buckets/bucket1/objects/a\\'b\\\"c\\\\d')");
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      quoteCharacter = '`',
      emptyValue = "",
      value = {
        "``|``",
        "plain|plain",
        "'|\\'",
        "\\\"|\\\\\\\"",
        "\\\\|\\\\\\\\",
        "a'b\\\\c|a\\'b\\\\\\\\c",
        "a\tb|a\\tb",
      })
  public void testQuoteForCelString(String input, String expectedOutput) {
    assertThat(GcpCredentialsStorageIntegration.escapeCelLiteral(input)).isEqualTo(expectedOutput);
  }

  @ParameterizedTest
  @MethodSource("handledControlCharacterCases")
  public void testQuoteForCelStringEscapesHandledControlCharacters(
      String input, String expectedOutput) {
    assertThat(GcpCredentialsStorageIntegration.escapeCelLiteral(input)).isEqualTo(expectedOutput);
  }

  private static Stream<Arguments> handledControlCharacterCases() {
    return Stream.of(
        Arguments.of("\b", "\\b"),
        Arguments.of("a\bb", "a\\bb"),
        Arguments.of("\f", "\\f"),
        Arguments.of("a\fb", "a\\fb"),
        Arguments.of("\n", "\\n"),
        Arguments.of("a\nb", "a\\nb"),
        Arguments.of("\r", "\\r"),
        Arguments.of("a\rb", "a\\rb"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"\uD83D\uDE00", "prefix-\uD834\uDD1E-suffix"})
  public void testQuoteForCelStringPreservesValidSurrogatePairs(String input) {
    assertThat(GcpCredentialsStorageIntegration.escapeCelLiteral(input)).isEqualTo(input);
  }

  @ParameterizedTest
  @MethodSource("unpairedSurrogateCases")
  public void testQuoteForCelStringRejectsUnpairedSurrogates(String input) {
    Assertions.assertThatThrownBy(() -> GcpCredentialsStorageIntegration.escapeCelLiteral(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported unpaired surrogate");
  }

  private static Stream<String> unpairedSurrogateCases() {
    return Stream.of(
        String.valueOf((char) 0xD83D),
        String.valueOf((char) 0xDE00),
        new String(new char[] {(char) 0xD83D, 'a'}),
        new String(new char[] {'a', (char) 0xDE00}));
  }

  @ParameterizedTest
  @ValueSource(ints = {0x0000, 0x0001, 0x0007, 0x000B, 0x001F, 0x007F})
  public void testQuoteForCelStringRejectsUnsupportedControlCharacters(int codePoint) {
    String input = "a" + (char) codePoint + "b";

    Assertions.assertThatThrownBy(() -> GcpCredentialsStorageIntegration.escapeCelLiteral(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported control character");
  }

  @Test
  public void testGenerateAccessBoundaryRejectsUnsupportedCelLiteralCharacters() {
    Assertions.assertThatThrownBy(
            () ->
                GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
                    Set.of("gs://bucket1/a\u001fb"), Set.of("gs://bucket1/a\u001fb"), Set.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported control character");
  }

  @Test
  public void testGenerateAccessBoundaryPreservesLiteralQuestionMarksInPath() {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            Set.of("gs://bucket1/path/to/data?with?question"),
            Set.of("gs://bucket1/path/to/data?with?question"),
            Set.of());

    JsonNode parsedRules = MAPPER.convertValue(credentialAccessBoundary, JsonNode.class);

    assertThat(
            parsedRules
                .path("accessBoundaryRules")
                .get(0)
                .path("availabilityCondition")
                .path("expression")
                .asText())
        .contains("projects/_/buckets/bucket1/objects/path/to/data?with?question")
        .contains("startsWith('path/to/data?with?question')");
  }

  @Test
  public void testRefreshCredentialsEndpointIsReturned() throws IOException {
    Assumptions.assumeThat(gcsServiceKeyJsonFileLocation)
        .describedAs("Environment variable GOOGLE_APPLICATION_CREDENTIALS not exits")
        .isNotNull()
        .isNotEmpty();

    StorageAccessConfig storageAccessConfig =
        subscopedCredsForOperations(
            List.of("gs://bucket1/path/to/data"), List.of("gs://bucket1/path/to/data"), true);
    assertThat(storageAccessConfig.get(StorageAccessProperty.GCS_REFRESH_CREDENTIALS_ENDPOINT))
        .isEqualTo(REFRESH_ENDPOINT);
  }

  @Test
  public void testImpersonation() throws IOException {
    String serviceAccount = "test-sa@project.iam.gserviceaccount.com";
    GcpStorageConfigurationInfo config =
        GcpStorageConfigurationInfo.builder()
            .addAllAllowedLocations(List.of("gs://bucket/path"))
            .gcpServiceAccount(serviceAccount)
            .build();

    IamCredentialsClient mockIamClient = Mockito.mock(IamCredentialsClient.class);
    GenerateAccessTokenResponse mockResponse =
        GenerateAccessTokenResponse.newBuilder()
            .setAccessToken("impersonated-token")
            .setExpireTime(
                Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000 + 3600).build())
            .build();
    Mockito.when(mockIamClient.generateAccessToken(Mockito.any(GenerateAccessTokenRequest.class)))
        .thenReturn(mockResponse);

    GoogleCredentials mockCreds = Mockito.mock(GoogleCredentials.class);
    Mockito.when(mockCreds.createScoped(Mockito.any(String.class))).thenReturn(mockCreds);

    GcpCredentialOps testOps =
        new GcpCredentialOps() {
          @Override
          public IamCredentialsClient createIamCredentialsClient(GoogleCredentials credentials) {
            return mockIamClient;
          }

          @Override
          public AccessToken refreshAccessToken(DownscopedCredentials credentials) {
            return new AccessToken("downscoped-token", new Date());
          }
        };
    GcpCredentialsStorageIntegration integration =
        new GcpCredentialsStorageIntegration(
            mockCreds,
            ServiceOptions.getFromServiceLoader(HttpTransportFactory.class, NetHttpTransport::new),
            config,
            EMPTY_REALM_CONFIG,
            testOps);

    integration.getStorageAccessConfig(
        toGrants(
            Set.of("gs://bucket/path"), Set.of("gs://bucket/path"), Set.of("gs://bucket/path")),
        Optional.empty(),
        org.apache.polaris.core.storage.CredentialVendingContext.empty());

    Mockito.verify(mockIamClient)
        .generateAccessToken(
            Mockito.argThat(
                request ->
                    request
                            .getName()
                            .equals(
                                GcpCredentialsStorageIntegration.SERVICE_ACCOUNT_PREFIX
                                    + serviceAccount)
                        && request.getScopeCount() > 0
                        && request
                            .getScope(0)
                            .equals(GcpCredentialsStorageIntegration.IMPERSONATION_SCOPE)));
  }

  private static String expressionAt(JsonNode parsedRules, int ruleIndex) {
    return parsedRules
        .path("accessBoundaryRules")
        .path(ruleIndex)
        .path("availabilityCondition")
        .path("expression")
        .asText();
  }

  // ── HNS folder-rule tests (predicate-driven) ──────────────────────────────────────

  @Test
  public void testGenerateAccessBoundaryHnsSingleBucketEmitsFolderRule() throws IOException {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of("gs://bucket1/path/to/data"),
            Set.of("gs://bucket1/path/to/data"),
            bucket -> true);
    assertThat(credentialAccessBoundary).isNotNull();
    // read + write + folder
    assertThat(credentialAccessBoundary.getAccessBoundaryRules()).hasSize(3);
    JsonNode parsedRules = MAPPER.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules = readResource("gcp-testGenerateAccessBoundaryHnsEnabled.json");
    assertThat(canonicalRules(parsedRules)).isEqualTo(canonicalRules(refRules));
  }

  @Test
  public void testGenerateAccessBoundaryHnsMultipleBucketsPartialWrites() throws IOException {
    // Two buckets read, one written. Only the written bucket gets a folder rule.
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of("gs://bucket1/normal/path/to/data", "gs://bucket1/awesome/path/to/data"),
            Set.of("gs://bucket1/normal/path/to/data"),
            bucket -> true);
    assertThat(credentialAccessBoundary).isNotNull();
    JsonNode parsedRules = MAPPER.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules = readResource("gcp-testGenerateAccessBoundaryHnsWithMultipleBuckets.json");
    assertThat(canonicalRules(parsedRules)).isEqualTo(canonicalRules(refRules));
  }

  @Test
  public void testGenerateAccessBoundaryHnsWithoutWritesNoFolderRules() throws IOException {
    // Read-only locations: no write rule, no folder rule, even with HNS predicate=true.
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true, Set.of("gs://bucket1/path/to/data"), Set.of(), bucket -> true);
    assertThat(credentialAccessBoundary).isNotNull();
    assertThat(credentialAccessBoundary.getAccessBoundaryRules()).hasSize(1);
    JsonNode parsedRules = MAPPER.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules = readResource("gcp-testGenerateAccessBoundaryHnsWithoutWrites.json");
    assertThat(canonicalRules(parsedRules)).isEqualTo(canonicalRules(refRules));
  }

  @Test
  public void testGenerateAccessBoundaryHnsSeparateMetadataAndDataBuckets() throws IOException {
    // Metadata and data on different buckets. Both are HNS — expect folder rule on each.
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of("gs://metadata-bucket/ns/table/metadata/", "gs://data-bucket/ns/table/data/"),
            Set.of("gs://metadata-bucket/ns/table/metadata/", "gs://data-bucket/ns/table/data/"),
            bucket -> true);
    assertThat(credentialAccessBoundary).isNotNull();
    // 2 reads + 2 writes + 2 folder rules
    assertThat(credentialAccessBoundary.getAccessBoundaryRules()).hasSize(6);
  }

  @Test
  public void testGenerateAccessBoundaryHnsPredicateGating() {
    // Mixed catalog: bucket1 is HNS, bucket2 is not. Only bucket1 gets a folder rule.
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of("gs://bucket1/data", "gs://bucket2/data"),
            Set.of("gs://bucket1/data", "gs://bucket2/data"),
            bucket -> bucket.equals("bucket1"));
    assertThat(credentialAccessBoundary).isNotNull();
    // 2 reads + 2 writes + 1 folder rule (only bucket1)
    assertThat(credentialAccessBoundary.getAccessBoundaryRules()).hasSize(5);
    long folderRuleCount =
        credentialAccessBoundary.getAccessBoundaryRules().stream()
            .filter(
                rule ->
                    rule.getAvailablePermissions().stream()
                        .anyMatch(p -> p.startsWith("storage.folders.")))
            .count();
    assertThat(folderRuleCount).isEqualTo(1);
  }

  @Test
  public void testGenerateAccessBoundaryFolderRuleUsesNarrowPermissions() {
    // Verify we grant only folders.create + folders.get, not roles/storage.folderAdmin.
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true, Set.of("gs://b/p"), Set.of("gs://b/p"), bucket -> true);
    CredentialAccessBoundary.AccessBoundaryRule folderRule =
        credentialAccessBoundary.getAccessBoundaryRules().stream()
            .filter(
                rule ->
                    rule.getAvailablePermissions().stream()
                        .anyMatch(p -> p.startsWith("storage.folders.")))
            .findFirst()
            .orElseThrow();
    assertThat(folderRule.getAvailablePermissions())
        .containsExactlyInAnyOrder("storage.folders.create", "storage.folders.get");
    // Scope is the folder hierarchy, never the managedFolders/ resource.
    assertThat(folderRule.getAvailabilityCondition().getExpression())
        .contains("folders/p")
        .doesNotContain("managedFolders");
  }

  @Test
  public void testGenerateAccessBoundaryNonHnsOmitsFolderRule() {
    // With predicate=false (or default 3-arg signature), no folder rule should be emitted.
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true, Set.of("gs://b/p"), Set.of("gs://b/p"), bucket -> false);
    assertThat(credentialAccessBoundary.getAccessBoundaryRules()).hasSize(2);
    assertThat(
            credentialAccessBoundary.getAccessBoundaryRules().stream()
                .flatMap(rule -> rule.getAvailablePermissions().stream()))
        .noneMatch(p -> p.startsWith("storage.folders."));
  }
}
