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
package org.apache.polaris.service.it;

import static io.restassured.RestAssured.given;
import static org.apache.polaris.core.auth.PolarisAuthorizableOperation.LOAD_TABLE;
import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.specification.RequestSpecification;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.LoadContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PathSegment;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.auth.oidc.TestPolarisAuthorizer;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Checks how a {@code referenced-by} value sent by the Iceberg REST client (and by a raw HTTP
 * request built the same way) reaches the server: the raw query string on the wire, and the chain
 * the authorizer sees in {@link SingleTargetAuthorizationIntent#referencedBy()}.
 *
 * <p>The wire format follows {@code RESTUtil.encodeReferencedBy}: every part of a view identifier
 * (namespace levels and name) is encoded with {@link RESTUtil#encodePathSegment}, the parts are
 * joined by the encoded namespace separator ({@code %1F}), and the entries of the chain are joined
 * by a literal comma. The server reads the value from the raw query string, because the REST
 * framework only hands out a decoded value, in which a comma in a name cannot be told from the
 * delimiter between entries.
 */
@QuarkusTest
@TestProfile(Profiles.RestCatalogFileIntegrationProfile.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReferencedByWireIntegrationTest {

  private static final String ENCODED_SEPARATOR = "%1F";

  /** Records the referenced-by chain of every LOAD_TABLE intent that reaches the authorizer. */
  private static final class RecordingAuthorizer extends TestPolarisAuthorizer {
    private final List<List<List<String>>> chains = new CopyOnWriteArrayList<>();

    void clearChains() {
      chains.clear();
    }

    List<List<List<String>>> chains() {
      return List.copyOf(chains);
    }

    @Override
    @NonNull
    public AuthorizationDecision authorize(
        @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
      request.intents().stream()
          .filter(SingleTargetAuthorizationIntent.class::isInstance)
          .map(SingleTargetAuthorizationIntent.class::cast)
          .filter(intent -> intent.operation() == LOAD_TABLE)
          .forEach(intent ->
              chains.add(
                  intent.referencedBy().stream()
                      .map(ReferencedByWireIntegrationTest::names)
                      .toList()));
      return super.authorize(authzState, request);
    }
  }

  private static final RecordingAuthorizer authorizer = new RecordingAuthorizer();

  private static final TableIdentifier TARGET = TableIdentifier.of("ns1", "target_table");

  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static String adminToken;

  private String catalogName;
  private String principalToken;
  private RESTCatalog restCatalog;

  @BeforeAll
  static void installMocks() {
    QuarkusMock.installMockForType(authorizer, PolarisAuthorizer.class);
  }

  @BeforeAll
  void setUp(PolarisApiEndpoints apiEndpoints, ClientPrincipal admin, @TempDir Path tempDir)
      throws Exception {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    adminToken = client.obtainToken(admin);
    ManagementApi managementApi = client.managementApi(adminToken);

    URI tempDirUri = IntegrationTestsHelper.getTemporaryDirectory(tempDir);
    String baseLocation = tempDirUri.toString().replaceAll("/+$", "");

    catalogName = client.newEntityName("referenced_by_wire");
    StorageConfigInfo storageConfig =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(baseLocation, "file://"))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(
                CatalogProperties.builder(baseLocation + "/path/to/data")
                    .addProperty("polaris.config.allow.unstructured.table.location", "true")
                    .addProperty("polaris.config.allow.external.metadata.file.location", "true")
                    .build())
            .setStorageConfigInfo(storageConfig)
            .build();
    managementApi.createCatalog(catalog);

    String principalName = client.newEntityName("referenced-by-user");
    String principalRoleName = client.newEntityName("referenced-by-admin");
    PrincipalWithCredentials principal =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);
    managementApi.makeAdmin(principalRoleName, catalog);
    principalToken =
        client.obtainToken(
            new ClientPrincipal(
                principal.getPrincipal().getName(),
                new ClientCredentials(
                    principal.getCredentials().getClientId(),
                    principal.getCredentials().getClientSecret())));

    restCatalog =
        IcebergHelper.restCatalog(
            endpoints,
            catalogName,
            ImmutableMap.of(
                org.apache.iceberg.CatalogProperties.FILE_IO_IMPL, ResolvingFileIO.class.getName()),
            principalToken);
    restCatalog.createNamespace(TARGET.namespace());
    restCatalog
        .buildTable(TARGET, new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
        .create();
  }

  @AfterAll
  void tearDown() throws Exception {
    try {
      if (restCatalog != null) {
        restCatalog.close();
      }
    } finally {
      client.cleanUp(adminToken);
      client.close();
    }
  }

  @BeforeEach
  void resetRecordings() {
    authorizer.clearChains();
    ReferencedByQueryCaptureFilter.reset();
  }

  // ---------------------------------------------------------------------------------------------
  // Real Iceberg REST client: RESTCatalog.loadTable(TableIdentifier, LoadContext)
  // ---------------------------------------------------------------------------------------------

  @Test
  void clientMultiLevelNamespace() {
    List<TableIdentifier> chain =
        List.of(TableIdentifier.of(Namespace.of("db", "schema"), "view_one"));

    restCatalog.loadTable(TARGET, LoadContext.builder().referencedBy(chain).build());

    // The client must not double-encode the separator: a single %1F, never %251F.
    var captured = singleCapture();
    assertThat(rawReferencedBy(captured))
        .isEqualTo("db" + ENCODED_SEPARATOR + "schema" + ENCODED_SEPARATOR + "view_one");
    assertThat(rawReferencedBy(captured)).doesNotContain("%25");
    assertThat(rawReferencedBy(captured)).isEqualTo(wire(chain));
    assertChainsSeenByAuthorizer(chain);
  }

  @Test
  void clientTwoViewChain() {
    List<TableIdentifier> chain =
        List.of(
            TableIdentifier.of(Namespace.of("db", "schema"), "outer_view"),
            TableIdentifier.of(Namespace.of("other"), "inner_view"));

    restCatalog.loadTable(TARGET, LoadContext.builder().referencedBy(chain).build());

    assertThat(rawReferencedBy(singleCapture())).isEqualTo(wire(chain));
    assertChainsSeenByAuthorizer(chain);
  }

  /**
   * A view name containing a comma: the client encodes it as {@code %2C}, a comma between entries
   * stays a literal comma.
   */
  @Test
  void clientCommaInViewName() {
    List<TableIdentifier> chain =
        List.of(
            TableIdentifier.of(Namespace.of("ns"), "view,with,comma"),
            TableIdentifier.of(Namespace.of("a,b"), "other"));

    restCatalog.loadTable(TARGET, LoadContext.builder().referencedBy(chain).build());

    assertThat(rawReferencedBy(singleCapture()))
        .isEqualTo(
            "ns" + ENCODED_SEPARATOR + "view%2Cwith%2Ccomma,a%2Cb" + ENCODED_SEPARATOR + "other");
    assertChainsSeenByAuthorizer(chain);
  }

  @ParameterizedTest
  @MethodSource("specialCharacters")
  void clientSpecialCharacters(String special) {
    List<TableIdentifier> chain = chainWithSpecialCharacters(special);

    restCatalog.loadTable(TARGET, LoadContext.builder().referencedBy(chain).build());

    assertThat(rawReferencedBy(singleCapture())).isEqualTo(wire(chain));
    assertChainsSeenByAuthorizer(chain);
  }

  // ---------------------------------------------------------------------------------------------
  // Raw HTTP: the exact wire value built with RESTUtil.encodeReferencedBy semantics
  // ---------------------------------------------------------------------------------------------

  @Test
  void rawRequestMultiLevelNamespace() {
    List<TableIdentifier> chain =
        List.of(TableIdentifier.of(Namespace.of("db", "schema"), "view_one"));

    assertThat(loadTableRaw(wire(chain))).isEqualTo(200);

    assertThat(singleCapture().rawQuery()).isEqualTo("referenced-by=db%1Fschema%1Fview_one");
    assertChainsSeenByAuthorizer(chain);
  }

  @Test
  void rawRequestTwoViewChain() {
    List<TableIdentifier> chain =
        List.of(
            TableIdentifier.of(Namespace.of("db", "schema"), "outer_view"),
            TableIdentifier.of(Namespace.of("other"), "inner_view"));

    assertThat(loadTableRaw(wire(chain))).isEqualTo(200);

    assertChainsSeenByAuthorizer(chain);
  }

  @Test
  void rawRequestCommaInViewName() {
    List<TableIdentifier> chain =
        List.of(TableIdentifier.of(Namespace.of("ns"), "view,with,comma"));
    assertThat(wire(chain)).isEqualTo("ns" + ENCODED_SEPARATOR + "view%2Cwith%2Ccomma");

    assertThat(loadTableRaw(wire(chain))).isEqualTo(200);

    assertChainsSeenByAuthorizer(chain);
  }

  @ParameterizedTest
  @MethodSource("specialCharacters")
  void rawRequestSpecialCharacters(String special) {
    List<TableIdentifier> chain = chainWithSpecialCharacters(special);

    assertThat(loadTableRaw(wire(chain))).isEqualTo(200);

    assertChainsSeenByAuthorizer(chain);
  }

  /** A bare {@code +} is a plus, not a space, and the case of the hex digits does not matter. */
  @Test
  void rawRequestPlainPercentDecoding() {
    assertThat(loadTableRaw("ns%1fa+b%2bc%20d")).isEqualTo(200);

    assertChainsSeenByAuthorizer(List.of(TableIdentifier.of("ns", "a+b+c d")));
  }

  @Test
  void rawRequestWithoutReferencedByHasNoChain() {
    assertThat(loadTableRawQuery("snapshots=all")).isEqualTo(200);
    assertChainsSeenByAuthorizer(List.of());

    authorizer.clearChains();
    assertThat(loadTableRawQuery("referenced-by=")).isEqualTo(200);
    assertChainsSeenByAuthorizer(List.of());

    authorizer.clearChains();
    assertThat(loadTableRawQuery(null)).isEqualTo(200);
    assertChainsSeenByAuthorizer(List.of());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "malformed",
        "n%1Fv,,m%1Fw",
        "n%1Fv,",
        "n%1F",
        "%1Fv",
        "n%1F%1Fv",
        // malformed UTF-8
        "n%1Fv%C3",
        "n%1Fv%FF",
      })
  void rawRequestMalformedChainIsRejected(String rawValue) {
    assertThat(loadTableRaw(rawValue)).isEqualTo(400);

    // nothing reached the authorizer: the request is rejected before authorization
    assertThat(authorizer.chains()).isEmpty();
  }

  @Test
  void rawRequestRepeatedReferencedByIsRejected() {
    assertThat(loadTableRawQuery("referenced-by=n%1Fa&referenced-by=m%1Fb")).isEqualTo(400);
    assertThat(authorizer.chains()).isEmpty();
  }

  /**
   * A malformed percent-encoding cannot be sent by an HTTP client library, which validates the URI
   * first, so the request is written to a socket.
   */
  @ParameterizedTest
  @ValueSource(strings = {"n%1Fv%", "n%1Fv%zz", "n%1Fv%2"})
  void rawRequestMalformedPercentEncodingIsRejected(String rawValue) throws Exception {
    URI base = endpoints.catalogApiEndpoint();
    String target =
        base.getRawPath()
            + "/v1/"
            + catalogName
            + "/namespaces/"
            + TARGET.namespace().level(0)
            + "/tables/"
            + TARGET.name()
            + "?referenced-by="
            + rawValue;
    StringBuilder request =
        new StringBuilder("GET ")
            .append(target)
            .append(" HTTP/1.1\r\nHost: ")
            .append(base.getHost())
            .append(':')
            .append(base.getPort())
            .append("\r\nAuthorization: Bearer ")
            .append(principalToken)
            .append("\r\nConnection: close\r\n");
    endpoints
        .extraHeaders()
        .forEach((name, value) -> request.append(name).append(": ").append(value).append("\r\n"));
    request.append("\r\n");

    String statusLine;
    try (Socket socket = new Socket(base.getHost(), base.getPort())) {
      socket.getOutputStream().write(request.toString().getBytes(StandardCharsets.US_ASCII));
      socket.getOutputStream().flush();
      statusLine =
          new BufferedReader(
                  new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII))
              .readLine();
    }

    assertThat(statusLine).startsWith("HTTP/1.1 400");
    assertThat(authorizer.chains()).isEmpty();
  }

  @Test
  void rawRequestChainContainingTheTargetIsRejected() {
    assertThat(loadTableRaw("ns1%1Ftarget_table")).isEqualTo(400);
    assertThat(authorizer.chains()).isEmpty();
  }

  static Stream<String> specialCharacters() {
    return Stream.of("&", "?", "#", "%", "+", " ", "/", "%2C", ",", "%1F", "=", ";", "\u00e9");
  }

  /** Special characters in namespace levels and in names, in a chain of two views. */
  private static List<TableIdentifier> chainWithSpecialCharacters(String special) {
    return List.of(
        TableIdentifier.of(
            Namespace.of("level1" + special, "level2" + special), "view" + special + "one"),
        TableIdentifier.of(Namespace.of(special + "other"), special));
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private ReferencedByQueryCaptureFilter.Captured singleCapture() {
    List<ReferencedByQueryCaptureFilter.Captured> captured =
        ReferencedByQueryCaptureFilter.captured();
    assertThat(captured).as("requests carrying referenced-by").hasSize(1);
    return captured.getFirst();
  }

  /** The still-encoded value of the {@code referenced-by} parameter in the captured query. */
  private static String rawReferencedBy(ReferencedByQueryCaptureFilter.Captured captured) {
    return Stream.of(captured.rawQuery().split("&"))
        .filter(param -> param.startsWith("referenced-by="))
        .map(param -> param.substring("referenced-by=".length()))
        .findFirst()
        .orElseThrow();
  }

  /** Mirrors {@code RESTUtil.encodeReferencedBy}, which is package-private in Iceberg. */
  private static String wire(List<TableIdentifier> chain) {
    return chain.stream()
        .map(id ->
            Stream.concat(Stream.of(id.namespace().levels()), Stream.of(id.name()))
                .map(RESTUtil::encodePathSegment)
                .collect(Collectors.joining(ENCODED_SEPARATOR)))
        .collect(Collectors.joining(","));
  }

  private int loadTableRaw(String referencedBy) {
    return loadTableRawQuery("referenced-by=" + referencedBy);
  }

  private int loadTableRawQuery(String rawQuery) {
    String url =
        endpoints.catalogApiEndpoint()
            + "/v1/"
            + catalogName
            + "/namespaces/"
            + TARGET.namespace().level(0)
            + "/tables/"
            + TARGET.name()
            + (rawQuery == null ? "" : "?" + rawQuery);
    RequestSpecification request = given().auth().oauth2(principalToken).urlEncodingEnabled(false);
    for (Map.Entry<String, String> header : endpoints.extraHeaders().entrySet()) {
      request = request.header(header.getKey(), header.getValue());
    }
    return request.get(url).then().extract().statusCode();
  }

  /** The authorizer received the chain, mapped into the catalog of the request. */
  private void assertChainsSeenByAuthorizer(List<TableIdentifier> chain) {
    List<List<String>> expected =
        chain.stream()
            .map(id ->
                Stream.concat(
                        Stream.of(catalogName),
                        Stream.concat(Stream.of(id.namespace().levels()), Stream.of(id.name())))
                    .toList())
            .toList();
    assertThat(authorizer.chains())
        .isNotEmpty()
        .allSatisfy(seen -> assertThat(seen).containsExactlyElementsOf(expected));
  }

  private static List<String> names(PolarisSecurable securable) {
    List<String> names = new ArrayList<>();
    for (PathSegment segment : securable.getPathSegments()) {
      names.add(segment.name());
    }
    return names;
  }
}
