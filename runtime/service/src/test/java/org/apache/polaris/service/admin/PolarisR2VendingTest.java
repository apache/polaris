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

import static org.apache.polaris.core.entity.PolarisEntityConstants.ENTITY_BASE_LOCATION;
import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.R2StorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.storage.r2.R2ParentToken;
import org.apache.polaris.core.storage.r2.R2TemporaryCredentialSigner;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

public class PolarisR2VendingTest {

  private static final String ACCOUNT = "0123456789abcdef0123456789abcdef";
  private static final String PARENT_KEY = "parent-key-id";
  private static final String PARENT_SECRET = "parent-secret-for-tests";
  private static final String BASE = "s3://r2-bucket/base";
  private static final String BUCKET_ROOT = "s3://r2-bucket";
  private static final UUID IDEMPOTENCY_KEY = new UUID(116617318654508422L, -7820829973016961092L);

  private final TestServices services;

  public PolarisR2VendingTest() {
    Supplier<FileIOFactory> fileIOFactorySupplier =
        () -> (FileIOFactory) (accessConfig, ioImplClassName, properties) -> new InMemoryFileIO();
    services =
        TestServices.builder()
            .config(
                Map.of(
                    "ALLOW_UNSTRUCTURED_TABLE_LOCATION", "false",
                    "ALLOW_TABLE_LOCATION_OVERLAP", "false",
                    "SUPPORTED_CATALOG_STORAGE_TYPES", List.of("R2")))
            .fileIOFactorySupplier(fileIOFactorySupplier)
            .r2ParentTokenResolver(
                name -> Optional.of(new R2ParentToken(PARENT_KEY, PARENT_SECRET)))
            .build();
  }

  private void createCatalog(String catalogName) {
    StorageConfigInfo config =
        R2StorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.R2)
            .setAccountId(ACCOUNT)
            .setAllowedLocations(List.of(BASE + "/" + catalogName))
            .build();
    Catalog catalog =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            catalogName,
            CatalogProperties.builder().setDefaultBaseLocation(BASE + "/" + catalogName).build(),
            1725487592064L,
            1725487592064L,
            1,
            config);
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  private void createNamespace(String catalogName, String namespace) {
    Map<String, String> properties = new HashMap<>();
    properties.put(ENTITY_BASE_LOCATION, BASE + "/" + catalogName + "/" + namespace);
    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder()
            .withNamespace(Namespace.of(namespace))
            .setProperties(properties)
            .build();
    try (Response response =
        services
            .restApi()
            .createNamespace(
                catalogName,
                request,
                IDEMPOTENCY_KEY,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private void createTable(String catalogName, String namespace, String table) {
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName(table)
            .withLocation(BASE + "/" + catalogName + "/" + namespace + "/" + table)
            .withSchema(SCHEMA)
            .build();
    try (Response response =
        services
            .restApi()
            .createTable(
                catalogName,
                namespace,
                request,
                null,
                IDEMPOTENCY_KEY,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private LoadTableResponse loadTableVended(String catalogName, String namespace, String table) {
    try (Response response =
        services
            .restApi()
            .loadTable(
                catalogName,
                namespace,
                table,
                "vended-credentials",
                null,
                "ALL",
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return response.readEntity(LoadTableResponse.class);
    }
  }

  private static DecodedJWT decode(String sessionToken) {
    String decoded = new String(Base64.getDecoder().decode(sessionToken), StandardCharsets.UTF_8);
    String jwt = decoded.substring(R2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX.length());
    return JWT.require(Algorithm.HMAC256(PARENT_SECRET)).build().verify(jwt);
  }

  @Test
  public void vendedLoadTableCarriesScopedR2Credentials() throws Exception {
    String cat = "r2vend";
    createCatalog(cat);
    createNamespace(cat, "ns");
    createTable(cat, "ns", "t1");

    LoadTableResponse first = loadTableVended(cat, "ns", "t1");
    Map<String, String> config = first.config();
    assertThat(config)
        .containsEntry("s3.access-key-id", PARENT_KEY)
        .containsEntry("s3.endpoint", "https://" + ACCOUNT + ".r2.cloudflarestorage.com")
        .containsEntry("s3.path-style-access", "true")
        .containsEntry("client.region", "auto")
        .containsKeys("s3.secret-access-key", "s3.session-token", "s3.session-token-expires-at-ms");
    assertThat(config.get("s3.secret-access-key")).isNotEqualTo(PARENT_SECRET);

    DecodedJWT jwt = decode(config.get("s3.session-token"));
    assertThat(jwt.getAudience()).containsExactly(ACCOUNT + ".r2.cloudflarestorage.com");
    assertThat(jwt.getIssuer()).isEqualTo(PARENT_KEY);
    assertThat(jwt.getSubject()).isEqualTo(ACCOUNT);
    assertThat(jwt.getClaim("bucket").asString()).isEqualTo("r2-bucket");
    assertThat(jwt.getClaim("scope").asString()).isEqualTo("object-read-write");
    assertThat(jwt.getClaim("paths").asMap().get("prefixPaths"))
        .asInstanceOf(InstanceOfAssertFactories.LIST)
        .containsExactly("base/" + cat + "/ns/t1/");

    // The advertised expiry is the exp claim, to the second.
    assertThat(Long.parseLong(config.get("s3.session-token-expires-at-ms")) / 1000)
        .isEqualTo(jwt.getExpiresAtAsInstant().getEpochSecond());
    // The temporary secret is the SHA-256 hex of the raw JWT, so a client can derive nothing else.
    assertThat(config.get("s3.secret-access-key"))
        .isEqualTo(sha256Hex(rawJwt(config.get("s3.session-token"))));

    LoadTableResponse second = loadTableVended(cat, "ns", "t1");
    assertThat(second.config().get("s3.session-token")).isEqualTo(config.get("s3.session-token"));
  }

  @Test
  public void nonDelegatedLoadTableCarriesNoCredentials() {
    String cat = "r2plain";
    createCatalog(cat);
    createNamespace(cat, "ns");
    createTable(cat, "ns", "t1");
    try (Response response =
        services
            .restApi()
            .loadTable(
                cat,
                "ns",
                "t1",
                null,
                null,
                "ALL",
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      LoadTableResponse resp = response.readEntity(LoadTableResponse.class);
      assertThat(resp.config())
          .doesNotContainKeys("s3.secret-access-key", "s3.session-token", "s3.access-key-id")
          .containsEntry("s3.endpoint", "https://" + ACCOUNT + ".r2.cloudflarestorage.com")
          .containsEntry("client.region", "auto");
    }
  }

  /**
   * The class-level services forbid unstructured locations, so a test that needs a {@code
   * write.data.path} outside the table prefix builds its own services with the feature enabled.
   */
  private static TestServices unstructuredServices() {
    Supplier<FileIOFactory> fileIOFactorySupplier =
        () -> (FileIOFactory) (accessConfig, ioImplClassName, properties) -> new InMemoryFileIO();
    return TestServices.builder()
        .config(
            Map.of(
                "ALLOW_UNSTRUCTURED_TABLE_LOCATION", "true",
                "ALLOW_TABLE_LOCATION_OVERLAP", "true",
                "SUPPORTED_CATALOG_STORAGE_TYPES", List.of("R2")))
        .fileIOFactorySupplier(fileIOFactorySupplier)
        .r2ParentTokenResolver(name -> Optional.of(new R2ParentToken(PARENT_KEY, PARENT_SECRET)))
        .build();
  }

  /**
   * A table whose locations span two buckets can never get a credential, because an R2 temporary
   * credential binds to exactly one bucket. Polaris needs a credential to write the first metadata
   * file, so the rejection lands on create and the table is never persisted. The message names both
   * buckets; it carries no table name because at create time the resolved path's leaf is still the
   * namespace ({@code StorageAccessConfigProvider#buildCredentialVendingContext} only sets {@code
   * tableName} for a TABLE_LIKE leaf). The table-named form of the message is covered by {@code
   * R2CredentialsStorageIntegrationTest.multiBucketRejectionNamesTheTable}.
   */
  @Test
  public void vendingAcrossTwoBucketsIsRejected() {
    TestServices unstructured = unstructuredServices();

    String cat = "r2twobuckets";
    StorageConfigInfo config =
        R2StorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.R2)
            .setAccountId(ACCOUNT)
            .setAllowedLocations(List.of(BASE + "/" + cat, "s3://other/"))
            .build();
    Catalog catalog =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            cat,
            CatalogProperties.builder()
                .setDefaultBaseLocation(BASE + "/" + cat)
                .addProperty("polaris.config.allow.unstructured.table.location", "true")
                .build(),
            1725487592064L,
            1725487592064L,
            1,
            config);
    try (Response response =
        unstructured
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                unstructured.realmContext(),
                unstructured.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    Map<String, String> namespaceProperties = new HashMap<>();
    namespaceProperties.put(ENTITY_BASE_LOCATION, BASE + "/" + cat + "/ns");
    try (Response response =
        unstructured
            .restApi()
            .createNamespace(
                cat,
                CreateNamespaceRequest.builder()
                    .withNamespace(Namespace.of("ns"))
                    .setProperties(namespaceProperties)
                    .build(),
                IDEMPOTENCY_KEY,
                unstructured.realmContext(),
                unstructured.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // The table lives under r2-bucket but writes its data under the second bucket.
    CreateTableRequest createTable =
        CreateTableRequest.builder()
            .withName("t1")
            .withLocation(BASE + "/" + cat + "/ns/t1")
            .withSchema(SCHEMA)
            .setProperty("write.data.path", "s3://other/x/")
            .build();
    // TestServices.restApi() is a bare POJO, so the failure surfaces as a thrown exception.
    assertThatThrownBy(
            () ->
                unstructured
                    .restApi()
                    .createTable(
                        cat,
                        "ns",
                        createTable,
                        null,
                        IDEMPOTENCY_KEY,
                        unstructured.realmContext(),
                        unstructured.securityContext()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("r2-bucket")
        .hasMessageContaining("other")
        .hasMessageContaining("one bucket per table");

    // The table was never created, so a vended load cannot reach a two-bucket credential either.
    assertThatThrownBy(
            () ->
                unstructured
                    .restApi()
                    .loadTable(
                        cat,
                        "ns",
                        "t1",
                        "vended-credentials",
                        null,
                        "ALL",
                        null,
                        unstructured.realmContext(),
                        unstructured.securityContext()))
        .isInstanceOf(NoSuchTableException.class);
  }

  /**
   * A catalog whose single allowed location is the bucket root, the layout the bucket-per-namespace
   * design describes. Namespaces sit directly under {@code s3://r2-bucket/}, because {@code
   * LocalIcebergCatalog.validateNamespaceLocation} derives the only permitted namespace location
   * from the allowed locations, not from the default base location.
   */
  private static void createBucketRootCatalog(TestServices svc, String cat) {
    StorageConfigInfo config =
        R2StorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.R2)
            .setAccountId(ACCOUNT)
            .setAllowedLocations(List.of(BUCKET_ROOT + "/"))
            .build();
    Catalog catalog =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            cat,
            CatalogProperties.builder()
                .setDefaultBaseLocation(BUCKET_ROOT)
                .addProperty("polaris.config.allow.unstructured.table.location", "true")
                .build(),
            1725487592064L,
            1725487592064L,
            1,
            config);
    try (Response response =
        svc.catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog), svc.realmContext(), svc.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  private static void createNamespaceAt(
      TestServices svc, String cat, String namespace, String location) {
    Map<String, String> properties = new HashMap<>();
    properties.put(ENTITY_BASE_LOCATION, location);
    try (Response response =
        svc.restApi()
            .createNamespace(
                cat,
                CreateNamespaceRequest.builder()
                    .withNamespace(Namespace.of(namespace))
                    .setProperties(properties)
                    .build(),
                IDEMPOTENCY_KEY,
                svc.realmContext(),
                svc.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private static Response createTableAt(
      TestServices svc,
      String cat,
      String namespace,
      String table,
      String location,
      @Nullable String writeDataPath) {
    CreateTableRequest.Builder request =
        CreateTableRequest.builder().withName(table).withLocation(location).withSchema(SCHEMA);
    if (writeDataPath != null) {
      request.setProperty("write.data.path", writeDataPath);
    }
    return svc.restApi()
        .createTable(
            cat,
            namespace,
            request.build(),
            null,
            IDEMPOTENCY_KEY,
            svc.realmContext(),
            svc.securityContext());
  }

  private static List<String> prefixPaths(TestServices svc, String cat, String ns, String table) {
    try (Response response =
        svc.restApi()
            .loadTable(
                cat,
                ns,
                table,
                "vended-credentials",
                null,
                "ALL",
                null,
                svc.realmContext(),
                svc.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      LoadTableResponse loaded = response.readEntity(LoadTableResponse.class);
      DecodedJWT jwt = decode(loaded.config().get("s3.session-token"));
      assertThat(jwt.getClaim("paths").isMissing())
          .describedAs("paths claim present, so the credential is not whole-bucket")
          .isFalse();
      @SuppressWarnings("unchecked")
      List<String> paths = (List<String>) jwt.getClaim("paths").asMap().get("prefixPaths");
      return paths;
    }
  }

  /**
   * A {@code write.data.path} with a doubled slash passes the raw {@code startsWith} location
   * validators, which see {@code r2-bucket//ns/victim/} as neither a child of nor a parent of the
   * real {@code r2-bucket/ns/victim/}. The vended prefix must therefore keep that leading slash:
   * collapsing it would hand the attacker's table a credential over the victim's objects. Before
   * this was fixed the claim read {@code ["ns/attacker/", "ns/victim/"]}.
   */
  @Test
  public void doubledSlashDataPathKeepsItsLeadingSlashInTheVendedPrefix() {
    TestServices svc = unstructuredServices();
    String cat = "r2sibling";
    createBucketRootCatalog(svc, cat);
    createNamespaceAt(svc, cat, "ns", BUCKET_ROOT + "/ns");
    try (Response response =
        createTableAt(svc, cat, "ns", "victim", BUCKET_ROOT + "/ns/victim", null)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    try (Response response =
        createTableAt(
            svc,
            cat,
            "ns",
            "attacker",
            BUCKET_ROOT + "/ns/attacker",
            BUCKET_ROOT + "//ns/victim/")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertThat(prefixPaths(svc, cat, "ns", "attacker"))
        .containsExactly("/ns/victim/", "ns/attacker/")
        .doesNotContain("ns/victim/");
  }

  /**
   * {@code s3://r2-bucket//} is the bucket root plus one key segment that is itself empty, not the
   * bucket root. Its prefix is {@code "/"}, and the credential keeps a {@code paths} claim rather
   * than degenerating to whole-bucket access. Before this was fixed the claim was absent, which is
   * how the signer spells whole-bucket.
   */
  @Test
  public void doubledSlashAtBucketRootDoesNotWidenToTheWholeBucket() {
    TestServices svc = unstructuredServices();
    String cat = "r2rootslash";
    createBucketRootCatalog(svc, cat);
    createNamespaceAt(svc, cat, "ns", BUCKET_ROOT + "/ns");
    try (Response response =
        createTableAt(svc, cat, "ns", "t1", BUCKET_ROOT + "/ns/t1", BUCKET_ROOT + "//")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    assertThat(prefixPaths(svc, cat, "ns", "t1")).containsExactly("/", "ns/t1/");
  }

  private static String rawJwt(String sessionToken) {
    String decoded = new String(Base64.getDecoder().decode(sessionToken), StandardCharsets.UTF_8);
    return decoded.substring(R2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX.length());
  }

  private static String sha256Hex(String value) throws NoSuchAlgorithmException {
    return HexFormat.of()
        .formatHex(
            MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
  }
}
