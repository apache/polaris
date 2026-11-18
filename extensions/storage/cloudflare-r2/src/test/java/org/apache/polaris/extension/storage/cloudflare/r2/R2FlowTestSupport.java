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
package org.apache.polaris.extension.storage.cloudflare.r2;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.polaris.core.entity.PolarisEntityConstants.ENTITY_BASE_LOCATION;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LocalSigningCredentialVendingMechanism.R2_LOCAL_SIGNING;
import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.exception.IcebergExceptionMapper;

/**
 * Shared setup for the flow tests: {@link TestServices} with an {@code R2_LOCAL_SIGNING} mechanism
 * installed through {@code additionalVendingMechanisms}, plus small helpers for the management and
 * catalog resource calls. The resource classes run as POJOs, so a refusal surfaces as the thrown
 * exception; {@link #statusOf} passes it through the REST exception mapper to get the status a
 * client would see.
 */
final class R2FlowTestSupport {

  private R2FlowTestSupport() {}

  static final String REALM = "r2-flow-test-realm";
  static final String ACCOUNT = "0123456789abcdef0123456789abcdef";
  static final String ENDPOINT = "https://" + ACCOUNT + ".r2.cloudflarestorage.com";
  static final String OTHER_ENDPOINT =
      "https://fedcba9876543210fedcba9876543210.r2.cloudflarestorage.com";
  static final String ACCESS_KEY = "test-r2-access-key";
  static final String SECRET_KEY = "test-r2-secret-key";
  static final String BUCKET = "r2-bucket";
  static final String BUCKET_ROOT = "s3://" + BUCKET;
  static final String BASE = BUCKET_ROOT + "/base";
  static final String OTHER_MECHANISM = "OTHER_MECHANISM";

  /** The mechanism's clock. Default credential lifetime is 3600 seconds, so expiry is NOW + 1 h. */
  static final Instant NOW = Instant.parse("2026-10-06T12:00:00Z");

  static final List<String> CREDENTIAL_KEYS =
      List.of(
          "s3.access-key-id",
          "s3.secret-access-key",
          "s3.session-token",
          "s3.session-token-expires-at-ms");

  static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  static final CloudflareR2ParentToken TOKEN = new CloudflareR2ParentToken(ACCESS_KEY, SECRET_KEY);

  /** Every storage name resolves to the dummy token. */
  static final CloudflareR2ParentTokenResolver FIXED_TOKEN = storageName -> Optional.of(TOKEN);

  // ---------------------------------------------------------------- services

  static Map<String, Object> config(Map<String, Object> overrides) {
    Map<String, Object> config = new HashMap<>();
    config.put("SUPPORTED_CATALOG_STORAGE_TYPES", List.of("S3"));
    config.put(
        "SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS",
        List.of("STS", R2_LOCAL_SIGNING, OTHER_MECHANISM));
    config.put("ALLOW_UNSTRUCTURED_TABLE_LOCATION", Boolean.FALSE);
    config.put("ALLOW_TABLE_LOCATION_OVERLAP", Boolean.FALSE);
    config.putAll(overrides);
    return config;
  }

  static FileIOFactory inMemoryFileIOFactory() {
    return (accessConfig, ioImplClassName, properties) -> new InMemoryFileIO();
  }

  /** A FileIO factory that records every {@link StorageAccessConfig} it is asked to load with. */
  static FileIOFactory recordingFileIOFactory(List<StorageAccessConfig> sink) {
    return (accessConfig, ioImplClassName, properties) -> {
      sink.add(accessConfig);
      return new InMemoryFileIO();
    };
  }

  static TestServices services(
      Map<String, Object> overrides, CloudflareR2ParentTokenResolver resolver) {
    return services(overrides, resolver, inMemoryFileIOFactory());
  }

  static TestServices services(
      Map<String, Object> overrides,
      CloudflareR2ParentTokenResolver resolver,
      FileIOFactory fileIOFactory) {
    Map<String, Object> config = config(overrides);
    RealmContext realmContext = () -> REALM;
    RealmConfig realmConfig = new RealmConfigImpl((rc, name) -> config.get(name), realmContext);
    S3CredentialVendingMechanism r2 =
        new R2LocalSigningCredentialVendingMechanism(
            resolver, Clock.fixed(NOW, ZoneOffset.UTC), null, realmConfig);
    return TestServices.builder()
        .config(config)
        .realmContext(realmContext)
        .fileIOFactorySupplier(() -> fileIOFactory)
        .additionalVendingMechanisms(
            Map.of(R2_LOCAL_SIGNING, r2, OTHER_MECHANISM, TestServices.fakeMechanism()))
        .build();
  }

  /** The services the doubled-slash tests need: a table may name a data path outside its prefix. */
  static TestServices unstructuredServices() {
    return services(
        Map.of("ALLOW_UNSTRUCTURED_TABLE_LOCATION", true, "ALLOW_TABLE_LOCATION_OVERLAP", true),
        FIXED_TOKEN);
  }

  // ---------------------------------------------------------------- catalogs

  static AwsStorageConfigInfo.Builder r2Config(List<String> allowedLocations) {
    return AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
        .setCredentialVendingMechanism(R2_LOCAL_SIGNING)
        .setEndpoint(ENDPOINT)
        .setPathStyleAccess(true)
        .setRegion("auto")
        .setAllowedLocations(allowedLocations);
  }

  static AwsStorageConfigInfo.Builder otherMechanismConfig(List<String> allowedLocations) {
    return AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
        .setCredentialVendingMechanism(OTHER_MECHANISM)
        .setEndpoint(ENDPOINT)
        .setPathStyleAccess(true)
        .setRegion("auto")
        .setAllowedLocations(allowedLocations);
  }

  static Catalog catalog(
      String name,
      String defaultBaseLocation,
      StorageConfigInfo storage,
      boolean allowUnstructuredLocation) {
    CatalogProperties.Builder properties = CatalogProperties.builder(defaultBaseLocation);
    if (allowUnstructuredLocation) {
      properties.addProperty("polaris.config.allow.unstructured.table.location", "true");
    }
    return PolarisCatalog.builder()
        .setType(Catalog.TypeEnum.INTERNAL)
        .setName(name)
        .setProperties(properties.build())
        .setStorageConfigInfo(storage)
        .build();
  }

  /** A catalog whose default base location is {@code BASE/<name>}. */
  static Catalog catalog(String name, StorageConfigInfo storage) {
    return catalog(name, BASE + "/" + name, storage, false);
  }

  static Response createCatalogResponse(TestServices svc, Catalog catalog) {
    return svc.catalogsApi()
        .createCatalog(
            new CreateCatalogRequest(catalog), svc.realmContext(), svc.securityContext());
  }

  static void createCatalog(TestServices svc, Catalog catalog) {
    try (Response response = createCatalogResponse(svc, catalog)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  static Catalog fetchCatalog(TestServices svc, String name) {
    try (Response response =
        svc.catalogsApi().getCatalog(name, svc.realmContext(), svc.securityContext())) {
      return (Catalog) response.getEntity();
    }
  }

  /** Updates catalog {@code name} to {@code storage}, keeping its default base location. */
  static Response updateCatalogResponse(TestServices svc, String name, StorageConfigInfo storage) {
    Catalog current = fetchCatalog(svc, name);
    UpdateCatalogRequest request =
        new UpdateCatalogRequest(
            current.getEntityVersion(),
            Map.of("default-base-location", BASE + "/" + name),
            storage);
    return svc.catalogsApi()
        .updateCatalog(name, request, svc.realmContext(), svc.securityContext());
  }

  // ------------------------------------------------------ namespaces, tables

  static void createNamespaceAt(TestServices svc, String cat, String namespace, String location) {
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
                null,
                svc.realmContext(),
                svc.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  /** Creates a table and returns the raw response. {@code writeDataPath} may be null. */
  static Response createTableAt(
      TestServices svc,
      String cat,
      String namespace,
      String table,
      String location,
      String writeDataPath) {
    CreateTableRequest.Builder request =
        CreateTableRequest.builder().withName(table).withLocation(location).withSchema(SCHEMA);
    if (writeDataPath != null) {
      request.setProperty("write.data.path", writeDataPath);
    }
    return svc.restApi()
        .createTable(
            cat, namespace, request.build(), null, null, svc.realmContext(), svc.securityContext());
  }

  /**
   * Creates the catalog, the namespace {@code ns} at {@code BASE/<cat>/ns}, and the table {@code
   * t1} at {@code BASE/<cat>/ns/t1}. Returns the table location.
   */
  static String createStructuredTable(TestServices svc, String cat, StorageConfigInfo storage) {
    createCatalog(svc, catalog(cat, storage));
    createNamespaceAt(svc, cat, "ns", BASE + "/" + cat + "/ns");
    String location = BASE + "/" + cat + "/ns/t1";
    try (Response response = createTableAt(svc, cat, "ns", "t1", location, null)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    return location;
  }

  /**
   * A catalog whose single allowed location is the bucket root. Namespaces sit directly under
   * {@code s3://r2-bucket/}, because the namespace check in {@code LocalIcebergCatalog} allows only
   * the location derived from the catalog's default base location, here the bucket root.
   */
  static void createBucketRootCatalog(TestServices svc, String cat) {
    createCatalog(
        svc, catalog(cat, BUCKET_ROOT, r2Config(List.of(BUCKET_ROOT + "/")).build(), true));
  }

  static Response loadTableResponse(
      TestServices svc, String cat, String ns, String table, String accessDelegation) {
    return svc.restApi()
        .loadTable(
            cat,
            ns,
            table,
            accessDelegation == null ? null : List.of(accessDelegation),
            null,
            "ALL",
            null,
            svc.realmContext(),
            svc.securityContext());
  }

  static LoadTableResponse loadTable(
      TestServices svc, String cat, String ns, String table, String accessDelegation) {
    try (Response response = loadTableResponse(svc, cat, ns, table, accessDelegation)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return (LoadTableResponse) response.getEntity();
    }
  }

  // --------------------------------------------------------------- assertions

  /** The HTTP status the REST exception mapper gives a thrown exception. */
  static int statusOf(Throwable thrown) {
    assertThat(thrown).isInstanceOf(RuntimeException.class);
    try (Response response = new IcebergExceptionMapper().toResponse((RuntimeException) thrown)) {
      return response.getStatus();
    }
  }

  static String rawJwt(String sessionToken) {
    String decoded = new String(Base64.getDecoder().decode(sessionToken), StandardCharsets.UTF_8);
    return decoded.substring(CloudflareR2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX.length());
  }

  /** Checks the signature with the dummy parent secret and skips the time claims. */
  static DecodedJWT decode(String sessionToken) {
    DecodedJWT jwt = JWT.decode(rawJwt(sessionToken));
    Algorithm.HMAC256(SECRET_KEY).verify(jwt);
    return jwt;
  }

  /** The {@code paths.prefixPaths} claim. The claim must exist: a missing claim is whole-bucket. */
  @SuppressWarnings("unchecked")
  static List<String> prefixPaths(DecodedJWT jwt) {
    assertThat(jwt.getClaim("paths").isMissing())
        .describedAs("paths claim present, so the credential is not whole-bucket")
        .isFalse();
    return (List<String>) jwt.getClaim("paths").asMap().get("prefixPaths");
  }

  static String sha256Hex(String value) throws NoSuchAlgorithmException {
    return HexFormat.of()
        .formatHex(
            MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
  }
}
