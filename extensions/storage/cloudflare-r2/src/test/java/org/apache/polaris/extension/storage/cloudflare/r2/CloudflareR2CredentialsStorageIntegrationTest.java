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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.ImmutableAwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

class CloudflareR2CredentialsStorageIntegrationTest {

  private static final String MECHANISM = "R2_LOCAL_SIGNING";
  private static final String FOR_MECHANISM =
      " for the " + MECHANISM + " credential vending mechanism";
  private static final String ACCOUNT = "0123456789abcdef0123456789abcdef";
  private static final String ENDPOINT = "https://" + ACCOUNT + ".r2.cloudflarestorage.com";
  private static final String REFRESH_PROPERTY =
      StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT.getPropertyName();
  private static final String RESOLVE_BY_NAME_KEY =
      FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key();

  private static RealmConfig realmConfig(Map<String, Object> values) {
    return new RealmConfigImpl((rc, name) -> values.get(name), () -> "realm");
  }

  /** The default realm config: {@code RESOLVE_CREDENTIALS_BY_STORAGE_NAME} is false. */
  private static final RealmConfig REALM_CONFIG = realmConfig(Map.of());

  private static final RealmConfig BY_NAME_REALM_CONFIG =
      realmConfig(Map.of(RESOLVE_BY_NAME_KEY, true));

  // Fixed at the current second, not at a literal past instant: StorageCredentialCache computes an
  // entry's TTL from (expirationTime - System.currentTimeMillis()) / 2, so credentials minted from
  // a clock in the past land in the cache already expired. See expiredCredentialIsNotReused.
  private static final Clock CLOCK =
      Clock.fixed(Instant.now().truncatedTo(ChronoUnit.SECONDS), ZoneOffset.UTC);

  private static ImmutableAwsStorageConfigurationInfo.Builder r2Config() {
    return AwsStorageConfigurationInfo.builder()
        .credentialVendingMechanism(MECHANISM)
        .endpoint(ENDPOINT)
        .pathStyleAccess(true)
        .region("auto")
        .addAllowedLocation("s3://bucket/wh/");
  }

  private static final AwsStorageConfigurationInfo CONFIG =
      r2Config().storageName("tenant-a").build();
  private static final AwsStorageConfigurationInfo UNNAMED_CONFIG = r2Config().build();

  private static final CloudflareR2ParentToken DEFAULT_TOKEN =
      new CloudflareR2ParentToken("test-r2-access-key", "test-r2-secret-key");
  private static final CloudflareR2ParentToken NAMED_TOKEN =
      new CloudflareR2ParentToken("test-r2-named-access-key", "test-r2-named-secret-key");

  /** The default token for a null name, the named token for {@code tenant-a}, nothing else. */
  private static final CloudflareR2ParentTokenResolver RESOLVER =
      name ->
          name == null
              ? Optional.of(DEFAULT_TOKEN)
              : "tenant-a".equals(name) ? Optional.of(NAMED_TOKEN) : Optional.empty();

  /** A resolver with no tokens; every lookup is empty. */
  private static final CloudflareR2ParentTokenResolver NO_TOKENS = name -> Optional.empty();

  private static final List<LocationGrant> GRANTS =
      List.of(new LocationGrant(Set.of("s3://bucket/wh/db/t/"), Set.of(PolarisStorageActions.ALL)));

  private static CloudflareR2CredentialsStorageIntegration integration(
      CloudflareR2ParentTokenResolver resolver,
      Clock clock,
      StorageCredentialCache cache,
      AwsStorageConfigurationInfo config) {
    return integration(resolver, clock, cache, config, REALM_CONFIG);
  }

  private static CloudflareR2CredentialsStorageIntegration integration(
      CloudflareR2ParentTokenResolver resolver,
      Clock clock,
      StorageCredentialCache cache,
      AwsStorageConfigurationInfo config,
      RealmConfig realmConfig) {
    return new CloudflareR2CredentialsStorageIntegration(
        resolver, clock, cache, config, realmConfig);
  }

  private static StorageAccessConfig vend(
      CloudflareR2CredentialsStorageIntegration integration, List<LocationGrant> grants) {
    return integration.getStorageAccessConfig(
        grants, Optional.empty(), CredentialVendingContext.empty());
  }

  private static StorageAccessConfig vendInRealm(
      CloudflareR2CredentialsStorageIntegration integration, String realmId) {
    return integration.getStorageAccessConfig(
        GRANTS,
        Optional.empty(),
        CredentialVendingContext.builder().realm(Optional.of(realmId)).build());
  }

  private static LocationGrant grant(String location, PolarisStorageActions... actions) {
    return new LocationGrant(Set.of(location), Set.of(actions));
  }

  /** The vended session token is base64("jwt/" + JWT); return the decoded JWT. */
  private static DecodedJWT vendedJwt(StorageAccessConfig cfg) {
    String sessionToken = cfg.credentials().get(StorageAccessProperty.AWS_TOKEN.getPropertyName());
    String raw = new String(Base64.getDecoder().decode(sessionToken), StandardCharsets.UTF_8);
    assertThat(raw).startsWith(CloudflareR2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX);
    return JWT.decode(
        raw.substring(CloudflareR2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX.length()));
  }

  /** Whether the JWT carries a valid HS256 signature of the given secret. */
  private static boolean signedWith(DecodedJWT jwt, String secret) {
    try {
      Algorithm.HMAC256(secret).verify(jwt);
      return true;
    } catch (SignatureVerificationException e) {
      return false;
    }
  }

  private static String vendedScope(StorageAccessConfig cfg) {
    return vendedJwt(cfg).getClaim("scope").asString();
  }

  private static String vendedKeyId(StorageAccessConfig cfg) {
    return cfg.credentials().get(StorageAccessProperty.AWS_KEY_ID.getPropertyName());
  }

  /** The refusal for a grant location with an empty path segment, as the client sees it. */
  private static String emptyPathSegmentMessage(String location) {
    return "location '"
        + location
        + "' contains an empty path segment ('//'), which the "
        + MECHANISM
        + " credential vending mechanism does not accept";
  }

  /**
   * Captures the log events of {@link CloudflareR2CredentialsStorageIntegration} at the given level
   * while it is open. Use it in a try-with-resources block, which restores the logger.
   */
  private static final class LogCapture implements AutoCloseable {
    private final Logger logger =
        (Logger) LoggerFactory.getLogger(CloudflareR2CredentialsStorageIntegration.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private final Level original = logger.getLevel();

    LogCapture(Level level) {
      appender.start();
      logger.setLevel(level);
      logger.addAppender(appender);
    }

    List<ILoggingEvent> events() {
      return List.copyOf(appender.list);
    }

    @Override
    public void close() {
      logger.detachAppender(appender);
      appender.stop();
      logger.setLevel(original);
    }
  }

  /**
   * Vends with a log capture at WARN and expects the refusal for a missing parent token. The
   * exception message reaches the client, so it must hold the given text and no property name. One
   * WARN event names the property that the operator has to set.
   */
  private static void assertRefusedForMissingToken(
      CloudflareR2CredentialsStorageIntegration integration,
      String expectedMessage,
      String expectedProperty) {
    Throwable thrown;
    List<ILoggingEvent> events;
    try (LogCapture logs = new LogCapture(Level.WARN)) {
      thrown = catchThrowable(() -> vend(integration, GRANTS));
      events = logs.events();
    }

    assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessage(expectedMessage);
    assertThat(thrown.getMessage()).doesNotContain("polaris.storage");
    assertThat(events)
        .filteredOn(event -> event.getLevel() == Level.WARN)
        .singleElement()
        .satisfies(event -> assertThat(event.getFormattedMessage()).contains(expectedProperty));
  }

  /**
   * Passes a config that never went through {@code validate} to the integration, and expects both
   * the cache load and a vend to refuse it with the given message. The message must hold neither
   * value of the dummy parent token.
   */
  private static void assertRefusedAtVendTime(AwsStorageConfigurationInfo config, String expected) {
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, null, config);

    StorageCredentialCacheKey key =
        integration.buildCacheKey(GRANTS, CredentialVendingContext.empty());
    assertThatThrownBy(key::load)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expected)
        .message()
        .doesNotContain(DEFAULT_TOKEN.accessKeyId())
        .doesNotContain(DEFAULT_TOKEN.secretAccessKey());
    assertThatThrownBy(() -> vend(integration, GRANTS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expected)
        .message()
        .doesNotContain(DEFAULT_TOKEN.accessKeyId())
        .doesNotContain(DEFAULT_TOKEN.secretAccessKey());
  }

  // ---- vending, cache and extra properties ----

  @Test
  void vendsWithoutCache() {
    StorageAccessConfig cfg = vend(integration(RESOLVER, CLOCK, null, CONFIG), GRANTS);
    assertThat(vendedKeyId(cfg)).isEqualTo(DEFAULT_TOKEN.accessKeyId());
    assertThat(cfg.extraProperties()).containsEntry("s3.endpoint", ENDPOINT);
  }

  @Test
  void secondCallIsServedFromCache() {
    StorageCredentialCache cache = new StorageCredentialCache(() -> 10_000);
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, cache, CONFIG);
    StorageAccessConfig first = vend(integration, GRANTS);
    StorageAccessConfig second = vend(integration, GRANTS);
    assertThat(second).isSameAs(first);
    assertThat(cache.getEstimatedSize()).isEqualTo(1);
  }

  /** The refresh endpoint is added after the cache lookup, so it is not part of the key. */
  @Test
  void twoRefreshEndpointsShareOneCacheEntry() {
    StorageCredentialCache cache = new StorageCredentialCache(() -> 10_000);
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, cache, CONFIG);
    StorageAccessConfig first =
        integration.getStorageAccessConfig(
            GRANTS, Optional.of("/v1/a/credentials"), CredentialVendingContext.empty());
    StorageAccessConfig second =
        integration.getStorageAccessConfig(
            GRANTS, Optional.of("/v1/b/credentials"), CredentialVendingContext.empty());
    StorageAccessConfig third = vend(integration, GRANTS);

    assertThat(cache.getEstimatedSize()).isEqualTo(1);
    assertThat(first.extraProperties()).containsEntry(REFRESH_PROPERTY, "/v1/a/credentials");
    assertThat(second.extraProperties()).containsEntry(REFRESH_PROPERTY, "/v1/b/credentials");
    assertThat(third.extraProperties()).doesNotContainKey(REFRESH_PROPERTY);
    assertThat(second.credentials()).isEqualTo(first.credentials());
    assertThat(third.credentials()).isEqualTo(first.credentials());
  }

  /**
   * Catalog, table, principal and role names are not part of the key: two requests that differ only
   * in them for the same location and actions share one cached credential.
   */
  @Test
  void contextsDifferingOnlyInCallerMetadataShareOneCacheEntry() {
    StorageCredentialCache cache = new StorageCredentialCache(() -> 10_000);
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, cache, CONFIG);
    CredentialVendingContext first =
        CredentialVendingContext.builder()
            .catalogName(Optional.of("catalog-a"))
            .tableName(Optional.of("table-a"))
            .principalName(Optional.of("principal-a"))
            .activatedRoles(Optional.of("role-a"))
            .build();
    CredentialVendingContext second =
        CredentialVendingContext.builder()
            .catalogName(Optional.of("catalog-b"))
            .tableName(Optional.of("table-b"))
            .principalName(Optional.of("principal-b"))
            .activatedRoles(Optional.of("role-b"))
            .build();
    StorageAccessConfig firstCfg =
        integration.getStorageAccessConfig(GRANTS, Optional.empty(), first);
    StorageAccessConfig secondCfg =
        integration.getStorageAccessConfig(GRANTS, Optional.empty(), second);
    assertThat(cache.getEstimatedSize()).isEqualTo(1);
    assertThat(secondCfg).isSameAs(firstCfg);
  }

  /**
   * The realm is part of the cache key. Two realms with the same storage config and the same grants
   * get two cache entries: the second realm's first request mints again, and each realm's next
   * request is served from its own entry. The test compares the cached instances. A mint is the
   * only way to get a new one.
   */
  @Test
  void twoRealmsWithTheSameConfigAndGrantsGetTwoCacheEntries() {
    StorageCredentialCache cache = new StorageCredentialCache(() -> 10_000);
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, cache, CONFIG);

    StorageAccessConfig firstInA = vendInRealm(integration, "realm-a");
    StorageAccessConfig firstInB = vendInRealm(integration, "realm-b");
    StorageAccessConfig secondInA = vendInRealm(integration, "realm-a");
    StorageAccessConfig secondInB = vendInRealm(integration, "realm-b");

    assertThat(firstInB).isNotSameAs(firstInA);
    assertThat(secondInA).isSameAs(firstInA);
    assertThat(secondInB).isSameAs(firstInB);
    assertThat(cache.getEstimatedSize()).isEqualTo(2);
  }

  /**
   * The cache key holds a fingerprint of the parent secret. After a rotation that keeps the access
   * key id, the next request misses the cache and mints again. The new credential carries a
   * signature of the new secret only. The test compares signatures through booleans.
   */
  @Test
  void aRotatedParentSecretWithTheSameKeyIdMintsAgain() {
    String oldSecret = "test-r2-old-secret-key";
    String newSecret = "test-r2-new-secret-key";
    AtomicInteger calls = new AtomicInteger();
    CloudflareR2ParentTokenResolver rotating =
        name ->
            Optional.of(
                new CloudflareR2ParentToken(
                    DEFAULT_TOKEN.accessKeyId(),
                    calls.getAndIncrement() == 0 ? oldSecret : newSecret));
    StorageCredentialCache cache = new StorageCredentialCache(() -> 10_000);
    CloudflareR2CredentialsStorageIntegration integration =
        integration(rotating, CLOCK, cache, CONFIG);

    StorageAccessConfig before = vend(integration, GRANTS);
    StorageAccessConfig after = vend(integration, GRANTS);

    assertThat(calls.get()).isEqualTo(2);
    assertThat(after).isNotSameAs(before);
    assertThat(cache.getEstimatedSize()).isEqualTo(2);
    assertThat(vendedKeyId(before)).isEqualTo(DEFAULT_TOKEN.accessKeyId());
    assertThat(vendedKeyId(after)).isEqualTo(DEFAULT_TOKEN.accessKeyId());
    assertThat(signedWith(vendedJwt(before), oldSecret)).isTrue();
    assertThat(signedWith(vendedJwt(before), newSecret)).isFalse();
    assertThat(signedWith(vendedJwt(after), newSecret)).isTrue();
    assertThat(signedWith(vendedJwt(after), oldSecret)).isFalse();
  }

  @Test
  void refreshEndpointIsAddedWhenPresentAndOmittedWhenAbsent() {
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, null, CONFIG);
    StorageAccessConfig with =
        integration.getStorageAccessConfig(
            GRANTS, Optional.of("http://polaris/creds"), CredentialVendingContext.empty());
    assertThat(with.extraProperties())
        .containsEntry(REFRESH_PROPERTY, "http://polaris/creds")
        .containsOnlyKeys("s3.endpoint", "s3.path-style-access", "client.region", REFRESH_PROPERTY);

    StorageAccessConfig without = vend(integration, GRANTS);
    assertThat(without.extraProperties())
        .containsOnlyKeys("s3.endpoint", "s3.path-style-access", "client.region");
  }

  /**
   * The client contract: the credential triple plus the expiry as credentials, and the endpoint,
   * path style and region as extra properties, taken from the catalog's explicit values.
   */
  @Test
  void emitsTheCredentialPropertiesAndExpiresAt() {
    StorageAccessConfig cfg = vend(integration(RESOLVER, CLOCK, null, CONFIG), GRANTS);
    assertThat(cfg.credentials())
        .containsEntry(
            StorageAccessProperty.AWS_KEY_ID.getPropertyName(), DEFAULT_TOKEN.accessKeyId())
        .containsOnlyKeys(
            StorageAccessProperty.AWS_KEY_ID.getPropertyName(),
            StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(),
            StorageAccessProperty.AWS_TOKEN.getPropertyName(),
            StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS.getPropertyName());
    assertThat(cfg.credentials().get(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName()))
        .isNotBlank()
        .isNotEqualTo(DEFAULT_TOKEN.secretAccessKey());
    assertThat(cfg.extraProperties())
        .containsEntry("s3.endpoint", ENDPOINT)
        .containsEntry("s3.path-style-access", "true")
        .containsEntry("client.region", "auto")
        .containsOnlyKeys("s3.endpoint", "s3.path-style-access", "client.region");
    assertThat(cfg.internalProperties()).isEmpty();
    Instant expected = CLOCK.instant().plusSeconds(3600);
    assertThat(cfg.expiresAt()).contains(expected);
    assertThat(
            cfg.credentials()
                .get(StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS.getPropertyName()))
        .isEqualTo(String.valueOf(expected.toEpochMilli()));
    assertThat(cfg.supportsCredentialVending()).isTrue();
    assertThat(vendedJwt(cfg).getClaim("bucket").asString()).isEqualTo("bucket");
  }

  /** The endpoint is parsed when the credential is minted, not when the key is built. */
  @ParameterizedTest
  @NullSource
  @ValueSource(
      strings = {
        "https://example.com",
        "https://s3.us-east-1.amazonaws.com",
        "http://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com",
        "https://0123456789ABCDEF0123456789ABCDEF.r2.cloudflarestorage.com"
      })
  void aNonR2EndpointFailsAtVendTime(String endpoint) {
    AwsStorageConfigurationInfo config = r2Config().endpoint(endpoint).build();
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, null, config);

    StorageCredentialCacheKey key =
        integration.buildCacheKey(GRANTS, CredentialVendingContext.empty());
    assertThatThrownBy(key::load)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("endpoint");
    assertThatThrownBy(() -> vend(integration, GRANTS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("endpoint");
  }

  /** The path style is checked when the credential is minted, as the endpoint is. */
  @ParameterizedTest
  @NullSource
  @ValueSource(booleans = false)
  void aPathStyleThatIsNotTrueFailsAtVendTime(Boolean pathStyleAccess) {
    assertRefusedAtVendTime(
        r2Config().pathStyleAccess(pathStyleAccess).build(),
        "pathStyleAccess must be true" + FOR_MECHANISM);
  }

  /** The region is checked when the credential is minted, as the endpoint is. */
  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"us-east-1", "AUTO"})
  void aRegionThatIsNotAutoFailsAtVendTime(String region) {
    assertRefusedAtVendTime(
        r2Config().region(region).build(), "region must be 'auto'" + FOR_MECHANISM);
  }

  // ---- parent token resolution ----

  @Test
  void missingDefaultTokenIsRefusedWithoutNamingAProperty() {
    CloudflareR2CredentialsStorageIntegration integration =
        integration(NO_TOKENS, CLOCK, null, UNNAMED_CONFIG);
    assertRefusedForMissingToken(
        integration,
        "No Cloudflare R2 parent token is configured on the server",
        "polaris.storage.cloudflare-r2.access-key");
  }

  @Test
  void flagFalseIgnoresTheStorageNameAndUsesTheDefaultToken() {
    List<String> seen = new ArrayList<>();
    CloudflareR2ParentTokenResolver recording =
        name -> {
          seen.add(name);
          return Optional.of(DEFAULT_TOKEN);
        };
    StorageAccessConfig cfg = vend(integration(recording, CLOCK, null, CONFIG), GRANTS);
    assertThat(seen).hasSize(1).containsOnlyNulls();
    assertThat(vendedKeyId(cfg)).isEqualTo(DEFAULT_TOKEN.accessKeyId());
  }

  @Test
  void flagFalseWithNoDefaultTokenFailsEvenWhenTheNamedTokenExists() {
    CloudflareR2ParentTokenResolver namedOnly =
        name -> "tenant-a".equals(name) ? Optional.of(NAMED_TOKEN) : Optional.empty();
    CloudflareR2CredentialsStorageIntegration integration =
        integration(namedOnly, CLOCK, null, CONFIG);
    assertRefusedForMissingToken(
        integration,
        "No Cloudflare R2 parent token is configured on the server",
        "polaris.storage.cloudflare-r2.access-key");
  }

  @Test
  void flagTrueSelectsTheNamedToken() {
    List<String> seen = new ArrayList<>();
    CloudflareR2ParentTokenResolver recording =
        name -> {
          seen.add(name);
          return RESOLVER.resolve(name);
        };
    StorageAccessConfig cfg =
        vend(integration(recording, CLOCK, null, CONFIG, BY_NAME_REALM_CONFIG), GRANTS);
    assertThat(seen).containsExactly("tenant-a");
    assertThat(vendedKeyId(cfg)).isEqualTo(NAMED_TOKEN.accessKeyId());
  }

  @Test
  void flagTrueWithoutAStorageNameUsesTheDefaultToken() {
    StorageAccessConfig cfg =
        vend(integration(RESOLVER, CLOCK, null, UNNAMED_CONFIG, BY_NAME_REALM_CONFIG), GRANTS);
    assertThat(vendedKeyId(cfg)).isEqualTo(DEFAULT_TOKEN.accessKeyId());
  }

  @Test
  void flagTrueWithAnUnknownStorageNameIsRefusedWithoutNamingAProperty() {
    AwsStorageConfigurationInfo unknown = r2Config().storageName("unknown").build();
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, null, unknown, BY_NAME_REALM_CONFIG);
    assertRefusedForMissingToken(
        integration,
        "Storage name 'unknown' has no Cloudflare R2 parent token configured on the server",
        "polaris.storage.cloudflare-r2.unknown.access-key");
  }

  @Test
  void theParentSecretNeverAppearsInAnErrorMessage() {
    CloudflareR2ParentToken token =
        new CloudflareR2ParentToken("test-r2-access-key", "secret-must-not-leak-1a2b");
    AwsStorageConfigurationInfo notR2 = r2Config().endpoint("https://example.com").build();
    assertThatThrownBy(
            () -> vend(integration(name -> Optional.of(token), CLOCK, null, notR2), GRANTS))
        .isInstanceOf(IllegalArgumentException.class)
        .message()
        .doesNotContain(token.secretAccessKey());
  }

  // ---- grants, buckets and scope ----

  @Test
  void multiBucketGrantsAreRejected() {
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, null, CONFIG);
    List<LocationGrant> grants =
        List.of(
            new LocationGrant(
                Set.of("s3://bucket/a/", "s3://other/b/"), Set.of(PolarisStorageActions.READ)));
    assertThatThrownBy(() -> vend(integration, grants))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("one bucket");
  }

  @Test
  void multiBucketRejectionNamesTheTable() {
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, null, CONFIG);
    List<LocationGrant> grants =
        List.of(
            new LocationGrant(
                Set.of("s3://bucket/a/", "s3://other/b/"), Set.of(PolarisStorageActions.READ)));
    CredentialVendingContext context =
        CredentialVendingContext.builder()
            .realm(Optional.of("realm"))
            .catalogName(Optional.of("c"))
            .namespace(Optional.of("ns"))
            .tableName(Optional.of("t"))
            .build();
    assertThatThrownBy(() -> integration.getStorageAccessConfig(grants, Optional.empty(), context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("c.ns.t")
        .hasMessageContaining("bucket")
        .hasMessageContaining("other");
  }

  @Test
  void twoGrantsInTwoBucketsAreRejected() {
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, null, CONFIG);
    List<LocationGrant> grants =
        List.of(
            grant("s3://bucket/a/", PolarisStorageActions.READ),
            grant("s3://other/b/", PolarisStorageActions.READ));
    assertThatThrownBy(() -> vend(integration, grants))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("one bucket");
  }

  @Test
  void uniformReadOnlyGrantsVendReadOnly() {
    List<LocationGrant> grants =
        List.of(
            grant("s3://bucket/wh/db/t/", PolarisStorageActions.READ),
            grant("s3://bucket/wh/db/u/", PolarisStorageActions.LIST));
    StorageAccessConfig cfg = vend(integration(RESOLVER, CLOCK, null, CONFIG), grants);
    assertThat(vendedScope(cfg))
        .isEqualTo(CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY);
  }

  @Test
  void uniformWriteCapableGrantsVendReadWrite() {
    List<LocationGrant> grants =
        List.of(
            grant("s3://bucket/wh/db/t/", PolarisStorageActions.WRITE),
            grant("s3://bucket/wh/db/u/", PolarisStorageActions.ALL));
    StorageAccessConfig cfg = vend(integration(RESOLVER, CLOCK, null, CONFIG), grants);
    assertThat(vendedScope(cfg))
        .isEqualTo(CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_WRITE);
  }

  @Test
  void mixedReadOnlyAndWriteGrantsAreRejected() {
    List<LocationGrant> grants =
        List.of(
            grant("s3://bucket/wh/db/t/", PolarisStorageActions.READ),
            grant("s3://bucket/wh/db/u/", PolarisStorageActions.WRITE));
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, null, CONFIG);
    CredentialVendingContext context =
        CredentialVendingContext.builder()
            .realm(Optional.of("realm"))
            .catalogName(Optional.of("c"))
            .namespace(Optional.of("ns"))
            .tableName(Optional.of("t"))
            .build();
    assertThatThrownBy(() -> integration.getStorageAccessConfig(grants, Optional.empty(), context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "R2 credentials carry one scope for all prefixes; grants with mixed read-only and"
                + " write actions cannot be vended together for table c.ns.t");
  }

  @Test
  void oneGrantMixingReadAndWriteActionsVendsReadWrite() {
    List<LocationGrant> grants =
        List.of(
            grant("s3://bucket/wh/db/t/", PolarisStorageActions.READ, PolarisStorageActions.WRITE));
    assertThat(CloudflareR2CredentialsStorageIntegration.scopeFor(grants))
        .isEqualTo(CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_WRITE);
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, null, CONFIG);
    assertThatCode(() -> vend(integration, grants)).doesNotThrowAnyException();
  }

  /** Scope is part of the cache key: a WRITE vend and a READ vend for one location never share. */
  @Test
  void writeAndReadGrantsForTheSameLocationGetDistinctCachedTokens() {
    StorageCredentialCache cache = new StorageCredentialCache(() -> 10_000);
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, cache, CONFIG);
    List<LocationGrant> write = List.of(grant("s3://bucket/wh/db/t/", PolarisStorageActions.WRITE));
    List<LocationGrant> read = List.of(grant("s3://bucket/wh/db/t/", PolarisStorageActions.READ));
    StorageAccessConfig writeCfg = vend(integration, write);
    StorageAccessConfig readCfg = vend(integration, read);
    assertThat(cache.getEstimatedSize()).isEqualTo(2);
    assertThat(vendedScope(writeCfg))
        .isEqualTo(CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_WRITE);
    assertThat(vendedScope(readCfg))
        .isEqualTo(CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY);
    String tokenName = StorageAccessProperty.AWS_TOKEN.getPropertyName();
    assertThat(readCfg.credentials().get(tokenName))
        .isNotEqualTo(writeCfg.credentials().get(tokenName));
  }

  // ---- prefixes in the vended token ----

  /** The prefix handed to the signer is the grant's path, with one trailing slash. */
  @ParameterizedTest
  @CsvSource({
    "s3://bucket/wh/db/t/, wh/db/t/",
    "s3://bucket/wh/db/t, wh/db/t/",
  })
  void theVendedPrefixClaimIsTheGrantPath(String location, String expectedPrefix) {
    StorageAccessConfig cfg =
        vend(
            integration(RESOLVER, CLOCK, null, CONFIG),
            List.of(grant(location, PolarisStorageActions.READ)));
    Object prefixes = vendedJwt(cfg).getClaim("paths").asMap().get("prefixPaths");
    assertThat(prefixes).isEqualTo(List.of(expectedPrefix));
  }

  /**
   * The module refuses a grant location with an empty path segment, in the middle of the path,
   * right after the bucket, or at the end. The cache load and a vend both refuse it, and the
   * message names the location.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {"s3://bucket/wh//db/t/", "s3://bucket//x/", "s3://bucket/x//", "s3://bucket//"})
  void aGrantLocationWithAnEmptyPathSegmentIsRefusedAtVendTime(String location) {
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, CLOCK, null, CONFIG);
    List<LocationGrant> grants = List.of(grant(location, PolarisStorageActions.READ));

    assertThatThrownBy(() -> integration.buildCacheKey(grants, CredentialVendingContext.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(emptyPathSegmentMessage(location));
    assertThatThrownBy(() -> vend(integration, grants))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(emptyPathSegmentMessage(location));
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3://bucket", "s3://bucket/"})
  void aBucketRootGrantOmitsThePathsClaim(String location) {
    StorageAccessConfig cfg =
        vend(
            integration(RESOLVER, CLOCK, null, CONFIG),
            List.of(grant(location, PolarisStorageActions.READ)));
    DecodedJWT jwt = vendedJwt(cfg);
    assertThat(jwt.getClaim("paths").isMissing()).isTrue();
    assertThat(jwt.getClaim("bucket").asString()).isEqualTo("bucket");
  }

  // ---- package-private helpers ----

  @Test
  void scopeMapping() {
    assertThat(
            CloudflareR2CredentialsStorageIntegration.scopeFor(
                List.of(grant("s3://b/x/", PolarisStorageActions.READ))))
        .isEqualTo("object-read-only");
    assertThat(
            CloudflareR2CredentialsStorageIntegration.scopeFor(
                List.of(
                    grant("s3://b/x/", PolarisStorageActions.READ),
                    grant("s3://b/x/", PolarisStorageActions.LIST))))
        .isEqualTo("object-read-only");
    assertThat(
            CloudflareR2CredentialsStorageIntegration.scopeFor(
                List.of(grant("s3://b/x/", PolarisStorageActions.WRITE))))
        .isEqualTo("object-read-write");
    assertThat(
            CloudflareR2CredentialsStorageIntegration.scopeFor(
                List.of(grant("s3://b/x/", PolarisStorageActions.DELETE))))
        .isEqualTo("object-read-write");
    assertThat(
            CloudflareR2CredentialsStorageIntegration.scopeFor(
                List.of(grant("s3://b/x/", PolarisStorageActions.ALL))))
        .isEqualTo("object-read-write");
  }

  @Test
  void prefixNormalization() {
    List<LocationGrant> grants =
        List.of(
            new LocationGrant(
                Set.of("s3://b/wh/db/t", "s3://b/wh/db/t/"), Set.of(PolarisStorageActions.READ)),
            grant("s3://b/wh/db/a/", PolarisStorageActions.WRITE));
    assertThat(CloudflareR2CredentialsStorageIntegration.normalizedPrefixes(grants))
        .containsExactly("wh/db/a/", "wh/db/t/");
    assertThat(
            CloudflareR2CredentialsStorageIntegration.normalizedPrefixes(
                List.of(
                    new LocationGrant(
                        Set.of("s3://b", "s3://b/x/"), Set.of(PolarisStorageActions.READ)))))
        .isEmpty();
  }

  @ParameterizedTest
  @CsvSource({
    "s3://b/x/, x/",
    "s3://b/x, x/",
    "s3://b/x/y, x/y/",
    "s3a://b/x/y/, x/y/",
  })
  void prefixIsTheGrantPathWithOneTrailingSlash(String location, String expected) {
    assertThat(
            CloudflareR2CredentialsStorageIntegration.normalizedPrefixes(
                List.of(grant(location, PolarisStorageActions.READ))))
        .containsExactly(expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3://b//x/", "s3://b/x//", "s3://b/x//y/", "s3://b//", "s3a://b//x/"})
  void aLocationWithAnEmptyPathSegmentIsRefused(String location) {
    assertThatThrownBy(
            () ->
                CloudflareR2CredentialsStorageIntegration.normalizedPrefixes(
                    List.of(grant(location, PolarisStorageActions.READ))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(emptyPathSegmentMessage(location));
  }

  /**
   * A bucket-root location scopes the credential to the whole bucket, but it does not hide an empty
   * path segment in another location. The test fixes the order of the locations, so the bucket root
   * comes first. A second case puts the bad location in another grant.
   */
  @Test
  void aBucketRootLocationDoesNotHideAnEmptyPathSegmentInAnotherLocation() {
    Set<String> bucketRootFirst = new LinkedHashSet<>(List.of("s3://b/", "s3://b//x/"));
    List<LocationGrant> sameGrant =
        List.of(new LocationGrant(bucketRootFirst, Set.of(PolarisStorageActions.READ)));
    List<LocationGrant> otherGrant =
        List.of(
            grant("s3://b/", PolarisStorageActions.READ),
            grant("s3://b//x/", PolarisStorageActions.READ));

    assertThatThrownBy(
            () -> CloudflareR2CredentialsStorageIntegration.normalizedPrefixes(sameGrant))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(emptyPathSegmentMessage("s3://b//x/"));
    assertThatThrownBy(
            () -> CloudflareR2CredentialsStorageIntegration.normalizedPrefixes(otherGrant))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(emptyPathSegmentMessage("s3://b//x/"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3://b", "s3://b/"})
  void onlyTheBucketRootScopesToTheWholeBucket(String location) {
    assertThat(
            CloudflareR2CredentialsStorageIntegration.normalizedPrefixes(
                List.of(grant(location, PolarisStorageActions.READ))))
        .isEmpty();
  }

  @Test
  void singleBucketRequired() {
    assertThat(
            CloudflareR2CredentialsStorageIntegration.singleBucket(
                List.of(
                    new LocationGrant(
                        Set.of("s3://b/x/", "s3a://b/y/"), Set.of(PolarisStorageActions.READ))),
                CredentialVendingContext.empty()))
        .isEqualTo("b");
    assertThatThrownBy(
            () ->
                CloudflareR2CredentialsStorageIntegration.singleBucket(
                    List.of(
                        new LocationGrant(
                            Set.of("s3://b1/x/", "s3://b2/y/"),
                            Set.of(PolarisStorageActions.READ))),
                    CredentialVendingContext.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("b1")
        .hasMessageContaining("b2");
  }

  @Test
  void locationWithoutABucketIsRejected() {
    assertThatThrownBy(
            () ->
                CloudflareR2CredentialsStorageIntegration.singleBucket(
                    List.of(grant("s3:///x/", PolarisStorageActions.READ)),
                    CredentialVendingContext.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("s3:///x/")
        .hasMessageContaining("names no bucket");
  }

  // ---- logging, TTL and expiry ----

  @Test
  void mintIsLoggedWithoutCredentialMaterial() {
    String parentKeyId = "parent-key-id-must-not-be-logged-4a1e";
    String parentSecret = "parent-secret-must-not-be-logged-9f3c";
    CloudflareR2ParentTokenResolver resolver =
        name -> Optional.of(new CloudflareR2ParentToken(parentKeyId, parentSecret));
    StorageAccessConfig cfg;
    List<ILoggingEvent> events;
    try (LogCapture logs = new LogCapture(Level.DEBUG)) {
      cfg = vend(integration(resolver, CLOCK, null, CONFIG), GRANTS);
      events = logs.events();
    }
    String sessionToken = cfg.credentials().get(StorageAccessProperty.AWS_TOKEN.getPropertyName());
    String derivedSecret =
        cfg.credentials().get(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName());
    // The raw JWT is not a substring of its base64 session token, and it is a usable credential:
    // the derived secret is its SHA-256.
    String rawJwt = vendedJwt(cfg).getToken();
    assertThat(sessionToken).isNotBlank();
    assertThat(derivedSecret).isNotBlank();
    assertThat(events)
        .extracting(ILoggingEvent::getFormattedMessage)
        .anySatisfy(
            message ->
                assertThat(message)
                    .contains("Minted R2 credential")
                    .contains("storageName=<default>")
                    .contains("bucket=bucket")
                    .contains("prefixes=1")
                    .contains("scope=object-read-write")
                    .contains("ttl=")
                    .contains("expiresAt="))
        .allSatisfy(
            message ->
                assertThat(message)
                    .doesNotContain(parentKeyId)
                    .doesNotContain(parentSecret)
                    .doesNotContain(sessionToken)
                    .doesNotContain(rawJwt)
                    .doesNotContain(derivedSecret));
    assertThat(events)
        .filteredOn(event -> event.getFormattedMessage().contains("Minted R2 credential"))
        .singleElement()
        .extracting(ILoggingEvent::getLevel)
        .isEqualTo(Level.DEBUG);
  }

  /** The log line prints the effective name: the catalog's name only when the flag is true. */
  @Test
  void mintIsLoggedWithTheEffectiveStorageNameWhenResolvingByName() {
    List<ILoggingEvent> events;
    try (LogCapture logs = new LogCapture(Level.DEBUG)) {
      vend(integration(RESOLVER, CLOCK, null, CONFIG, BY_NAME_REALM_CONFIG), GRANTS);
      events = logs.events();
    }
    assertThat(events)
        .extracting(ILoggingEvent::getFormattedMessage)
        .anySatisfy(message -> assertThat(message).contains("storageName=tenant-a"));
  }

  @Test
  void mintOfTheDefaultStorageNameIsLoggedAsDefault() {
    List<ILoggingEvent> events;
    try (LogCapture logs = new LogCapture(Level.DEBUG)) {
      vend(integration(RESOLVER, CLOCK, null, UNNAMED_CONFIG), GRANTS);
      events = logs.events();
    }
    assertThat(events)
        .extracting(ILoggingEvent::getFormattedMessage)
        .anySatisfy(message -> assertThat(message).contains("storageName=<default>"));
  }

  /** The credential lifetime comes from the realm's STORAGE_CREDENTIAL_DURATION_SECONDS. */
  @Test
  void ttlComesFromTheRealmConfig() {
    RealmConfig tenMinutes =
        realmConfig(Map.of(FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS.key(), 600));
    StorageAccessConfig cfg = vend(integration(RESOLVER, CLOCK, null, CONFIG, tenMinutes), GRANTS);
    DecodedJWT jwt = vendedJwt(cfg);
    Instant expected = CLOCK.instant().plusSeconds(600);
    assertThat(jwt.getExpiresAtAsInstant()).isEqualTo(expected);
    assertThat(
            jwt.getExpiresAtAsInstant().getEpochSecond()
                - jwt.getIssuedAtAsInstant().getEpochSecond())
        .isEqualTo(600);
    assertThat(cfg.expiresAt()).contains(expected);
  }

  @Test
  void expiredCredentialIsNotReused() {
    // A clock two hours behind the wall clock mints credentials whose expiry is already past, so
    // StorageCredentialCache computes a zero TTL and the second call has to mint again.
    Clock stale = Clock.fixed(Instant.now().minus(2, ChronoUnit.HOURS), ZoneOffset.UTC);
    StorageCredentialCache cache = new StorageCredentialCache(() -> 10_000);
    CloudflareR2CredentialsStorageIntegration integration =
        integration(RESOLVER, stale, cache, CONFIG);
    StorageAccessConfig first = vend(integration, GRANTS);
    StorageAccessConfig second = vend(integration, GRANTS);
    assertThat(second).isNotSameAs(first);
  }
}
