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

import static org.apache.polaris.core.config.RealmConfigurationSource.EMPTY_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.auth0.jwt.JWT;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.ImmutableAwsStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

class CloudflareR2CredentialCacheKeyTest {

  private static final String MECHANISM = "R2_LOCAL_SIGNING";
  private static final String ACCOUNT = "0123456789abcdef0123456789abcdef";
  private static final String ENDPOINT = "https://" + ACCOUNT + ".r2.cloudflarestorage.com";
  private static final RealmConfig REALM_CONFIG = new RealmConfigImpl(EMPTY_CONFIG, () -> "r");
  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-09-01T12:00:00Z"), ZoneOffset.UTC);
  private static final Duration TTL = Duration.ofSeconds(3600);
  private static final String READ_ONLY =
      CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY;
  private static final String READ_WRITE =
      CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_WRITE;

  private static ImmutableAwsStorageConfigurationInfo.Builder r2Config(String endpoint) {
    return AwsStorageConfigurationInfo.builder()
        .credentialVendingMechanism(MECHANISM)
        .endpoint(endpoint)
        .pathStyleAccess(true)
        .region("auto")
        .addAllowedLocation("s3://bucket/");
  }

  private static final AwsStorageConfigurationInfo CONFIG = r2Config(ENDPOINT).build();

  private static CloudflareR2CredentialCacheKey key(
      String realmId,
      AwsStorageConfigurationInfo config,
      String bucket,
      List<String> prefixes,
      String scope,
      Optional<String> storageName,
      String parentKeyId,
      String secret,
      Duration ttl) {
    return CloudflareR2CredentialCacheKey.of(
        realmId,
        config,
        bucket,
        prefixes,
        scope,
        storageName,
        parentKeyId,
        secret,
        ttl,
        CLOCK,
        REALM_CONFIG);
  }

  private static CloudflareR2CredentialCacheKey keyFor(String secret) {
    return key(
        "realm",
        CONFIG,
        "bucket",
        List.of("p/"),
        READ_ONLY,
        Optional.empty(),
        "parent-key",
        secret,
        TTL);
  }

  @Test
  void equalityIncludesTheSecret() {
    assertThat(keyFor("s1")).isEqualTo(keyFor("s1")).hasSameHashCodeAs(keyFor("s1"));
    assertThat(keyFor("s1")).isNotEqualTo(keyFor("s2"));
  }

  @Test
  void theClockAndTheRealmConfigDoNotAffectEquality() {
    RealmConfig otherRealmConfig = new RealmConfigImpl(EMPTY_CONFIG, () -> "other");
    CloudflareR2CredentialCacheKey other =
        CloudflareR2CredentialCacheKey.of(
            "realm",
            CONFIG,
            "bucket",
            List.of("p/"),
            READ_ONLY,
            Optional.empty(),
            "parent-key",
            "s1",
            TTL,
            Clock.systemUTC(),
            otherRealmConfig);
    assertThat(keyFor("s1")).isEqualTo(other).hasSameHashCodeAs(other);
  }

  @Test
  void everyVendingInputIsPartOfTheKey() {
    AwsStorageConfigurationInfo otherAccount =
        r2Config("https://ffffffffffffffffffffffffffffffff.r2.cloudflarestorage.com").build();
    CloudflareR2CredentialCacheKey base = keyFor("s1");
    assertThat(base)
        .isNotEqualTo(
            key(
                "other",
                CONFIG,
                "bucket",
                List.of("p/"),
                READ_ONLY,
                Optional.empty(),
                "parent-key",
                "s1",
                TTL))
        .isNotEqualTo(
            key(
                "realm",
                otherAccount,
                "bucket",
                List.of("p/"),
                READ_ONLY,
                Optional.empty(),
                "parent-key",
                "s1",
                TTL))
        .isNotEqualTo(
            key(
                "realm",
                CONFIG,
                "other",
                List.of("p/"),
                READ_ONLY,
                Optional.empty(),
                "parent-key",
                "s1",
                TTL))
        .isNotEqualTo(
            key(
                "realm",
                CONFIG,
                "bucket",
                List.of("q/"),
                READ_ONLY,
                Optional.empty(),
                "parent-key",
                "s1",
                TTL))
        .isNotEqualTo(
            key(
                "realm",
                CONFIG,
                "bucket",
                List.of("p/"),
                READ_WRITE,
                Optional.empty(),
                "parent-key",
                "s1",
                TTL))
        .isNotEqualTo(
            key(
                "realm",
                CONFIG,
                "bucket",
                List.of("p/"),
                READ_ONLY,
                Optional.empty(),
                "other-key",
                "s1",
                TTL))
        .isNotEqualTo(
            key(
                "realm",
                CONFIG,
                "bucket",
                List.of("p/"),
                READ_ONLY,
                Optional.empty(),
                "parent-key",
                "s1",
                Duration.ofSeconds(600)))
        .isNotEqualTo(
            key(
                "realm",
                CONFIG,
                "bucket",
                List.of("p/"),
                READ_ONLY,
                Optional.empty(),
                "parent-key",
                "s2",
                TTL))
        .isNotEqualTo(
            key(
                "realm",
                CONFIG,
                "bucket",
                List.of("p/"),
                READ_ONLY,
                Optional.of("prod"),
                "parent-key",
                "s1",
                TTL));
  }

  @Test
  void theKeyHasNoRefreshEndpointComponent() {
    assertThat(
            Arrays.stream(CloudflareR2CredentialCacheKey.class.getMethods()).map(Method::getName))
        .doesNotContain("refreshCredentialsEndpoint");
  }

  @Test
  void toStringNeverShowsTheSecret() {
    String secret = "very-secret-value";
    String text = keyFor(secret).toString();
    assertThat(text).contains("parent-key").doesNotContain(secret);
  }

  @Test
  void loadUsesTheJurisdictionEndpointAndAudience() {
    String expectedHost = ACCOUNT + ".eu.r2.cloudflarestorage.com";
    AwsStorageConfigurationInfo eu = r2Config("https://" + expectedHost).build();
    StorageAccessConfig cfg =
        key(
                "realm",
                eu,
                "bucket",
                List.of("wh/t1/"),
                READ_WRITE,
                Optional.empty(),
                "parent-key",
                "secret",
                TTL)
            .load();
    assertThat(cfg.extraProperties()).containsEntry("s3.endpoint", "https://" + expectedHost);

    String sessionToken = cfg.credentials().get(StorageAccessProperty.AWS_TOKEN.getPropertyName());
    String decoded = new String(Base64.getDecoder().decode(sessionToken), StandardCharsets.UTF_8);
    String jwt =
        decoded.substring(CloudflareR2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX.length());
    assertThat(JWT.decode(jwt).getAudience()).containsExactly(expectedHost);
    assertThat(JWT.decode(jwt).getSubject()).isEqualTo(ACCOUNT);
  }

  /**
   * The client contract: the credential triple plus the expiry as credentials, and the endpoint,
   * path style and region as extra properties, taken from the catalog's explicit values. The key
   * never emits a refresh endpoint; the integration adds it after the cache lookup.
   */
  @Test
  void loadEmitsCredentialsAndExtraPropertiesWithoutARefreshEndpoint() {
    StorageAccessConfig cfg =
        key(
                "realm",
                CONFIG,
                "bucket",
                List.of("wh/t1/"),
                READ_WRITE,
                Optional.empty(),
                "parent-key",
                "secret",
                TTL)
            .load();
    assertThat(cfg.credentials())
        .containsEntry(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), "parent-key")
        .containsOnlyKeys(
            StorageAccessProperty.AWS_KEY_ID.getPropertyName(),
            StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(),
            StorageAccessProperty.AWS_TOKEN.getPropertyName(),
            StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS.getPropertyName());
    assertThat(cfg.credentials().get(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName()))
        .isNotEqualTo("secret");
    assertThat(cfg.extraProperties())
        .containsEntry("s3.endpoint", ENDPOINT)
        .containsEntry("s3.path-style-access", "true")
        .containsEntry("client.region", "auto")
        .containsOnlyKeys("s3.endpoint", "s3.path-style-access", "client.region");
    assertThat(cfg.internalProperties()).isEmpty();
    assertThat(cfg.expiresAt()).contains(Instant.parse("2026-09-01T13:00:00Z"));
    assertThat(cfg.supportsCredentialVending()).isTrue();
  }

  @Test
  void loadFailsClosedOnANonR2Endpoint() {
    AwsStorageConfigurationInfo notR2 = r2Config("https://example.com").build();
    CloudflareR2CredentialCacheKey notR2Key =
        key(
            "realm",
            notR2,
            "bucket",
            List.of("p/"),
            READ_ONLY,
            Optional.empty(),
            "parent-key",
            "s",
            TTL);
    assertThatThrownBy(notR2Key::load)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("endpoint");
  }
}
