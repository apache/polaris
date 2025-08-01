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
package org.apache.polaris.service.storage.sign;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.crypto.SecretKey;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.storage.StorageLocation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class RemoteSigningTokenServiceTest {

  private static final String TEST_SECRET_KEY_1 =
      Base64.getEncoder()
          .encodeToString(
              "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                  .getBytes(StandardCharsets.UTF_8));

  private static final String TEST_SECRET_KEY_2 =
      Base64.getEncoder()
          .encodeToString(
              "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
                  .getBytes(StandardCharsets.UTF_8));

  private static final String TEST_KEY_ID_1 = "test-key-1";

  private static final String TEST_KEY_ID_2 = "test-key-2";

  private static final Duration TOKEN_LIFESPAN = Duration.ofMinutes(15);

  private static final TableIdentifier TEST_TABLE =
      TableIdentifier.of(Namespace.of("ns1"), "table1");

  private Clock clock;
  private RemoteSigningConfiguration configuration;

  @BeforeEach
  void setUp() {
    clock = Clock.fixed(Instant.parse("2026-01-15T10:00:00Z"), ZoneOffset.UTC);
    configuration = mock(RemoteSigningConfiguration.class);
    when(configuration.keys()).thenReturn(Map.of(TEST_KEY_ID_1, TEST_SECRET_KEY_1));
    when(configuration.keyFiles()).thenReturn(Map.of());
    when(configuration.currentKeyId()).thenReturn(TEST_KEY_ID_1);
    when(configuration.tokenLifespan()).thenReturn(TOKEN_LIFESPAN);
    when(configuration.encryptionMethod()).thenReturn("A256CBC-HS512");
  }

  @Test
  void testEncryptDecrypt() {
    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);

    ImmutableRemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("test-principal")
            .catalogName("test-catalog")
            .tableIdentifier(TEST_TABLE)
            .allowedLocations(
                Set.of(
                    StorageLocation.of("s3://bucket/path1"),
                    StorageLocation.of("s3://bucket/path2")))
            .readWrite(true)
            .build();

    String encrypted = service.encrypt(token);
    assertThat(encrypted).isNotBlank();

    // Token should be a valid JWE (5 parts separated by dots)
    assertThat(encrypted.split("\\.")).hasSize(5);

    RemoteSigningToken decrypted = service.decrypt(encrypted);
    assertThat(decrypted.principalName()).isEqualTo(token.principalName());
    assertThat(decrypted.catalogName()).isEqualTo(token.catalogName());
    assertThat(decrypted.tableIdentifier()).isEqualTo(token.tableIdentifier());
    assertThat(decrypted.allowedLocations()).isEqualTo(token.allowedLocations());
    assertThat(decrypted.readWrite()).isEqualTo(token.readWrite());
  }

  @Test
  void testExpiredToken() {
    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);

    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("test-principal")
            .catalogName("test-catalog")
            .tableIdentifier(TEST_TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://bucket/path/")))
            .readWrite(false)
            .build();

    String encrypted = service.encrypt(token);

    // Create a new service with a clock that's past the expiration time
    Clock expiredClock =
        Clock.fixed(Instant.parse("2026-01-15T10:20:00Z"), ZoneOffset.UTC); // 20 min later
    RemoteSigningTokenService expiredService =
        new RemoteSigningTokenService(configuration, expiredClock);

    assertThatThrownBy(() -> expiredService.decrypt(encrypted))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid or expired remote signing token");
  }

  @Test
  void testUnknownKeyId() {
    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);

    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("test-principal")
            .catalogName("test-catalog")
            .tableIdentifier(TEST_TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://bucket/path/")))
            .readWrite(false)
            .build();

    String encrypted = service.encrypt(token);

    // Create a service with a different key ID (simulating the original key was removed)
    RemoteSigningConfiguration differentConfig = mock(RemoteSigningConfiguration.class);
    when(differentConfig.keys()).thenReturn(Map.of("different-key", TEST_SECRET_KEY_2));
    when(differentConfig.keyFiles()).thenReturn(Map.of());
    when(differentConfig.currentKeyId()).thenReturn("different-key");
    when(differentConfig.tokenLifespan()).thenReturn(TOKEN_LIFESPAN);
    when(differentConfig.encryptionMethod()).thenReturn("A256CBC-HS512");

    RemoteSigningTokenService differentService =
        new RemoteSigningTokenService(differentConfig, clock);

    // Token was encrypted with TEST_KEY_ID_1, but that key is not in the new service
    assertThatThrownBy(() -> differentService.decrypt(encrypted))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown key ID: test-key-1");
  }

  @Test
  void testKeyRotation() {
    // Create a token with key1
    RemoteSigningTokenService service1 = new RemoteSigningTokenService(configuration, clock);

    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("test-principal")
            .catalogName("test-catalog")
            .tableIdentifier(TEST_TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://bucket/path/")))
            .readWrite(false)
            .build();

    String tokenWithKey1 = service1.encrypt(token);

    // Now simulate key rotation: add key2 and make it the current key, but keep key1 for decryption
    RemoteSigningConfiguration rotatedConfig = mock(RemoteSigningConfiguration.class);
    when(rotatedConfig.keys())
        .thenReturn(Map.of(TEST_KEY_ID_1, TEST_SECRET_KEY_1, TEST_KEY_ID_2, TEST_SECRET_KEY_2));
    when(rotatedConfig.keyFiles()).thenReturn(Map.of());
    when(rotatedConfig.currentKeyId()).thenReturn(TEST_KEY_ID_2);
    when(rotatedConfig.tokenLifespan()).thenReturn(TOKEN_LIFESPAN);
    when(rotatedConfig.encryptionMethod()).thenReturn("A256CBC-HS512");

    RemoteSigningTokenService service2 = new RemoteSigningTokenService(rotatedConfig, clock);

    // Old token (encrypted with key1) should still be decryptable
    RemoteSigningToken decrypted1 = service2.decrypt(tokenWithKey1);
    assertThat(decrypted1.catalogName()).isEqualTo("test-catalog");

    // New tokens should be encrypted with key2
    String tokenWithKey2 = service2.encrypt(token);
    RemoteSigningToken decrypted2 = service2.decrypt(tokenWithKey2);
    assertThat(decrypted2.catalogName()).isEqualTo("test-catalog");

    // The original service (with only key1) should NOT be able to decrypt tokens from key2
    assertThatThrownBy(() -> service1.decrypt(tokenWithKey2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown key ID: test-key-2");
  }

  @Test
  void testMissingKeysGeneratesRandomKey() {
    RemoteSigningConfiguration noKeyConfig = mock(RemoteSigningConfiguration.class);
    when(noKeyConfig.keys()).thenReturn(Map.of());
    when(noKeyConfig.keyFiles()).thenReturn(Map.of());
    when(noKeyConfig.currentKeyId()).thenReturn("default");
    when(noKeyConfig.encryptionMethod()).thenReturn("A256CBC-HS512");
    RemoteSigningTokenService service = new RemoteSigningTokenService(noKeyConfig, clock);
    assertThat(service)
        .extracting("encryptionKeys", map(String.class, SecretKey.class))
        .hasSize(1)
        .containsKey("default");
  }

  @Test
  void testCurrentKeyIdNotFound() {
    RemoteSigningConfiguration badConfig = mock(RemoteSigningConfiguration.class);
    when(badConfig.keys()).thenReturn(Map.of("key1", TEST_SECRET_KEY_1, "key2", TEST_SECRET_KEY_2));
    when(badConfig.keyFiles()).thenReturn(Map.of());
    when(badConfig.currentKeyId()).thenReturn("nonexistent-key");
    when(badConfig.encryptionMethod()).thenReturn("A256CBC-HS512");
    assertThatThrownBy(() -> new RemoteSigningTokenService(badConfig, clock))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Current key ID 'nonexistent-key' not found in configured keys. Available key IDs: key1, key2");
  }

  @Test
  void testKeyWrongSize() {
    String shortKey =
        Base64.getEncoder()
            .encodeToString("0123456789abcdef".getBytes(StandardCharsets.UTF_8)); // Only 16 bytes
    RemoteSigningConfiguration shortKeyConfig = mock(RemoteSigningConfiguration.class);
    when(shortKeyConfig.keys()).thenReturn(Map.of("short-key", shortKey));
    when(shortKeyConfig.keyFiles()).thenReturn(Map.of());
    when(shortKeyConfig.currentKeyId()).thenReturn("short-key");
    when(shortKeyConfig.encryptionMethod()).thenReturn("A256CBC-HS512");
    assertThatThrownBy(() -> new RemoteSigningTokenService(shortKeyConfig, clock))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Secret key 'short-key' must be exactly 64 bytes for A256CBC-HS512 encryption, but was 16 bytes.");
  }

  @ParameterizedTest
  @MethodSource("encryptionMethods")
  void testEncryptionMethod(String encryptionMethod, int keyLengthBytes) {
    byte[] keyBytes = new byte[keyLengthBytes];
    new SecureRandom().nextBytes(keyBytes);
    String encodedKey = Base64.getEncoder().encodeToString(keyBytes);

    RemoteSigningConfiguration config = mock(RemoteSigningConfiguration.class);
    when(config.keys()).thenReturn(Map.of("key", encodedKey));
    when(config.keyFiles()).thenReturn(Map.of());
    when(config.currentKeyId()).thenReturn("key");
    when(config.tokenLifespan()).thenReturn(TOKEN_LIFESPAN);
    when(config.encryptionMethod()).thenReturn(encryptionMethod);

    RemoteSigningTokenService service = new RemoteSigningTokenService(config, clock);

    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("test-principal")
            .catalogName("test-catalog")
            .tableIdentifier(TEST_TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://bucket/path/")))
            .readWrite(false)
            .build();

    String encrypted = service.encrypt(token);
    RemoteSigningToken decrypted = service.decrypt(encrypted);
    assertThat(decrypted.catalogName()).isEqualTo("test-catalog");
  }

  static Stream<Arguments> encryptionMethods() {
    return Stream.of(
        Arguments.of("A128GCM", 16),
        Arguments.of("A192GCM", 24),
        Arguments.of("A256GCM", 32),
        Arguments.of("A128CBC-HS256", 32),
        Arguments.of("A192CBC-HS384", 48),
        Arguments.of("A256CBC-HS512", 64));
  }

  @Test
  void testUnsupportedEncryptionMethod() {
    RemoteSigningConfiguration badConfig = mock(RemoteSigningConfiguration.class);
    when(badConfig.keys()).thenReturn(Map.of("key", TEST_SECRET_KEY_1));
    when(badConfig.keyFiles()).thenReturn(Map.of());
    when(badConfig.currentKeyId()).thenReturn("key");
    when(badConfig.encryptionMethod()).thenReturn("INVALID_METHOD");

    assertThatThrownBy(() -> new RemoteSigningTokenService(badConfig, clock))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Unsupported encryption method: INVALID_METHOD. Supported methods are: A128CBC-HS256, A192CBC-HS384, A256CBC-HS512, A128GCM, A192GCM, A256GCM");
  }

  @ParameterizedTest
  @ValueSource(strings = {"PT0S", "-PT1H"})
  void testUnexpirableToken(Duration tokenLifespan) {
    RemoteSigningConfiguration unexpirableConfig = mock(RemoteSigningConfiguration.class);
    when(unexpirableConfig.keys()).thenReturn(Map.of(TEST_KEY_ID_1, TEST_SECRET_KEY_1));
    when(unexpirableConfig.keyFiles()).thenReturn(Map.of());
    when(unexpirableConfig.currentKeyId()).thenReturn(TEST_KEY_ID_1);
    when(unexpirableConfig.tokenLifespan()).thenReturn(tokenLifespan);
    when(unexpirableConfig.encryptionMethod()).thenReturn("A256CBC-HS512");

    RemoteSigningTokenService service = new RemoteSigningTokenService(unexpirableConfig, clock);

    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("test-principal")
            .catalogName("test-catalog")
            .tableIdentifier(TEST_TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://bucket/path/")))
            .readWrite(false)
            .build();

    String encrypted = service.encrypt(token);

    // Token should be decryptable even far in the future
    Clock futureClock =
        Clock.fixed(Instant.parse("2050-01-15T10:00:00Z"), ZoneOffset.UTC); // 24 years later
    RemoteSigningTokenService futureService =
        new RemoteSigningTokenService(unexpirableConfig, futureClock);

    RemoteSigningToken decrypted = futureService.decrypt(encrypted);
    assertThat(decrypted.catalogName()).isEqualTo("test-catalog");
  }

  @Test
  void testUnexpirableTokenNotAllowed() {
    // Default configuration does not allow unexpirable tokens
    when(configuration.tokenLifespan()).thenReturn(Duration.ofHours(1));

    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);

    // Create a token with unexpirable config
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .principalName("test-principal")
            .catalogName("test-catalog")
            .tableIdentifier(TEST_TABLE)
            .allowedLocations(Set.of(StorageLocation.of("s3://bucket/path/")))
            .readWrite(false)
            .build();

    RemoteSigningConfiguration unexpirableConfig = mock(RemoteSigningConfiguration.class);
    when(unexpirableConfig.keys()).thenReturn(Map.of(TEST_KEY_ID_1, TEST_SECRET_KEY_1));
    when(unexpirableConfig.keyFiles()).thenReturn(Map.of());
    when(unexpirableConfig.currentKeyId()).thenReturn(TEST_KEY_ID_1);
    when(unexpirableConfig.tokenLifespan()).thenReturn(Duration.ZERO);
    when(unexpirableConfig.encryptionMethod()).thenReturn("A256CBC-HS512");

    RemoteSigningTokenService unexpirableService =
        new RemoteSigningTokenService(unexpirableConfig, clock);
    String unexpirableToken = unexpirableService.encrypt(token);

    // Service with default config should reject unexpirable tokens
    assertThatThrownBy(() -> service.decrypt(unexpirableToken))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Token is missing expiration time");
  }
}
