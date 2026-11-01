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
package org.apache.polaris.service.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

public class StorageConfigurationTest {

  private static final String TEST_ACCESS_KEY = "test-access-key";
  private static final String TEST_GCP_TOKEN = "ya29.test-token";
  private static final String TEST_SECRET_KEY = "test-secret-key";
  private static final String STORAGE_ACCESS_KEY = "storage-access-key";
  private static final String STORAGE_SECRET_KEY = "storage-secret-key";
  private static final Duration TEST_TOKEN_LIFESPAN = Duration.ofMinutes(20);

  private StorageConfiguration configWithAwsCredentialsAndGcpToken() {
    return new StorageConfiguration() {
      @Override
      public AwsStorageConfig aws() {
        return new AwsStorageConfig() {
          @Override
          public Optional<String> accessKey() {
            return Optional.of(TEST_ACCESS_KEY);
          }

          @Override
          public Optional<String> secretKey() {
            return Optional.of(TEST_SECRET_KEY);
          }

          @Override
          public Map<String, StorageConfig> storages() {
            return Map.of();
          }
        };
      }

      @Override
      public GcpStorageConfig gcp() {
        return Map::of;
      }

      @Override
      public Optional<String> gcpAccessToken() {
        return Optional.of(TEST_GCP_TOKEN);
      }

      @Override
      public Optional<Duration> gcpAccessTokenLifespan() {
        return Optional.of(TEST_TOKEN_LIFESPAN);
      }

      @Override
      public OptionalInt clientsCacheMaxSize() {
        return OptionalInt.empty();
      }

      @Override
      public OptionalInt maxHttpConnections() {
        return OptionalInt.empty();
      }

      @Override
      public Optional<Duration> readTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionAcquisitionTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionMaxIdleTime() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionTimeToLive() {
        return Optional.empty();
      }

      @Override
      public Optional<Boolean> expectContinueEnabled() {
        return Optional.empty();
      }
    };
  }

  private StorageConfiguration configWithoutGcpToken() {
    return new StorageConfiguration() {
      @Override
      public AwsStorageConfig aws() {
        return new AwsStorageConfig() {
          @Override
          public Optional<String> accessKey() {
            return Optional.empty();
          }

          @Override
          public Optional<String> secretKey() {
            return Optional.empty();
          }

          @Override
          public Map<String, StorageConfig> storages() {
            return Map.of();
          }
        };
      }

      @Override
      public GcpStorageConfig gcp() {
        return Map::of;
      }

      @Override
      public Optional<String> gcpAccessToken() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> gcpAccessTokenLifespan() {
        return Optional.empty();
      }

      @Override
      public OptionalInt clientsCacheMaxSize() {
        return OptionalInt.empty();
      }

      @Override
      public OptionalInt maxHttpConnections() {
        return OptionalInt.empty();
      }

      @Override
      public Optional<Duration> readTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionAcquisitionTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionMaxIdleTime() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionTimeToLive() {
        return Optional.empty();
      }

      @Override
      public Optional<Boolean> expectContinueEnabled() {
        return Optional.empty();
      }
    };
  }

  private StorageConfiguration configWithNamedStorage() {
    return new StorageConfiguration() {
      @Override
      public AwsStorageConfig aws() {
        return new AwsStorageConfig() {
          @Override
          public Optional<String> accessKey() {
            return Optional.of(TEST_ACCESS_KEY);
          }

          @Override
          public Optional<String> secretKey() {
            return Optional.of(TEST_SECRET_KEY);
          }

          @Override
          public Map<String, StorageConfig> storages() {
            return Map.of(
                "myStorage",
                new StorageConfig() {
                  @Override
                  public String accessKey() {
                    return STORAGE_ACCESS_KEY;
                  }

                  @Override
                  public String secretKey() {
                    return STORAGE_SECRET_KEY;
                  }
                });
          }
        };
      }

      @Override
      public GcpStorageConfig gcp() {
        return Map::of;
      }

      @Override
      public Optional<String> gcpAccessToken() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> gcpAccessTokenLifespan() {
        return Optional.empty();
      }

      @Override
      public OptionalInt clientsCacheMaxSize() {
        return OptionalInt.empty();
      }

      @Override
      public OptionalInt maxHttpConnections() {
        return OptionalInt.empty();
      }

      @Override
      public Optional<Duration> readTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionAcquisitionTimeout() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionMaxIdleTime() {
        return Optional.empty();
      }

      @Override
      public Optional<Duration> connectionTimeToLive() {
        return Optional.empty();
      }

      @Override
      public Optional<Boolean> expectContinueEnabled() {
        return Optional.empty();
      }
    };
  }

  @Test
  public void testSingletonStsClientWithStaticCredentials() {
    StsClientBuilder mockBuilder = mock(StsClientBuilder.class);
    StsClient mockStsClient = mock(StsClient.class);
    ArgumentCaptor<StaticCredentialsProvider> credsCaptor =
        ArgumentCaptor.forClass(StaticCredentialsProvider.class);

    when(mockBuilder.credentialsProvider(credsCaptor.capture())).thenReturn(mockBuilder);
    when(mockBuilder.region(any())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockStsClient);

    try (MockedStatic<StsClient> staticMock = Mockito.mockStatic(StsClient.class)) {
      staticMock.when(StsClient::builder).thenReturn(mockBuilder);

      StorageConfiguration config = configWithAwsCredentialsAndGcpToken();
      Supplier<StsClient> supplier = config.stsClientSupplier(true);
      StsClient client1 = supplier.get();
      StsClient client2 = supplier.get();

      assertThat(client1).isSameAs(client2);
      assertThat(client1).isNotNull();

      StaticCredentialsProvider credentialsProvider = credsCaptor.getValue();
      assertThat(credentialsProvider.resolveCredentials().accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
      assertThat(credentialsProvider.resolveCredentials().secretAccessKey())
          .isEqualTo(TEST_SECRET_KEY);
    }
  }

  @Test
  public void testStaticStsCredentials() {
    StorageConfiguration config = configWithAwsCredentialsAndGcpToken();
    AwsCredentialsProvider credentialsProvider = config.stsCredentials();
    assertThat(credentialsProvider).isInstanceOf(StaticCredentialsProvider.class);
    assertThat(credentialsProvider.resolveCredentials().accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
    assertThat(credentialsProvider.resolveCredentials().secretAccessKey())
        .isEqualTo(TEST_SECRET_KEY);
  }

  @Test
  public void testCreateGcpCredentialsFromStaticToken() {
    Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
    Supplier<GoogleCredentials> supplier =
        configWithAwsCredentialsAndGcpToken().gcpCredentialsSupplier(clock);

    GoogleCredentials credentials = supplier.get();
    assertThat(credentials).isNotNull();

    AccessToken accessToken = credentials.getAccessToken();
    assertThat(accessToken).isNotNull();
    assertThat(accessToken.getTokenValue()).isEqualTo(TEST_GCP_TOKEN);
    assertThat(accessToken.getExpirationTime())
        .isEqualTo(clock.instant().plus(Duration.ofMinutes(20)));
  }

  @Test
  public void testGcpCredentialsFromDefault() {
    GoogleCredentials mockDefaultCreds = mock(GoogleCredentials.class);

    try (MockedStatic<GoogleCredentials> mockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {

      mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockDefaultCreds);

      Supplier<GoogleCredentials> supplier =
          configWithoutGcpToken().gcpCredentialsSupplier(Clock.systemUTC());
      GoogleCredentials result = supplier.get();

      assertThat(result).isSameAs(mockDefaultCreds);
      mockedStatic.verify(GoogleCredentials::getApplicationDefault, times(1));
    }
  }

  private static StorageConfiguration configFrom(Map<String, String> properties) {
    return new SmallRyeConfigBuilder()
        .withMapping(StorageConfiguration.class)
        .withSources(new PropertiesConfigSource(properties, "test", 100))
        .build()
        .getConfigMapping(StorageConfiguration.class);
  }

  @Test
  public void testNamedGcpStoragesDoNotChangeDefaultGcpProperties() {
    StorageConfiguration config =
        configFrom(
            Map.of(
                "polaris.storage.gcp.token", TEST_GCP_TOKEN,
                "polaris.storage.gcp.lifespan", "PT20M",
                "polaris.storage.gcp.team-a.credentials-file", "/secrets/a.json",
                "polaris.storage.gcp.team-b.credentials-file", "/secrets/b.json"));

    assertThat(config.gcpAccessToken()).contains(TEST_GCP_TOKEN);
    assertThat(config.gcpAccessTokenLifespan()).contains(TEST_TOKEN_LIFESPAN);
    assertThat(config.gcpNamedCredentialsSuppliers()).containsOnlyKeys("team-a", "team-b");
  }

  @Test
  public void testNoNamedGcpStoragesByDefault() {
    StorageConfiguration config = configFrom(Map.of("polaris.storage.gcp.token", TEST_GCP_TOKEN));

    assertThat(config.gcpNamedCredentialsSuppliers()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"token", "lifespan"})
  public void testReservedGcpStorageNamesAreNotResolvable(String reserved) {
    StorageConfiguration config =
        configFrom(
            Map.of("polaris.storage.gcp." + reserved + ".credentials-file", "/secrets.json"));

    assertThat(config.gcpNamedCredentialsSuppliers()).isEmpty();
  }

  private static String serviceAccountJson(String privateKey) throws Exception {
    return new ObjectMapper()
        .writeValueAsString(
            Map.of(
                "type", "service_account",
                "project_id", "project",
                "private_key_id", "key-id",
                "private_key", privateKey,
                "client_email", "named@project.iam.gserviceaccount.com",
                "client_id", "client-id"));
  }

  private static String generatedPrivateKeyPem() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    String encoded =
        Base64.getMimeEncoder(64, "\n".getBytes(UTF_8))
            .encodeToString(generator.generateKeyPair().getPrivate().getEncoded());
    return "-----BEGIN PRIVATE KEY-----\n" + encoded + "\n-----END PRIVATE KEY-----\n";
  }

  private static Supplier<GoogleCredentials> namedSupplier(Path credentialsFile) {
    return configFrom(
            Map.of("polaris.storage.gcp.team-a.credentials-file", credentialsFile.toString()))
        .gcpNamedCredentialsSuppliers()
        .get("team-a");
  }

  @Test
  public void testNamedGcpServiceAccountFileLoadsOnceWithoutExposingTheKey(@TempDir Path dir)
      throws Exception {
    String privateKey = generatedPrivateKeyPem();
    Path file = Files.writeString(dir.resolve("a.json"), serviceAccountJson(privateKey));
    Supplier<GoogleCredentials> supplier = namedSupplier(file);

    GoogleCredentials credentials = supplier.get();

    assertThat(credentials).isInstanceOf(ServiceAccountCredentials.class);
    assertThat(((ServiceAccountCredentials) credentials).getClientEmail())
        .isEqualTo("named@project.iam.gserviceaccount.com");
    assertThat(supplier.get()).isSameAs(credentials);
    assertThat(credentials.toString()).doesNotContain(privateKey.lines().toList().get(1));
  }

  @Test
  public void testNamedGcpWorkloadIdentityFederationFileLoads(@TempDir Path dir) throws Exception {
    String json =
        new ObjectMapper()
            .writeValueAsString(
                Map.of(
                    "type", "external_account",
                    "audience",
                        "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/q",
                    "subject_token_type", "urn:ietf:params:oauth:token-type:jwt",
                    "token_url", "https://sts.googleapis.com/v1/token",
                    "credential_source", Map.of("file", "/var/run/token")));
    Path file = Files.writeString(dir.resolve("wif.json"), json);

    assertThat(namedSupplier(file).get()).isInstanceOf(ExternalAccountCredentials.class);
  }

  @Test
  public void testNamedGcpFileOfUnsupportedTypeIsRefused(@TempDir Path dir) throws Exception {
    Path file =
        Files.writeString(
            dir.resolve("user.json"),
            "{\"type\":\"authorized_user\",\"client_id\":\"c\",\"client_secret\":\"file-secret\","
                + "\"refresh_token\":\"r\"}");

    assertThatThrownBy(() -> namedSupplier(file).get())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("team-a")
        .hasMessageContaining("service_account")
        .hasMessageNotContaining("file-secret");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "not json file-secret",
        "{\"type\":\"service_account\",\"private_key\":\"file-secret\"}",
        "{\"type\":\"service_account\",\"private_key\":\"file-secret\",\"client_id\":\"c\","
            + "\"client_email\":\"e\",\"private_key_id\":\"i\"}",
        "{\"type\":\"external_account\",\"audience\":\"file-secret\"}"
      })
  public void testBadNamedGcpFileFailsWithoutRepeatingItsContent(String content, @TempDir Path dir)
      throws Exception {
    Path file = Files.writeString(dir.resolve("bad.json"), content);

    assertThatThrownBy(() -> namedSupplier(file).get())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("team-a")
        .hasNoCause()
        .hasMessageNotContaining("file-secret");
  }

  @Test
  public void testFailedNamedGcpLoadIsRetried(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("late.json");
    Supplier<GoogleCredentials> supplier = namedSupplier(file);

    assertThatThrownBy(supplier::get)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("NoSuchFileException")
        .hasMessageNotContaining(file.toString());

    Files.writeString(file, serviceAccountJson(generatedPrivateKeyPem()));
    assertThat(supplier.get()).isInstanceOf(ServiceAccountCredentials.class);
  }

  @Test
  public void testStsCredentialsWithNamedStorage() {
    StorageConfiguration config = configWithNamedStorage();
    AwsCredentialsProvider credentialsProvider = config.stsCredentials("myStorage");
    assertThat(credentialsProvider).isInstanceOf(StaticCredentialsProvider.class);
    assertThat(credentialsProvider.resolveCredentials().accessKeyId())
        .isEqualTo(STORAGE_ACCESS_KEY);
    assertThat(credentialsProvider.resolveCredentials().secretAccessKey())
        .isEqualTo(STORAGE_SECRET_KEY);
  }

  @Test
  public void testStsCredentialsWithUnknownStorageThrows() {
    StorageConfiguration config = configWithNamedStorage();
    assertThatThrownBy(() -> config.stsCredentials("unknownStorage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unknownStorage")
        .hasMessageContaining("not configured");
  }

  @Test
  public void testStsCredentialsWithNullStorageNameFallsBackToDefault() {
    StorageConfiguration config = configWithNamedStorage();
    AwsCredentialsProvider credentialsProvider = config.stsCredentials(null);
    assertThat(credentialsProvider).isInstanceOf(StaticCredentialsProvider.class);
    assertThat(credentialsProvider.resolveCredentials().accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
    assertThat(credentialsProvider.resolveCredentials().secretAccessKey())
        .isEqualTo(TEST_SECRET_KEY);
  }
}
