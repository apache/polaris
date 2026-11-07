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
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.sun.net.httpserver.HttpServer;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.config.RealmConfigurationSource;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.gcp.GcpCredentialOps;
import org.apache.polaris.core.storage.gcp.GcpCredentialsStorageIntegration;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
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
    return configWithAwsCredentialsAndGcpToken(Optional.empty());
  }

  private StorageConfiguration configWithAwsCredentialsAndGcpToken(Optional<Instant> gcpExpiry) {
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
      public Optional<Instant> gcpAccessTokenExpiry() {
        return gcpExpiry;
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
      public Optional<Instant> gcpAccessTokenExpiry() {
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
      public Optional<Instant> gcpAccessTokenExpiry() {
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
  public void testGcpStaticTokenCredentialsRefreshKeepsFixedExpiry() throws IOException {
    Instant start = Instant.parse("2026-01-01T00:00:00Z");
    Clock clock = mock(Clock.class);
    when(clock.instant()).thenReturn(start, start.plus(Duration.ofHours(1)));

    Supplier<GoogleCredentials> supplier =
        configWithAwsCredentialsAndGcpToken().gcpCredentialsSupplier(clock);
    GoogleCredentials credentials = supplier.get();

    Instant firstExpiry = credentials.getAccessToken().getExpirationTime().toInstant();
    assertThat(firstExpiry).isEqualTo(start.plus(TEST_TOKEN_LIFESPAN));

    // A plain GoogleCredentials.create(token) would throw here because it cannot refresh. The
    // configured-token credential supports refresh but keeps the expiry fixed: the token value
    // never changes, so neither does its reported expiry (#2253).
    credentials.refresh();

    AccessToken refreshed = credentials.getAccessToken();
    assertThat(refreshed.getTokenValue()).isEqualTo(TEST_GCP_TOKEN);
    assertThat(refreshed.getExpirationTime().toInstant()).isEqualTo(firstExpiry);
  }

  @Test
  public void testGcpStaticTokenExpiryOverridesLifespan() throws IOException {
    Instant start = Instant.parse("2026-01-01T00:00:00Z");
    Instant configuredExpiry = Instant.parse("2027-06-15T12:00:00Z");
    Clock clock = mock(Clock.class);
    when(clock.instant()).thenReturn(start);

    Supplier<GoogleCredentials> supplier =
        configWithAwsCredentialsAndGcpToken(Optional.of(configuredExpiry))
            .gcpCredentialsSupplier(clock);
    GoogleCredentials credentials = supplier.get();

    // gcp.expiry is used verbatim as the token expiry, ignoring gcp.lifespan, and stays fixed
    // across refresh.
    assertThat(credentials.getAccessToken().getExpirationTime().toInstant())
        .isEqualTo(configuredExpiry);
    credentials.refresh();
    assertThat(credentials.getAccessToken().getExpirationTime().toInstant())
        .isEqualTo(configuredExpiry);
  }

  @Test
  public void testGcpStaticTokenVendsThroughDownscoping() {
    GcpCredentialOps ops =
        new GcpCredentialOps() {
          @Override
          public AccessToken refreshAccessToken(DownscopedCredentials credentials) {
            return new AccessToken("vended", new Date(System.currentTimeMillis() + 3_600_000));
          }
        };

    Supplier<GoogleCredentials> supplier =
        configWithAwsCredentialsAndGcpToken().gcpCredentialsSupplier(Clock.systemUTC());
    RealmConfig realmConfig =
        new RealmConfigImpl(RealmConfigurationSource.EMPTY_CONFIG, () -> "test-realm");

    // On main the source credential is GoogleCredentials.create(token), whose refresh() throws
    // during vending; with the configured-token credential the downscoped token is vended.
    StorageAccessConfig vended =
        new GcpCredentialsStorageIntegration(
                supplier.get(),
                NetHttpTransport::new,
                GcpStorageConfigurationInfo.builder().addAllowedLocation("gs://b/p/").build(),
                realmConfig,
                ops)
            .getStorageAccessConfig(
                List.of(new LocationGrant(Set.of("gs://b/p/"), Set.of(PolarisStorageActions.READ))),
                Optional.empty(),
                CredentialVendingContext.empty());

    assertThat(vended.get(StorageAccessProperty.GCS_ACCESS_TOKEN)).isEqualTo("vended");
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
    return serviceAccountJson(
        privateKey, "named@project.iam.gserviceaccount.com", "https://oauth2.googleapis.com/token");
  }

  private static String serviceAccountJson(String privateKey, String email, String tokenUri)
      throws Exception {
    return new ObjectMapper()
        .writeValueAsString(
            Map.of(
                "type", "service_account",
                "project_id", "project",
                "private_key_id", "key-id",
                "private_key", privateKey,
                "client_email", email,
                "client_id", "client-id",
                "token_uri", tokenUri));
  }

  /** Answers OAuth token requests with a token naming the service account that signed the JWT. */
  private static HttpServer startFakeTokenEndpoint() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/token",
        exchange -> {
          String form = new String(exchange.getRequestBody().readAllBytes(), UTF_8);
          String assertion = form.substring(form.indexOf("assertion=") + "assertion=".length());
          String claims =
              new String(
                  Base64.getUrlDecoder()
                      .decode(URLDecoder.decode(assertion, UTF_8).split("\\.")[1]),
                  UTF_8);
          String issuer = new ObjectMapper().readTree(claims).get("iss").asText();
          byte[] response =
              new ObjectMapper()
                  .writeValueAsBytes(
                      Map.of(
                          "access_token",
                          "source-token-for-" + issuer,
                          "token_type",
                          "Bearer",
                          "expires_in",
                          3600));
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    return server;
  }

  /** Records each downscoping request and answers it with a vended token. */
  private static HttpTransport fakeStsTransport(List<String> stsRequests) {
    return new HttpTransport() {
      @Override
      protected LowLevelHttpRequest buildRequest(String method, String url) {
        return new MockLowLevelHttpRequest(url) {
          @Override
          public LowLevelHttpResponse execute() throws IOException {
            stsRequests.add(URLDecoder.decode(getContentAsString(), UTF_8));
            return new MockLowLevelHttpResponse()
                .setContentType("application/json")
                .setContent(
                    "{\"access_token\":\"vended\",\"token_type\":\"Bearer\","
                        + "\"issued_token_type\":\"urn:ietf:params:oauth:token-type:access_token\","
                        + "\"expires_in\":3600}");
          }
        };
      }
    };
  }

  private static String vendFromServiceAccountFile(
      Path dir, String name, String email, String tokenUri, List<String> stsRequests)
      throws Exception {
    Path file =
        Files.writeString(
            dir.resolve(name + ".json"),
            serviceAccountJson(generatedPrivateKeyPem(), email, tokenUri));
    GoogleCredentials credentials =
        configFrom(Map.of("polaris.storage.gcp." + name + ".credentials-file", file.toString()))
            .gcpNamedCredentialsSuppliers()
            .get(name)
            .get();
    StorageAccessConfig vended =
        new GcpCredentialsStorageIntegration(
                credentials,
                () -> fakeStsTransport(stsRequests),
                GcpStorageConfigurationInfo.builder().addAllowedLocation("gs://b/p/").build(),
                new RealmConfigImpl((realm, key) -> null, () -> "test-realm"))
            .getStorageAccessConfig(
                List.of(new LocationGrant(Set.of("gs://b/p/"), Set.of(PolarisStorageActions.READ))),
                Optional.empty(),
                CredentialVendingContext.empty());
    return vended.get(StorageAccessProperty.GCS_ACCESS_TOKEN);
  }

  @Test
  public void testNamedGcpServiceAccountFilesVendWithTheirOwnIdentity(@TempDir Path dir)
      throws Exception {
    HttpServer tokenEndpoint = startFakeTokenEndpoint();
    try {
      String tokenUri = "http://localhost:" + tokenEndpoint.getAddress().getPort() + "/token";
      String emailA = "team-a@project.iam.gserviceaccount.com";
      String emailB = "team-b@project.iam.gserviceaccount.com";
      List<String> stsRequests = new ArrayList<>();

      assertThat(vendFromServiceAccountFile(dir, "team-a", emailA, tokenUri, stsRequests))
          .isEqualTo("vended");
      assertThat(vendFromServiceAccountFile(dir, "team-b", emailB, tokenUri, stsRequests))
          .isEqualTo("vended");

      assertThat(stsRequests).hasSize(2);
      assertThat(stsRequests.get(0))
          .contains("subject_token=source-token-for-" + emailA)
          .doesNotContain(emailB);
      assertThat(stsRequests.get(1))
          .contains("subject_token=source-token-for-" + emailB)
          .doesNotContain(emailA);
    } finally {
      tokenEndpoint.stop(0);
    }
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
