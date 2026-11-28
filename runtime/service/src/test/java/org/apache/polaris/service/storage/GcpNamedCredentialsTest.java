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
import static org.apache.polaris.core.config.FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GcpNamedCredentialsTest {

  private static final String PRIVATE_KEY = generatedPrivateKeyPem();

  private static String generatedPrivateKeyPem() {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      String encoded =
          Base64.getMimeEncoder(64, "\n".getBytes(UTF_8))
              .encodeToString(generator.generateKeyPair().getPrivate().getEncoded());
      return "-----BEGIN PRIVATE KEY-----\n" + encoded + "\n-----END PRIVATE KEY-----\n";
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /** A service account key file, as one line of JSON, like the ones GCP issues. */
  static String serviceAccountJson(String clientEmail) {
    return json(
        Map.of(
            "type", "service_account",
            "project_id", "project",
            "private_key_id", "key-id",
            "private_key", PRIVATE_KEY,
            "client_email", clientEmail,
            "client_id", "client-id"));
  }

  private static String json(Map<String, Object> content) {
    try {
      return new ObjectMapper().writeValueAsString(content);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static String workloadIdentityFederationJson() {
    return json(
        Map.of(
            "type", "external_account",
            "audience",
                "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/q",
            "subject_token_type", "urn:ietf:params:oauth:token-type:jwt",
            "token_url", "https://sts.googleapis.com/v1/token",
            "credential_source", Map.of("file", "/var/run/token")));
  }

  private static StorageConfiguration configFrom(Map<String, String> properties) {
    return configFrom(properties, Map.of());
  }

  private static StorageConfiguration configFrom(
      Map<String, String> properties, Map<String, String> environment) {
    return new SmallRyeConfigBuilder()
        .withMapping(StorageConfiguration.class)
        .withSources(new PropertiesConfigSource(properties, "test", 100))
        .withSources(new EnvConfigSource(environment, 300))
        .build()
        .getConfigMapping(StorageConfiguration.class);
  }

  private static GcpCredentialsFactory factory(StorageConfiguration config, boolean byName) {
    Map<String, Object> flags = Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), byName);
    RealmConfig realmConfig =
        new RealmConfigImpl((rc, flag) -> flags.get(flag), () -> "test-realm");
    return new GcpCredentialsFactory(config, realmConfig, Clock.systemUTC());
  }

  private static GcpStorageConfigurationInfo storage(String storageName) {
    return GcpStorageConfigurationInfo.builder()
        .addAllowedLocation("gs://bucket/path/")
        .storageName(storageName)
        .build();
  }

  private static String clientEmail(GoogleCredentials credentials) {
    assertThat(credentials).isInstanceOf(ServiceAccountCredentials.class);
    return ((ServiceAccountCredentials) credentials).getClientEmail();
  }

  @Test
  void eachNameUsesItsOwnCredentials(@TempDir Path dir) throws Exception {
    Path teamAFile = Files.writeString(dir.resolve("a.json"), serviceAccountJson("a@p.iam"));
    StorageConfiguration config =
        configFrom(
            Map.of(
                "polaris.storage.gcp.team-a.credentials-file", teamAFile.toString(),
                "polaris.storage.gcp.team-b.credentials", serviceAccountJson("b@p.iam")));
    GcpCredentialsFactory factory = factory(config, true);

    assertThat(config.gcp().storages()).containsOnlyKeys("team-a", "team-b");
    assertThat(clientEmail(factory.credentials(storage("team-a")))).isEqualTo("a@p.iam");
    assertThat(clientEmail(factory.credentials(storage("team-b")))).isEqualTo("b@p.iam");
  }

  @Test
  void aWorkloadIdentityFederationFileIsAccepted(@TempDir Path dir) throws Exception {
    Path file = Files.writeString(dir.resolve("wif.json"), workloadIdentityFederationJson());
    StorageConfiguration config =
        configFrom(Map.of("polaris.storage.gcp.team-a.credentials-file", file.toString()));

    assertThat(factory(config, true).credentials(storage("team-a")))
        .isInstanceOf(ExternalAccountCredentials.class);
  }

  @Test
  void aNameIsLoadedOnceAndShared(@TempDir Path dir) throws Exception {
    Path file = Files.writeString(dir.resolve("a.json"), serviceAccountJson("a@p.iam"));
    GcpCredentialsFactory factory =
        factory(
            configFrom(Map.of("polaris.storage.gcp.team-a.credentials-file", file.toString())),
            true);

    GoogleCredentials first = factory.credentials(storage("team-a"));
    Files.delete(file);

    assertThat(factory.credentials(storage("team-a"))).isSameAs(first);
  }

  @Test
  void aFailedLoadIsRetriedAndKeepsTheFileOutOfTheMessage(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("a.json");
    GcpCredentialsFactory factory =
        factory(
            configFrom(Map.of("polaris.storage.gcp.team-a.credentials-file", file.toString())),
            true);

    // The cause names the file, for the server log; the message names only the property.
    assertThatThrownBy(() -> factory.credentials(storage("team-a")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("polaris.storage.gcp.team-a.credentials-file")
        .hasMessageNotContaining(dir.toString())
        .hasCauseInstanceOf(NoSuchFileException.class);
    Files.writeString(file, serviceAccountJson("a@p.iam"));

    assertThat(clientEmail(factory.credentials(storage("team-a")))).isEqualTo("a@p.iam");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "not json file-secret",
        "{\"type\":\"service_account\",\"private_key\":\"file-secret\"}",
        "{\"type\":\"external_account\",\"audience\":\"file-secret\"}",
        "{\"type\":\"authorized_user\",\"client_secret\":\"file-secret\"}"
      })
  void badCredentialsFailWithoutRepeatingTheirContent(String content, @TempDir Path dir)
      throws Exception {
    Path file = Files.writeString(dir.resolve("bad.json"), content);
    StorageConfiguration config =
        configFrom(
            Map.of(
                "polaris.storage.gcp.team-a.credentials-file", file.toString(),
                "polaris.storage.gcp.team-b.credentials", content,
                "polaris.storage.gcp.team-c.credentials", serviceAccountJson("c@p.iam")));
    GcpCredentialsFactory factory = factory(config, true);

    assertThatThrownBy(() -> factory.credentials(storage("team-a")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("polaris.storage.gcp.team-a.credentials-file")
        .hasMessageNotContaining("file-secret")
        .hasNoCause();
    assertThatThrownBy(() -> factory.credentials(storage("team-b")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("polaris.storage.gcp.team-b.credentials")
        .hasMessageNotContaining("file-secret")
        .hasNoCause();
    // A bad entry fails only its own name.
    assertThat(clientEmail(factory.credentials(storage("team-c")))).isEqualTo("c@p.iam");
  }

  @Test
  void aNameWithBothAFileAndContentFailsWithoutRevealingThem() {
    StorageConfiguration config =
        configFrom(
            Map.of(
                "polaris.storage.gcp.team-a.credentials-file",
                "/secrets/a.json",
                "polaris.storage.gcp.team-a.credentials",
                serviceAccountJson("a@p.iam")));

    assertThatThrownBy(() -> factory(config, true).credentials(storage("team-a")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("polaris.storage.gcp.team-a.credentials-file")
        .hasMessageContaining("polaris.storage.gcp.team-a.credentials")
        .hasMessageNotContaining("/secrets/a.json")
        .hasMessageNotContaining("a@p.iam");
  }

  @ParameterizedTest
  @ValueSource(strings = {"unknown", "Team-A"})
  void anUnknownNameFailsWithoutFallback(String storageName) {
    StorageConfiguration config =
        configFrom(
            Map.of(
                "polaris.storage.gcp.team-a.credentials", serviceAccountJson("a@p.iam"),
                "polaris.storage.gcp.credentials", serviceAccountJson("unnamed@p.iam")));

    assertThatThrownBy(() -> factory(config, true).credentials(storage(storageName)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(storageName)
        .hasMessageContaining("not configured");
  }

  @Test
  void namesOfTheGcpAccessTokenPropertiesAreOrdinaryNames() {
    Map<String, String> properties = new HashMap<>();
    properties.put("polaris.storage.gcp.token", "gcp-token");
    properties.put("polaris.storage.gcp.lifespan", "PT20M");
    properties.put("polaris.storage.gcp.expiry", "2027-01-01T00:00:00Z");
    properties.put("polaris.storage.gcp.token.credentials", serviceAccountJson("token@p.iam"));
    properties.put("polaris.storage.gcp.lifespan.credentials-file", "/secrets/lifespan.json");
    properties.put("polaris.storage.gcp.expiry.credentials-file", "/secrets/expiry.json");
    StorageConfiguration config = configFrom(properties);

    assertThat(config.gcpAccessToken()).contains("gcp-token");
    assertThat(config.gcpAccessTokenLifespan()).contains(Duration.ofMinutes(20));
    assertThat(config.gcpAccessTokenExpiry()).contains(Instant.parse("2027-01-01T00:00:00Z"));
    assertThat(config.gcp().storages()).containsOnlyKeys("token", "lifespan", "expiry");
    assertThat(clientEmail(factory(config, true).credentials(storage("token"))))
        .isEqualTo("token@p.iam");
  }

  @Test
  void unnamedCredentialsAreNotTakenForAStorageName() {
    StorageConfiguration config =
        configFrom(
            Map.of(
                "polaris.storage.gcp.credentials-file",
                "/secrets/unnamed.json",
                "polaris.storage.gcp.team-a.credentials",
                serviceAccountJson("a@p.iam")));

    assertThat(config.gcp().storages()).containsOnlyKeys("team-a");
    assertThat(config.gcp().credentialsFile()).contains(Path.of("/secrets/unnamed.json"));
    assertThat(config.gcp().credentials()).isEmpty();
  }

  @Test
  void environmentVariablesCanSetTheContent() {
    StorageConfiguration config =
        configFrom(
            Map.of(),
            Map.of(
                "polaris.storage.gcp.team-a.credentials", serviceAccountJson("a@p.iam"),
                "POLARIS_STORAGE_GCP_TEAMB_CREDENTIALS", serviceAccountJson("b@p.iam"),
                "POLARIS_STORAGE_GCP_CREDENTIALS", serviceAccountJson("unnamed@p.iam")));
    GcpCredentialsFactory factory = factory(config, true);

    assertThat(clientEmail(factory.credentials(storage("team-a")))).isEqualTo("a@p.iam");
    assertThat(clientEmail(factory.credentials(storage("teamb")))).isEqualTo("b@p.iam");
    assertThat(clientEmail(factory.credentials(storage(null)))).isEqualTo("unnamed@p.iam");
  }

  @Test
  void contentPastedIntoAPropertiesFileIsAccepted() throws Exception {
    // Properties.load, which reads application.properties, turns the \n escapes of the private
    // key into line breaks.
    Properties loaded = new Properties();
    loaded.load(
        new StringReader("polaris.storage.gcp.credentials=" + serviceAccountJson("u@p.iam")));
    String content = loaded.getProperty("polaris.storage.gcp.credentials");
    assertThat(content).contains("\n");

    GcpCredentialsFactory factory =
        factory(configFrom(Map.of("polaris.storage.gcp.credentials", content)), true);

    assertThat(clientEmail(factory.credentials(storage(null)))).isEqualTo("u@p.iam");
  }

  @Test
  void unnamedCredentialsReplaceTheDefaultAndAreLoadedOnce() {
    GcpCredentialsFactory factory =
        factory(
            configFrom(Map.of("polaris.storage.gcp.credentials", serviceAccountJson("u@p.iam"))),
            false);

    GoogleCredentials credentials = factory.credentials(storage(null));

    assertThat(clientEmail(credentials)).isEqualTo("u@p.iam");
    // With the flag off, a storage name is ignored.
    assertThat(factory.credentials(storage("team-a"))).isSameAs(credentials);
  }

  @Test
  void unnamedCredentialsCannotBeCombinedWithTheGcpAccessToken() {
    GcpCredentialsFactory factory =
        factory(
            configFrom(
                Map.of(
                    "polaris.storage.gcp.token", "gcp-token",
                    "polaris.storage.gcp.credentials-file", "/secrets/unnamed.json")),
            true);

    assertThatThrownBy(() -> factory.credentials(storage(null)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("polaris.storage.gcp.token")
        .hasMessageContaining("polaris.storage.gcp.credentials-file")
        .hasMessageNotContaining("gcp-token")
        .hasMessageNotContaining("/secrets/unnamed.json");
  }

  @Test
  void withoutCredentialsTheGcpAccessTokenIsUsedAndBuiltOnce() {
    GcpCredentialsFactory factory =
        factory(configFrom(Map.of("polaris.storage.gcp.token", "gcp-token")), true);

    GoogleCredentials credentials = factory.credentials(storage(null));

    assertThat(credentials).isInstanceOf(FixedGcpAccessTokenCredentials.class);
    assertThat(factory.credentials(storage(null))).isSameAs(credentials);
  }
}
