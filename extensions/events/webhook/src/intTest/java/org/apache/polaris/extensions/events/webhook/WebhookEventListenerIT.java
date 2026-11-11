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
package org.apache.polaris.extensions.events.webhook;

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.events.CloudEventsEventJsonMapper;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Exercises production {@link WebhookEventListener#createTransport()} (Quarkus REST client) by
 * spawning a full Polaris server with the webhook listener enabled. A Gradle startup action binds
 * the HTTP receiver before the server starts and records POSTs for this test to read.
 *
 * <p>This module is not a Quarkus application, so {@code @QuarkusTest} cannot build the production
 * client. Unit tests keep a JDK-HttpClient seam for listener logic (HMAC, bounds, retries).
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WebhookEventListenerIT {

  private static final String SECRET = "webhook-it-secret";
  private static final String AUTHORIZATION = "Bearer webhook-it-token";
  private static final JsonMapper OBJECT_MAPPER = JsonMapper.shared();

  private Path captureDir;
  private PolarisClient client;
  private ManagementApi managementApi;
  private URI baseLocation;

  private record RecordedRequest(
      String body, String signature, String eventType, String authorization) {}

  @BeforeAll
  public void setup(
      PolarisApiEndpoints endpoints, ClientCredentials credentials, @TempDir Path tempDir) {
    String captureDirProperty = System.getProperty("webhook.it.capture-dir");
    assertThat(captureDirProperty).as("webhook.it.capture-dir").isNotBlank();
    captureDir = Path.of(captureDirProperty);
    client = polarisClient(endpoints);
    managementApi = client.managementApi(client.obtainToken(credentials));
    baseLocation =
        IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve(endpoints.realmId() + "/");
  }

  @AfterAll
  public void close() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void createCatalogPostsCloudEventsEnvelopeViaQuarkusRestClient() {
    String catalogName = client.newEntityName("webhookRestClient");
    Catalog catalog =
        PolarisCatalog.builder()
            .setName(catalogName)
            .setType(Catalog.TypeEnum.INTERNAL)
            .setProperties(CatalogProperties.builder(baseLocation.toString()).build())
            .setStorageConfigInfo(
                FileStorageConfigInfo.builder()
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
                    .setAllowedLocations(List.of(baseLocation.toString()))
                    .build())
            .build();
    managementApi.createCatalog(catalog);

    Awaitility.await("webhook delivered through Quarkus REST client")
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () ->
                assertThat(findCreateCatalog())
                    .as("captured POSTs under %s", captureDir)
                    .isPresent());

    RecordedRequest request = findCreateCatalog().orElseThrow();
    JsonNode json = OBJECT_MAPPER.readTree(request.body());
    assertThat(json.get("specversion").asString())
        .isEqualTo(CloudEventsEventJsonMapper.SPEC_VERSION);
    assertThat(json.get("source").asString()).isEqualTo(CloudEventsEventJsonMapper.SOURCE);
    assertThat(json.get("type").asString())
        .isEqualTo(
            CloudEventsEventJsonMapper.cloudEventsType(PolarisEventType.AFTER_CREATE_CATALOG));
    assertThat(json.get("datacontenttype").asString()).isEqualTo("application/json");
    assertThat(json.get("id").asString()).isNotBlank();
    assertThat(json.get("time").asString()).isNotBlank();
    assertThat(json.get("data").get("event_type").asString())
        .isEqualTo(PolarisEventType.AFTER_CREATE_CATALOG.name());
    assertThat(json.get("data").get("catalog_name").asString()).isEqualTo(catalogName);

    assertThat(request.authorization()).isEqualTo(AUTHORIZATION);
    assertThat(request.signature())
        .isEqualTo("sha256=" + WebhookEventListener.sign(request.body(), SECRET));
    assertThat(request.signature()).doesNotContain("forged");
  }

  private Optional<RecordedRequest> findCreateCatalog() {
    if (!Files.isDirectory(captureDir)) {
      return Optional.empty();
    }
    try (Stream<Path> files = Files.list(captureDir)) {
      return files
          .map(path -> path.getFileName().toString())
          .filter(name -> name.endsWith(".eventType"))
          .map(name -> name.substring(0, name.length() - ".eventType".length()))
          .filter(id -> Files.exists(captureDir.resolve(id + ".body")))
          .map(this::readRequest)
          .filter(
              request -> PolarisEventType.AFTER_CREATE_CATALOG.name().equals(request.eventType()))
          .findFirst();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private RecordedRequest readRequest(String id) {
    try {
      return new RecordedRequest(
          Files.readString(captureDir.resolve(id + ".body"), StandardCharsets.UTF_8),
          Files.readString(captureDir.resolve(id + ".signature"), StandardCharsets.UTF_8),
          Files.readString(captureDir.resolve(id + ".eventType"), StandardCharsets.UTF_8),
          Files.readString(captureDir.resolve(id + ".authorization"), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
