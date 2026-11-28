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

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.Suppliers;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.apache.polaris.service.storage.StorageConfiguration.GcpNamedStorageConfig;
import org.apache.polaris.service.storage.StorageConfiguration.GcpStorageConfig;

/**
 * Selects the GCP credentials of a storage configuration from the {@code polaris.storage.gcp}
 * properties. In order: the credentials of the storage name, the unnamed credentials, the GCP
 * access token, the application default credentials. Credentials read from a file or a property are
 * loaded the first time they are used and then shared; a failed load is retried on the next use.
 * The vended credentials are cached by the caller.
 *
 * <p>The messages can reach API clients, so they never include a configured value.
 */
@ApplicationScoped
class GcpCredentialsFactory {

  private static final String PREFIX = "polaris.storage.gcp.";

  // A credentials file is one line of JSON. Pasted into application.properties, its \n escapes
  // become real line breaks, which strict JSON rejects inside a string.
  private static final ObjectMapper MAPPER =
      JsonMapper.builder().enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS).build();

  private final StorageConfiguration storageConfiguration;
  private final RealmConfig realmConfig;
  private final Supplier<GoogleCredentials> defaultCredentials;
  private final Supplier<GoogleCredentials> unnamedCredentials;
  private final Map<String, GoogleCredentials> namedCredentials = new ConcurrentHashMap<>();

  /**
   * For the client proxy ArC generates for this normal-scoped bean: the proxy subclass needs a
   * no-args constructor. A working instance always comes from the constructor below.
   */
  protected GcpCredentialsFactory() {
    this.storageConfiguration = null;
    this.realmConfig = null;
    this.defaultCredentials = null;
    this.unnamedCredentials = null;
  }

  @Inject
  GcpCredentialsFactory(
      StorageConfiguration storageConfiguration, RealmConfig realmConfig, Clock clock) {
    this.storageConfiguration = storageConfiguration;
    this.realmConfig = realmConfig;
    // The GCP access token or the application default credentials, built once.
    this.defaultCredentials = storageConfiguration.gcpCredentialsSupplier(clock);
    GcpStorageConfig gcp = storageConfiguration.gcp();
    this.unnamedCredentials =
        Suppliers.memoize(() -> load(PREFIX, gcp.credentialsFile(), gcp.credentials()));
  }

  /**
   * Returns the credentials of the storage configuration. With {@code
   * RESOLVE_CREDENTIALS_BY_STORAGE_NAME}, a storage name selects its named credentials and never
   * falls back. Otherwise the unnamed credentials are used if set, else the GCP access token or the
   * application default credentials.
   */
  GoogleCredentials credentials(GcpStorageConfigurationInfo storageConfig) {
    String storageName = storageConfig.getStorageName();
    if (storageName != null
        && realmConfig.getConfig(FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME)) {
      // Attribution may replace the source credentials with a per-principal federated identity: it
      // does when the storage has a gcpServiceAccount and the request has a principal, and falls
      // back to the source credentials otherwise. A storage name would then apply only to some
      // requests, so refuse the combination rather than ignore either setting.
      if (realmConfig.getConfig(FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_ENABLED)) {
        throw new IllegalStateException(
            "Storage name '"
                + storageName
                + "' cannot be used while GCS_PRINCIPAL_ATTRIBUTION_ENABLED is true");
      }
      GcpNamedStorageConfig named = storageConfiguration.gcp().storages().get(storageName);
      if (named == null) {
        throw new IllegalArgumentException(
            "Storage name '" + storageName + "' is not configured on the server");
      }
      // A failed load throws out of computeIfAbsent and stores nothing.
      return namedCredentials.computeIfAbsent(
          storageName,
          name -> load(PREFIX + name + ".", named.credentialsFile(), named.credentials()));
    }
    GcpStorageConfig gcp = storageConfiguration.gcp();
    if (gcp.credentialsFile().isEmpty() && gcp.credentials().isEmpty()) {
      return defaultCredentials.get();
    }
    if (storageConfiguration.gcpAccessToken().isPresent()) {
      throw new IllegalStateException(
          "GCP credentials are configured twice on the server: "
              + PREFIX
              + "token cannot be combined with "
              + PREFIX
              + "credentials-file or "
              + PREFIX
              + "credentials");
    }
    return unnamedCredentials.get();
  }

  private static GoogleCredentials load(
      String prefix, Optional<Path> credentialsFile, Optional<String> credentials) {
    if (credentialsFile.isPresent() == credentials.isPresent()) {
      throw new IllegalStateException(
          "Set exactly one of " + prefix + "credentials-file and " + prefix + "credentials");
    }
    if (credentials.isPresent()) {
      return parse(prefix + "credentials", credentials.get().getBytes(UTF_8));
    }
    String property = prefix + "credentials-file";
    byte[] content;
    try {
      content = Files.readAllBytes(credentialsFile.get());
    } catch (IOException e) {
      // The cause names the file, for the server log. The message names only the property.
      throw new IllegalStateException(
          "Failed to read GCP credentials from " + property + ": " + e.getClass().getSimpleName(),
          e);
    }
    return parse(property, content);
  }

  private static GoogleCredentials parse(String property, byte[] content) {
    try {
      String type = MAPPER.readTree(content).path("type").asText();
      InputStream stream = new ByteArrayInputStream(content);
      if ("service_account".equals(type)) {
        return ServiceAccountCredentials.fromStream(stream);
      }
      if ("external_account".equals(type)) {
        return ExternalAccountCredentials.fromStream(stream);
      }
    } catch (IOException | RuntimeException e) {
      // No cause, even for the server log: parser and library messages can repeat the content.
      throw new IllegalStateException(
          "Failed to load GCP credentials from " + property + ": " + e.getClass().getSimpleName());
    }
    throw new IllegalStateException(
        "Unsupported GCP credentials in "
            + property
            + ": only service_account and external_account credentials are accepted");
  }
}
