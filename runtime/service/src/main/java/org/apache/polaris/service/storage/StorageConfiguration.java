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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.polaris.docs.ConfigDocs;
import org.apache.polaris.service.storage.aws.S3AccessConfig;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

@ConfigMapping(prefix = "polaris.storage")
public interface StorageConfiguration extends S3AccessConfig {

  Duration DEFAULT_TOKEN_LIFESPAN = Duration.ofHours(1);

  /**
   * Names that cannot select GCP credentials, because they are the property names of the default
   * GCP credentials: {@code polaris.storage.gcp.token} and {@code polaris.storage.gcp.lifespan}.
   */
  Set<String> RESERVED_GCP_STORAGE_NAMES = Set.of("token", "lifespan");

  @WithName("aws")
  AwsStorageConfig aws();

  /**
   * @deprecated Use {@link #aws()}.{@link AwsStorageConfig#accessKey() accessKey()} instead.
   */
  @Deprecated
  default Optional<String> awsAccessKey() {
    return aws().accessKey();
  }

  /**
   * @deprecated Use {@link #aws()}.{@link AwsStorageConfig#secretKey() secretKey()} instead.
   */
  @Deprecated
  default Optional<String> awsSecretKey() {
    return aws().secretKey();
  }

  interface AwsStorageConfig {
    /**
     * The AWS access key to use for authentication. If not present, the default credentials
     * provider chain will be used.
     */
    @WithName("access-key")
    Optional<String> accessKey();

    /**
     * The AWS secret key to use for authentication. If not present, the default credentials
     * provider chain will be used.
     */
    @WithName("secret-key")
    Optional<String> secretKey();

    @WithParentName
    @ConfigDocs.ConfigPropertyName("storage")
    Map<String, StorageConfig> storages();
  }

  interface StorageConfig {
    /** The AWS access key to use for authentication when using named storages. */
    @WithName("access-key")
    String accessKey();

    /** The AWS secret key to use for authentication when using named storages. */
    @WithName("secret-key")
    String secretKey();
  }

  /**
   * The GCP access token to use for authentication. If not present, the default credentials
   * provider chain will be used.
   */
  @WithName("gcp.token")
  Optional<String> gcpAccessToken();

  /**
   * How far in the future the configured GCP access token's expiry is stamped. The expiry is
   * computed once, when the credential is created, and is returned unchanged on every refresh. If
   * not present, the {@linkplain #DEFAULT_TOKEN_LIFESPAN default token lifespan} is used. Ignored
   * when {@link #gcpAccessTokenExpiry()} is set. This does not extend the token's real validity on
   * the provider side; it only controls the expiry Polaris reports for the configured token.
   */
  @WithName("gcp.lifespan")
  Optional<Duration> gcpAccessTokenLifespan();

  /**
   * A fixed expiry for the configured GCP access token, as an ISO-8601 instant (for example {@code
   * 2027-01-01T00:00:00Z}). When present, this exact instant is reported as the token's expiry and
   * is returned unchanged on every refresh, and {@link #gcpAccessTokenLifespan()} is ignored. When
   * absent, the expiry is computed from the lifespan. Like the lifespan, this does not extend the
   * token's real validity on the provider side; it only controls the expiry Polaris reports for the
   * configured token.
   */
  @WithName("gcp.expiry")
  Optional<Instant> gcpAccessTokenExpiry();

  @WithName("gcp")
  GcpStorageConfig gcp();

  interface GcpStorageConfig {
    @WithParentName
    @ConfigDocs.ConfigPropertyName("storage")
    Map<String, GcpNamedStorageConfig> storages();
  }

  interface GcpNamedStorageConfig {
    /**
     * Path to the GCP credentials file to use for authentication when using named storages. This is
     * the same JSON format that the {@code GOOGLE_APPLICATION_CREDENTIALS} environment variable
     * points to for the default credentials. Service account key files and Workload Identity
     * Federation configuration files are accepted. The names {@code token} and {@code lifespan} are
     * reserved and cannot be used as storage names.
     */
    @WithName("credentials-file")
    Path credentialsFile();
  }

  default Supplier<StsClient> stsClientSupplier() {
    return stsClientSupplier(true);
  }

  default Supplier<StsClient> stsClientSupplier(boolean withCredentials) {
    return Suppliers.memoize(
        () -> {
          StsClientBuilder stsClientBuilder = StsClient.builder();
          if (withCredentials) {
            stsClientBuilder.credentialsProvider(stsCredentials());
          }
          return stsClientBuilder.build();
        });
  }

  default AwsCredentialsProvider stsCredentials() {
    if (aws().accessKey().isPresent() && aws().secretKey().isPresent()) {
      LoggerFactory.getLogger(StorageConfiguration.class)
          .warn("Using hard-coded AWS credentials - this is not recommended for production");
      return StaticCredentialsProvider.create(
          AwsBasicCredentials.create(aws().accessKey().get(), aws().secretKey().get()));
    } else {
      return DefaultCredentialsProvider.builder().build();
    }
  }

  default AwsCredentialsProvider stsCredentials(String storageName) {
    if (storageName != null) {
      if (!aws().storages().containsKey(storageName)) {
        throw new IllegalArgumentException(
            "Storage name '" + storageName + "' is not configured on the server");
      }
      StorageConfig storageConfig = aws().storages().get(storageName);
      return StaticCredentialsProvider.create(
          AwsBasicCredentials.create(storageConfig.accessKey(), storageConfig.secretKey()));
    }
    return stsCredentials();
  }

  default Supplier<GoogleCredentials> gcpCredentialsSupplier(Clock clock) {
    return Suppliers.memoize(
        () -> {
          if (gcpAccessToken().isEmpty()) {
            try {
              return GoogleCredentials.getApplicationDefault();
            } catch (IOException e) {
              throw new RuntimeException("Failed to get GCP credentials", e);
            }
          } else {
            // A statically configured token has no token endpoint to call, and
            // GoogleCredentials.create(..) returns a credential that cannot refresh. Vending calls
            // refresh() on the source credential before downscoping, so with that credential the
            // first vend fails with an IllegalStateException regardless of expiry (#2253). Wrap the
            // token in a credential that supports refresh by returning it with a fixed expiry: the
            // configured gcp.expiry if set, otherwise computed once here from the configured
            // lifespan.
            Instant expiry =
                gcpAccessTokenExpiry()
                    .orElseGet(
                        () ->
                            clock
                                .instant()
                                .plus(gcpAccessTokenLifespan().orElse(DEFAULT_TOKEN_LIFESPAN)));
            return new FixedGcpAccessTokenCredentials(gcpAccessToken().orElseThrow(), expiry);
          }
        });
  }

  /**
   * Returns one supplier per configured GCP storage name. Each supplier loads its credentials file
   * on first use and shares the result, like {@link #gcpCredentialsSupplier(Clock)} does for the
   * default credentials. A failed load is retried on the next use.
   */
  default Map<String, Supplier<GoogleCredentials>> gcpNamedCredentialsSuppliers() {
    ImmutableMap.Builder<String, Supplier<GoogleCredentials>> suppliers = ImmutableMap.builder();
    gcp()
        .storages()
        .forEach(
            (storageName, storageConfig) -> {
              if (!RESERVED_GCP_STORAGE_NAMES.contains(storageName)) {
                suppliers.put(
                    storageName,
                    Suppliers.memoize(
                        () -> loadGcpCredentials(storageName, storageConfig.credentialsFile())));
              }
            });
    return suppliers.build();
  }

  private static GoogleCredentials loadGcpCredentials(String storageName, Path credentialsFile) {
    try {
      byte[] content = Files.readAllBytes(credentialsFile);
      String type = new ObjectMapper().readTree(content).path("type").asText();
      InputStream stream = new ByteArrayInputStream(content);
      if ("service_account".equals(type)) {
        return ServiceAccountCredentials.fromStream(stream);
      }
      if ("external_account".equals(type)) {
        return ExternalAccountCredentials.fromStream(stream);
      }
    } catch (IOException | RuntimeException e) {
      // Only the failure type is reported: parser and library messages can repeat file content.
      throw new IllegalStateException(
          "Failed to load GCP credentials for storage name '"
              + storageName
              + "': "
              + e.getClass().getSimpleName());
    }
    throw new IllegalStateException(
        "Unsupported GCP credentials file for storage name '"
            + storageName
            + "': only service_account and external_account files are accepted");
  }
}
