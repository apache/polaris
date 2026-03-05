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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Suppliers;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
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
   * The lifespan of the GCP access token. If not present, the {@linkplain #DEFAULT_TOKEN_LIFESPAN
   * default token lifespan} will be used.
   */
  @WithName("gcp.lifespan")
  Optional<Duration> gcpAccessTokenLifespan();

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
            AccessToken accessToken =
                new AccessToken(
                    gcpAccessToken().get(),
                    new Date(
                        clock
                            .instant()
                            .plus(gcpAccessTokenLifespan().orElse(DEFAULT_TOKEN_LIFESPAN))
                            .toEpochMilli()));
            return GoogleCredentials.create(accessToken);
          }
        });
  }
}
