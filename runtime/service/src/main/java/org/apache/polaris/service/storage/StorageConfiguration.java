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
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.service.storage.aws.S3AccessConfig;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

@ConfigMapping(prefix = "polaris.storage")
public interface StorageConfiguration extends S3AccessConfig {

  Duration DEFAULT_TOKEN_LIFESPAN = Duration.ofHours(1);

  /**
   * The AWS access key to use for authentication. If not present, the default credentials provider
   * chain will be used.
   */
  @WithName("aws.access-key")
  Optional<String> awsAccessKey();

  /**
   * The AWS secret key to use for authentication. If not present, the default credentials provider
   * chain will be used.
   */
  @WithName("aws.secret-key")
  Optional<String> awsSecretKey();

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
    return Suppliers.memoize(
        () -> {
          StsClientBuilder builder = StsClient.builder();
          awsSystemCredentials().ifPresent(builder::credentialsProvider);
          return builder.build();
        });
  }

  /**
   * Returns an {@link AwsCredentialsProvider} that provides system-wide AWS credentials. If both
   * access key and secret key are present, a static credentials provider is returned. Otherwise, an
   * empty optional is returned, implying that the default credentials provider chain should be
   * used.
   *
   * <p>The returned provider is not meant to be vended directly to clients, but rather used with
   * STS, unless credential subscoping is disabled.
   */
  default Optional<AwsCredentialsProvider> awsSystemCredentials() {
    if (awsAccessKey().isPresent() && awsSecretKey().isPresent()) {
      LoggerFactory.getLogger(StorageConfiguration.class)
          .warn("Using hard-coded AWS credentials - this is not recommended for production");
      return Optional.of(
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(awsAccessKey().get(), awsSecretKey().get())));
    } else {
      return Optional.empty();
    }
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
