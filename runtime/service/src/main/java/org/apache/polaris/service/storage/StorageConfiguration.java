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
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

public interface StorageConfiguration {

  Duration DEFAULT_TOKEN_LIFESPAN = Duration.ofHours(1);

  /**
   * The AWS access key to use for authentication. If not present, the default credentials provider
   * chain will be used.
   */
  Optional<String> awsAccessKey();

  /**
   * The AWS secret key to use for authentication. If not present, the default credentials provider
   * chain will be used.
   */
  Optional<String> awsSecretKey();

  /**
   * The GCP access token to use for authentication. If not present, the default credentials
   * provider chain will be used.
   */
  Optional<String> gcpAccessToken();

  /**
   * The lifespan of the GCP access token. If not present, the {@linkplain #DEFAULT_TOKEN_LIFESPAN
   * default token lifespan} will be used.
   */
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
    if (awsAccessKey().isPresent() && awsSecretKey().isPresent()) {
      LoggerFactory.getLogger(StorageConfiguration.class)
          .warn("Using hard-coded AWS credentials - this is not recommended for production");
      return StaticCredentialsProvider.create(
          AwsBasicCredentials.create(awsAccessKey().get(), awsSecretKey().get()));
    } else {
      return DefaultCredentialsProvider.builder().build();
    }
  }

  default Supplier<GoogleCredentials> gcpCredentialsSupplier() {
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
                        Instant.now()
                            .plus(gcpAccessTokenLifespan().orElse(DEFAULT_TOKEN_LIFESPAN))
                            .toEpochMilli()));
            return GoogleCredentials.create(accessToken);
          }
        });
  }
}
