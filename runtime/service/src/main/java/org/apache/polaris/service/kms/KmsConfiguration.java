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
package org.apache.polaris.service.kms;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.docs.ConfigDocs;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

/** Server-local KMS credentials configuration. */
@ConfigMapping(prefix = "polaris.kms")
public interface KmsConfiguration {

  static KmsConfiguration empty() {
    return () ->
        new AwsKmsConfig() {
          @Override
          public Optional<String> accessKey() {
            return Optional.empty();
          }

          @Override
          public Optional<String> secretKey() {
            return Optional.empty();
          }

          @Override
          public Map<String, AwsKmsCredentialsConfig> keyManagementServices() {
            return Map.of();
          }
        };
  }

  @WithName("aws")
  AwsKmsConfig aws();

  interface AwsKmsConfig {
    /**
     * The AWS access key to use for KMS authentication. If not present, the default credentials
     * provider chain will be used.
     */
    @WithName("access-key")
    Optional<String> accessKey();

    /**
     * The AWS secret key to use for KMS authentication. If not present, the default credentials
     * provider chain will be used.
     */
    @WithName("secret-key")
    Optional<String> secretKey();

    @WithParentName
    @ConfigDocs.ConfigPropertyName("kms")
    Map<String, AwsKmsCredentialsConfig> keyManagementServices();
  }

  interface AwsKmsCredentialsConfig {
    /** The AWS access key to use for KMS authentication when using named KMS configurations. */
    @WithName("access-key")
    String accessKey();

    /** The AWS secret key to use for KMS authentication when using named KMS configurations. */
    @WithName("secret-key")
    String secretKey();
  }

  default Optional<AwsCredentials> awsCredentials(@Nullable String kmsName) {
    if (kmsName != null) {
      if (!aws().keyManagementServices().containsKey(kmsName)) {
        throw new IllegalArgumentException(
            "KMS name '" + kmsName + "' is not configured on the server");
      }
      AwsKmsCredentialsConfig kmsConfig = aws().keyManagementServices().get(kmsName);
      return Optional.of(awsCredentials(kmsConfig.accessKey(), kmsConfig.secretKey()));
    }

    if (aws().accessKey().isPresent() && aws().secretKey().isPresent()) {
      return Optional.of(awsCredentials(aws().accessKey().get(), aws().secretKey().get()));
    }

    return Optional.empty();
  }

  private static AwsCredentials awsCredentials(String accessKey, String secretKey) {
    return AwsBasicCredentials.create(accessKey, secretKey);
  }
}
