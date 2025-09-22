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

package org.apache.polaris.service.identity;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.resolved.ResolvedAwsIamServiceIdentity;
import org.apache.polaris.service.identity.registry.DefaultServiceIdentityRegistry;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Configuration for an AWS IAM service identity used by Polaris to access AWS services.
 *
 * <p>This includes the IAM ARN and optionally, static credentials (access key, secret key, and
 * session token). If credentials are provided, they will be used to construct a {@link
 * ResolvedAwsIamServiceIdentity}; otherwise, the AWS default credential provider chain is used.
 */
public interface AwsIamServiceIdentityConfiguration extends ResolvableServiceIdentityConfiguration {

  /** The IAM role or user ARN representing the service identity. */
  String iamArn();

  /**
   * Optional AWS access key ID associated with the IAM identity. If not provided, the AWS default
   * credential chain will be used.
   */
  Optional<String> accessKeyId();

  /**
   * Optional AWS secret access key associated with the IAM identity. If not provided, the AWS
   * default credential chain will be used.
   */
  Optional<String> secretAccessKey();

  /**
   * Optional AWS session token associated with the IAM identity. If not provided, the AWS default
   * credential chain will be used.
   */
  Optional<String> sessionToken();

  /**
   * Resolves this configuration into a {@link ResolvedAwsIamServiceIdentity} if the IAM ARN is
   * present.
   *
   * @return the resolved identity, or an empty optional if the ARN is missing
   */
  @Override
  default Optional<ResolvedAwsIamServiceIdentity> resolve(@Nonnull String realmIdentifier) {
    if (iamArn() == null) {
      return Optional.empty();
    } else {
      return Optional.of(
          new ResolvedAwsIamServiceIdentity(
              DefaultServiceIdentityRegistry.buildIdentityInfoReference(
                  realmIdentifier, ServiceIdentityType.AWS_IAM),
              iamArn(),
              awsCredentialsProvider()));
    }
  }

  /**
   * Constructs an {@link AwsCredentialsProvider} based on the configured access key, secret key,
   * and session token. If the access key and secret key are provided, a static credentials provider
   * is created; otherwise, the default credentials provider chain is used.
   *
   * @return the constructed AWS credentials provider
   */
  @Nonnull
  default AwsCredentialsProvider awsCredentialsProvider() {
    if (accessKeyId().isPresent() && secretAccessKey().isPresent()) {
      if (sessionToken().isPresent()) {
        return StaticCredentialsProvider.create(
            AwsSessionCredentials.create(
                accessKeyId().get(), secretAccessKey().get(), sessionToken().get()));
      } else {
        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKeyId().get(), secretAccessKey().get()));
      }
    } else {
      return DefaultCredentialsProvider.builder().build();
    }
  }
}
