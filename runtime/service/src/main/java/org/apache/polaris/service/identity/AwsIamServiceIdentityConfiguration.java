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
import org.apache.polaris.core.admin.model.AwsIamServiceIdentityInfo;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.credential.AwsIamServiceIdentityCredential;
import org.apache.polaris.core.secrets.ServiceSecretReference;
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
 * AwsIamServiceIdentityCredential}; otherwise, the AWS default credential provider chain is used.
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
   * Returns the type of service identity represented by this configuration, which is always {@link
   * ServiceIdentityType#AWS_IAM}.
   *
   * @return the AWS IAM service identity type
   */
  @Override
  default ServiceIdentityType getType() {
    return ServiceIdentityType.AWS_IAM;
  }

  /**
   * Returns the {@link AwsIamServiceIdentityInfo} model containing only the IAM ARN.
   *
   * <p>This method is lightweight and does not construct AWS credential providers. It should be
   * used for API responses where only identity metadata is needed.
   *
   * @return the service identity info model, or empty if the IAM ARN is not configured
   */
  @Override
  default Optional<AwsIamServiceIdentityInfo> asServiceIdentityInfoModel() {
    if (iamArn() == null) {
      return Optional.empty();
    }
    return Optional.of(
        AwsIamServiceIdentityInfo.builder()
            .setIdentityType(ServiceIdentityInfo.IdentityTypeEnum.AWS_IAM)
            .setIamArn(iamArn())
            .build());
  }

  /**
   * Converts this configuration into a {@link AwsIamServiceIdentityCredential} with actual AWS
   * credentials.
   *
   * <p>Creates a credential object containing the configured IAM ARN and AWS credentials provider.
   * The credentials provider is constructed based on whether static credentials (access key, secret
   * key, session token) are configured or whether to use the default AWS credential chain.
   *
   * <p>This method should only be called when credentials are actually needed for authentication.
   *
   * @param serviceIdentityReference the reference to associate with this credential
   * @return the service identity credential, or empty if the IAM ARN is not configured
   */
  @Override
  default Optional<AwsIamServiceIdentityCredential> asServiceIdentityCredential(
      @Nonnull ServiceSecretReference serviceIdentityReference) {
    if (iamArn() == null) {
      return Optional.empty();
    }
    return Optional.of(
        new AwsIamServiceIdentityCredential(
            serviceIdentityReference, iamArn(), awsCredentialsProvider()));
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
