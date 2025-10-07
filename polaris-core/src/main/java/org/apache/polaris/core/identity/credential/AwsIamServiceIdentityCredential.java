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
package org.apache.polaris.core.identity.credential;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.admin.model.AwsIamServiceIdentityInfo;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.dpo.AwsIamServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.secrets.SecretReference;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

/**
 * Represents an AWS IAM service identity credential used by Polaris to authenticate to AWS
 * services.
 *
 * <p>This credential encapsulates:
 *
 * <ul>
 *   <li>The IAM ARN (role or user) representing the Polaris service identity
 *   <li>An {@link AwsCredentialsProvider} that supplies AWS credentials (access key, secret key,
 *       and optional session token)
 * </ul>
 *
 * <p>Polaris uses this identity to assume customer-provided IAM roles when accessing remote
 * catalogs with SigV4 authentication. The {@link AwsCredentialsProvider} can be configured to use
 * either:
 *
 * <ul>
 *   <li>Static credentials (for testing or single-tenant deployments)
 *   <li>DefaultCredentialsProvider (which chains through various AWS credential sources)
 *   <li>Custom credential providers (for vendor-specific secret management)
 * </ul>
 */
public class AwsIamServiceIdentityCredential extends ServiceIdentityCredential {

  /** IAM role or user ARN representing the Polaris service identity. */
  private final String iamArn;

  /** AWS credentials provider for accessing AWS services. */
  private final AwsCredentialsProvider awsCredentialsProvider;

  public AwsIamServiceIdentityCredential(@Nullable String iamArn) {
    this(null, iamArn, DefaultCredentialsProvider.builder().build());
  }

  public AwsIamServiceIdentityCredential(
      @Nullable String iamArn, @Nonnull AwsCredentialsProvider awsCredentialsProvider) {
    this(null, iamArn, awsCredentialsProvider);
  }

  public AwsIamServiceIdentityCredential(
      @Nullable SecretReference secretReference,
      @Nullable String iamArn,
      @Nonnull AwsCredentialsProvider awsCredentialsProvider) {
    super(ServiceIdentityType.AWS_IAM, secretReference);
    this.iamArn = iamArn;
    this.awsCredentialsProvider = awsCredentialsProvider;
  }

  public @Nullable String getIamArn() {
    return iamArn;
  }

  public @Nonnull AwsCredentialsProvider getAwsCredentialsProvider() {
    return awsCredentialsProvider;
  }

  @Override
  public @Nonnull ServiceIdentityInfoDpo asServiceIdentityInfoDpo() {
    return new AwsIamServiceIdentityInfoDpo(getIdentityInfoReference());
  }

  @Override
  public @Nonnull ServiceIdentityInfo asServiceIdentityInfoModel() {
    return AwsIamServiceIdentityInfo.builder()
        .setIdentityType(ServiceIdentityInfo.IdentityTypeEnum.AWS_IAM)
        .setIamArn(getIamArn())
        .build();
  }
}
