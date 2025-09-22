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
package org.apache.polaris.core.identity.resolved;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.admin.model.AwsIamServiceIdentityInfo;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.dpo.AwsIamServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.secrets.ServiceSecretReference;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

/**
 * Represents a fully resolved AWS IAM service identity, including the associated IAM ARN and
 * credentials. Polaris uses this class internally to access AWS services on behalf of a configured
 * service identity.
 *
 * <p>It contains AWS credentials (access key, secret, and optional session token) and provides a
 * lazily initialized {@link StsClient} for performing role assumptions or identity verification.
 *
 * <p>The resolved identity can be converted back into its persisted DPO form using {@link
 * #asServiceIdentityInfoDpo()}.
 *
 * <p>The resolved identity can also be converted into its API model representation using {@link
 * #asServiceIdentityInfoModel()}
 */
public class ResolvedAwsIamServiceIdentity extends ResolvedServiceIdentity {

  /** IAM role or user ARN representing the Polaris service identity. */
  private final String iamArn;

  /** AWS credentials provider for accessing AWS services. */
  private final AwsCredentialsProvider awsCredentialsProvider;

  public ResolvedAwsIamServiceIdentity(@Nullable String iamArn) {
    this(null, iamArn, DefaultCredentialsProvider.builder().build());
  }

  public ResolvedAwsIamServiceIdentity(
      @Nullable String iamArn, @Nonnull AwsCredentialsProvider awsCredentialsProvider) {
    this(null, iamArn, awsCredentialsProvider);
  }

  public ResolvedAwsIamServiceIdentity(
      @Nullable ServiceSecretReference serviceSecretReference,
      @Nullable String iamArn,
      @Nonnull AwsCredentialsProvider awsCredentialsProvider) {
    super(ServiceIdentityType.AWS_IAM, serviceSecretReference);
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

  /** Returns a memoized supplier for creating an STS client using the resolved credentials. */
  public @Nonnull Supplier<StsClient> stsClientSupplier() {
    return Suppliers.memoize(
        () -> {
          StsClientBuilder stsClientBuilder =
              StsClient.builder().credentialsProvider(getAwsCredentialsProvider());
          return stsClientBuilder.build();
        });
  }
}
