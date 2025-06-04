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
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.dpo.AwsIamServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.secrets.ServiceSecretReference;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

/**
 * Represents a fully resolved AWS IAM service identity, including the associated IAM ARN and
 * credentials. This class is used internally by Polaris to access AWS services on behalf of a
 * configured service identity.
 *
 * <p>It contains AWS credentials (access key, secret, and optional session token) and provides a
 * lazily initialized {@link StsClient} for performing role assumptions or identity verification.
 *
 * <p>The resolved identity can be converted back into its persisted DPO form using {@link
 * #asServiceIdentityInfoDpo()}.
 */
public class ResolvedAwsIamServiceIdentity extends ResolvedServiceIdentity {

  /** IAM role or user ARN representing the Polaris service identity. */
  private final String iamArn;

  /** AWS access key ID of the AWS credential associated with the identity. */
  private final String accessKeyId;

  /** AWS secret access key of the AWS credential associated with the identity. */
  private final String secretAccessKey;

  /** The AWS session token of the AWS credential associated with the identity. */
  private final String sessionToken;

  public ResolvedAwsIamServiceIdentity(
      String iamArn, String accessKeyId, String secretAccessKey, String sessionToken) {
    this(null, iamArn, accessKeyId, secretAccessKey, sessionToken);
  }

  public ResolvedAwsIamServiceIdentity(
      ServiceSecretReference serviceSecretReference,
      String iamArn,
      String accessKeyId,
      String secretAccessKey,
      String sessionToken) {
    super(ServiceIdentityType.AWS_IAM, serviceSecretReference);
    this.iamArn = iamArn;
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
  }

  public String getIamArn() {
    return iamArn;
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }

  public String getSessionToken() {
    return sessionToken;
  }

  @Nonnull
  @Override
  public ServiceIdentityInfoDpo asServiceIdentityInfoDpo() {
    return new AwsIamServiceIdentityInfoDpo(getIdentityInfoReference(), getIamArn());
  }

  /** Returns a memoized supplier for creating an STS client using the resolved credentials. */
  public Supplier<StsClient> stsClientSupplier() {
    return Suppliers.memoize(
        () -> {
          StsClientBuilder stsClientBuilder = StsClient.builder();
          if (getAccessKeyId() != null && getSecretAccessKey() != null) {
            StaticCredentialsProvider awsCredentialsProvider =
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(getAccessKeyId(), getSecretAccessKey()));
            if (getSessionToken() != null) {
              awsCredentialsProvider =
                  StaticCredentialsProvider.create(
                      AwsSessionCredentials.create(
                          getAccessKeyId(), getSecretAccessKey(), getSessionToken()));
            }
            stsClientBuilder.credentialsProvider(awsCredentialsProvider);
          }
          return stsClientBuilder.build();
        });
  }
}
