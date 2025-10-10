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
package org.apache.polaris.service.credentials.connection;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.SigV4AuthenticationParametersDpo;
import org.apache.polaris.core.credentials.connection.CatalogAccessProperty;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialVendor;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.core.identity.credential.AwsIamServiceIdentityCredential;
import org.apache.polaris.core.identity.credential.ServiceIdentityCredential;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.storage.aws.StsClientProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

/**
 * Connection credential vendor for AWS SigV4 authentication.
 *
 * <p>This vendor uses Polaris's AWS IAM service identity to assume a customer-provided IAM role via
 * AWS STS, generating temporary credentials that Polaris uses to access external AWS services
 * (e.g., AWS Glue catalog) with SigV4 request signing.
 *
 * <p>Flow:
 *
 * <ol>
 *   <li>Receives Polaris's {@link AwsIamServiceIdentityCredential} (the IAM user/role Polaris owns)
 *   <li>Extracts customer's role ARN from {@link SigV4AuthenticationParametersDpo}
 *   <li>Calls AWS STS AssumeRole to get temporary credentials
 *   <li>Returns temporary access key, secret key, and session token
 * </ol>
 *
 * <p>This is the default implementation with {@code @Priority(100)}. Custom implementations can
 * override this by providing a higher priority value.
 */
@RequestScoped
@AuthType(AuthenticationType.SIGV4)
@Priority(100)
public class SigV4ConnectionCredentialVendor implements ConnectionCredentialVendor {

  private static final String DEFAULT_ROLE_SESSION_NAME = "polaris";

  private final StsClientProvider stsClientProvider;
  private final ServiceIdentityProvider serviceIdentityProvider;

  @Inject
  public SigV4ConnectionCredentialVendor(
      StsClientProvider stsClientProvider, ServiceIdentityProvider serviceIdentityProvider) {
    this.stsClientProvider = stsClientProvider;
    this.serviceIdentityProvider = serviceIdentityProvider;
  }

  @Override
  public @Nonnull ConnectionCredentials getConnectionCredentials(
      @Nonnull ConnectionConfigInfoDpo connectionConfig) {

    // Validate and extract authentication parameters
    Preconditions.checkArgument(
        connectionConfig.getAuthenticationParameters() instanceof SigV4AuthenticationParametersDpo,
        "Expected SigV4AuthenticationParametersDpo, got: %s",
        connectionConfig.getAuthenticationParameters().getClass().getName());
    SigV4AuthenticationParametersDpo sigv4Params =
        (SigV4AuthenticationParametersDpo) connectionConfig.getAuthenticationParameters();

    // Resolve the service identity credential
    Optional<ServiceIdentityCredential> serviceCredentialOpt =
        serviceIdentityProvider.getServiceIdentityCredential(connectionConfig.getServiceIdentity());
    if (serviceCredentialOpt.isEmpty()) {
      return ConnectionCredentials.builder().build();
    }

    // Validate and cast service identity credential
    ServiceIdentityCredential serviceCredential = serviceCredentialOpt.get();
    Preconditions.checkArgument(
        serviceCredential instanceof AwsIamServiceIdentityCredential,
        "Expected AwsIamServiceIdentityCredential, got: %s",
        serviceCredential.getClass().getName());
    AwsIamServiceIdentityCredential awsCredential =
        (AwsIamServiceIdentityCredential) serviceCredential;

    // Use Polaris's IAM identity to assume the customer's role
    StsClient stsClient = getStsClient(sigv4Params);

    // Build the AssumeRole request with Polaris's credentials
    // TODO: Generate service-level scoping policy to restrict permissions
    AssumeRoleRequest.Builder requestBuilder =
        AssumeRoleRequest.builder()
            .roleArn(sigv4Params.getRoleArn())
            .roleSessionName(
                Optional.ofNullable(sigv4Params.getRoleSessionName())
                    .orElse(DEFAULT_ROLE_SESSION_NAME))
            .externalId(sigv4Params.getExternalId());

    // Configure the request to use Polaris's service identity credentials
    requestBuilder.overrideConfiguration(
        config -> config.credentialsProvider(awsCredential.getAwsCredentialsProvider()));

    AssumeRoleResponse response = stsClient.assumeRole(requestBuilder.build());

    // Build connection credentials from AWS temporary credentials
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();
    builder.put(CatalogAccessProperty.AWS_ACCESS_KEY_ID, response.credentials().accessKeyId());
    builder.put(
        CatalogAccessProperty.AWS_SECRET_ACCESS_KEY, response.credentials().secretAccessKey());
    builder.put(CatalogAccessProperty.AWS_SESSION_TOKEN, response.credentials().sessionToken());
    Optional.ofNullable(response.credentials().expiration())
        .ifPresent(
            expiration ->
                builder.put(
                    CatalogAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS,
                    String.valueOf(expiration.toEpochMilli())));

    return builder.build();
  }

  @VisibleForTesting
  StsClient getStsClient(@Nonnull SigV4AuthenticationParametersDpo sigv4Params) {
    // Get STS client from the provider (potentially pooled)
    // The Polaris service identity credentials are set on the AssumeRole request via
    // overrideConfiguration, not on the STS client itself
    // TODO: Configure proper StsDestination with region/endpoint from sigv4Params
    return stsClientProvider.stsClient(StsClientProvider.StsDestination.of(null, null));
  }
}
