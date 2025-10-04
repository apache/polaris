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
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumMap;
import java.util.Optional;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.SigV4AuthenticationParametersDpo;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialProperty;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialVendor;
import org.apache.polaris.core.identity.credential.AwsIamServiceIdentityCredential;
import org.apache.polaris.core.identity.credential.ServiceIdentityCredential;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.storage.aws.StsClientProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

/**
 * Connection credential provider for AWS SigV4 authentication.
 *
 * <p>This provider uses Polaris's AWS IAM service identity to assume a customer-provided IAM role
 * via AWS STS, generating temporary credentials that Polaris uses to access external AWS services
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
 */
@ApplicationScoped
@SupportsAuthType(AuthenticationType.SIGV4)
public class SigV4ConnectionCredentialProvider implements ConnectionCredentialVendor {

  private final StsClientProvider stsClientProvider;
  private final ServiceIdentityProvider serviceIdentityProvider;

  @Inject
  public SigV4ConnectionCredentialProvider(
      StsClientProvider stsClientProvider, ServiceIdentityProvider serviceIdentityProvider) {
    this.stsClientProvider = stsClientProvider;
    this.serviceIdentityProvider = serviceIdentityProvider;
  }

  @Override
  public @Nonnull EnumMap<ConnectionCredentialProperty, String> getConnectionCredentials(
      @Nonnull ServiceIdentityInfoDpo serviceIdentity,
      @Nonnull AuthenticationParametersDpo authenticationParameters) {

    EnumMap<ConnectionCredentialProperty, String> credentialMap =
        new EnumMap<>(ConnectionCredentialProperty.class);

    // Resolve the service identity credential
    Optional<ServiceIdentityCredential> serviceCredentialOpt =
        serviceIdentityProvider.getServiceIdentityCredential(serviceIdentity);
    if (serviceCredentialOpt.isEmpty()) {
      return credentialMap;
    }

    // Cast to expected types for SigV4
    AwsIamServiceIdentityCredential awsCredential =
        (AwsIamServiceIdentityCredential) serviceCredentialOpt.get();
    SigV4AuthenticationParametersDpo sigv4Params =
        (SigV4AuthenticationParametersDpo) authenticationParameters;

    // Use Polaris's IAM identity to assume the customer's role
    StsClient stsClient = getStsClient(sigv4Params);

    // Build the AssumeRole request with Polaris's credentials
    AssumeRoleRequest.Builder requestBuilder =
        AssumeRoleRequest.builder()
            .roleArn(sigv4Params.getRoleArn())
            .roleSessionName(
                Optional.ofNullable(sigv4Params.getRoleSessionName()).orElse("polaris"))
            .externalId(sigv4Params.getExternalId());

    // Configure the request to use Polaris's service identity credentials
    requestBuilder.overrideConfiguration(
        config -> config.credentialsProvider(awsCredential.getAwsCredentialsProvider()));

    AssumeRoleResponse response = stsClient.assumeRole(requestBuilder.build());

    // Map AWS temporary credentials to connection properties
    credentialMap.put(
        ConnectionCredentialProperty.AWS_ACCESS_KEY_ID, response.credentials().accessKeyId());
    credentialMap.put(
        ConnectionCredentialProperty.AWS_SECRET_ACCESS_KEY,
        response.credentials().secretAccessKey());
    credentialMap.put(
        ConnectionCredentialProperty.AWS_SESSION_TOKEN, response.credentials().sessionToken());
    Optional.ofNullable(response.credentials().expiration())
        .ifPresent(
            expiration -> {
              credentialMap.put(
                  ConnectionCredentialProperty.EXPIRATION_TIME,
                  String.valueOf(expiration.toEpochMilli()));
            });

    return credentialMap;
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
