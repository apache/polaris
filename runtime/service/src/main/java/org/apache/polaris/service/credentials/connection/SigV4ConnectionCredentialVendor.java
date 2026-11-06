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
import jakarta.annotation.Priority;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
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
import org.apache.polaris.service.credentials.CredentialVendorPriorities;
import org.apache.polaris.service.storage.StorageConfiguration;
import org.jspecify.annotations.NonNull;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

/**
 * Connection credential vendor for AWS SigV4 authentication.
 *
 * <p>Two forms, chosen by which authentication parameters are set.
 *
 * <p><b>Role assumption</b> — for AWS services such as Glue. Polaris uses its own {@link
 * AwsIamServiceIdentityCredential} to assume the customer's role via AWS STS, and returns the
 * temporary access key, secret key and session token.
 *
 * <p><b>Named server-side credentials</b> — for SigV4-compatible catalogs that are not AWS, which
 * have neither a role to assume nor an STS endpoint. The static keys configured under {@code
 * polaris.storage.aws.<storage-name>} are handed to the signer directly; STS is not involved. This
 * requires {@link FeatureConfiguration#RESOLVE_CREDENTIALS_BY_STORAGE_NAME}.
 *
 * <p>This is the default implementation with {@code @Priority(CredentialVendorPriorities.DEFAULT)}.
 * Custom implementations can override this by providing a higher priority value.
 */
@RequestScoped
@AuthType(AuthenticationType.SIGV4)
@Priority(CredentialVendorPriorities.DEFAULT)
public class SigV4ConnectionCredentialVendor implements ConnectionCredentialVendor {

  private static final String DEFAULT_ROLE_SESSION_NAME = "polaris";

  private final StsClientProvider stsClientProvider;
  private final ServiceIdentityProvider serviceIdentityProvider;
  private final StorageConfiguration storageConfiguration;
  private final RealmConfig realmConfig;

  @Inject
  public SigV4ConnectionCredentialVendor(
      StsClientProvider stsClientProvider,
      ServiceIdentityProvider serviceIdentityProvider,
      StorageConfiguration storageConfiguration,
      RealmConfig realmConfig) {
    this.stsClientProvider = stsClientProvider;
    this.serviceIdentityProvider = serviceIdentityProvider;
    this.storageConfiguration = storageConfiguration;
    this.realmConfig = realmConfig;
  }

  @Override
  public @NonNull ConnectionCredentials getConnectionCredentials(
      @NonNull ConnectionConfigInfoDpo connectionConfig) {

    // Validate and extract authentication parameters
    Preconditions.checkArgument(
        connectionConfig.getAuthenticationParameters() instanceof SigV4AuthenticationParametersDpo,
        "Expected SigV4AuthenticationParametersDpo, got: %s",
        connectionConfig.getAuthenticationParameters().getClass().getName());
    SigV4AuthenticationParametersDpo sigv4Params =
        (SigV4AuthenticationParametersDpo) connectionConfig.getAuthenticationParameters();

    if (sigv4Params.usesStorageCredentials()) {
      return storageCredentials(sigv4Params.getStorageName());
    }

    // Resolve the service identity credential
    Optional<ServiceIdentityCredential> serviceCredentialOpt =
        serviceIdentityProvider.getServiceIdentityCredential(connectionConfig.getServiceIdentity());
    if (serviceCredentialOpt.isEmpty()) {
      return ConnectionCredentials.EMPTY;
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
    AssumeRoleRequest.Builder requestBuilder =
        AssumeRoleRequest.builder()
            .roleArn(sigv4Params.getRoleArn())
            .roleSessionName(
                Optional.ofNullable(sigv4Params.getRoleSessionName())
                    .orElse(DEFAULT_ROLE_SESSION_NAME))
            .externalId(sigv4Params.getExternalId());

    // Attach user-provided session policy to restrict assumed role permissions (PoLP)
    if (StringUtils.isNotEmpty(sigv4Params.getSessionPolicy())) {
      requestBuilder.policy(sigv4Params.getSessionPolicy());
    }

    // Configure the request to use Polaris's service identity credentials
    requestBuilder.overrideConfiguration(
        config -> config.credentialsProvider(awsCredential.getAwsCredentialsProvider()));

    AssumeRoleResponse response = stsClient.assumeRole(requestBuilder.build());

    // Build connection credentials from AWS temporary credentials
    Map<CatalogAccessProperty, String> props = new HashMap<>();
    props.put(CatalogAccessProperty.AWS_ACCESS_KEY_ID, response.credentials().accessKeyId());
    props.put(
        CatalogAccessProperty.AWS_SECRET_ACCESS_KEY, response.credentials().secretAccessKey());
    props.put(CatalogAccessProperty.AWS_SESSION_TOKEN, response.credentials().sessionToken());
    Optional.ofNullable(response.credentials().expiration())
        .ifPresent(
            expiration ->
                props.put(
                    CatalogAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS,
                    String.valueOf(expiration.toEpochMilli())));

    return ConnectionCredentials.of(props);
  }

  /** Long-lived keys handed to the signer as-is. They do not expire, so no expiry is reported. */
  private ConnectionCredentials storageCredentials(@NonNull String storageName) {
    // Re-checked here, not only at catalog creation, so turning the flag off takes effect for
    // catalogs that already reference a storage name.
    FeatureConfiguration.enforceFeatureEnabledOrThrow(
        realmConfig, FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME);
    AwsCredentials credentials =
        storageConfiguration.stsCredentials(storageName).resolveCredentials();
    return ConnectionCredentials.of(
        Map.of(
            CatalogAccessProperty.AWS_ACCESS_KEY_ID,
            credentials.accessKeyId(),
            CatalogAccessProperty.AWS_SECRET_ACCESS_KEY,
            credentials.secretAccessKey()));
  }

  @VisibleForTesting
  StsClient getStsClient(@NonNull SigV4AuthenticationParametersDpo sigv4Params) {
    // Get STS client from the provider (potentially pooled)
    // The Polaris service identity credentials are set on the AssumeRole request via
    // overrideConfiguration, not on the STS client itself
    // TODO: Configure proper StsDestination with endpoint from sigv4Params
    return stsClientProvider.stsClient(
        StsClientProvider.StsDestination.of(null, sigv4Params.getSigningRegion()));
  }
}
