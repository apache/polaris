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
import java.net.URI;
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
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

/**
 * Connection credential vendor for AWS SigV4 authentication.
 *
 * <p>STS is used when {@code roleArn} is set. The keys that call AssumeRole come from {@code
 * storageName} when that is set, otherwise from Polaris's {@link AwsIamServiceIdentityCredential}.
 *
 * <p>When {@code roleArn} is absent, the static keys under {@code
 * polaris.storage.aws.<storage-name>} are handed to the signer directly. Named keys require {@link
 * FeatureConfiguration#RESOLVE_CREDENTIALS_BY_STORAGE_NAME}.
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

    if (StringUtils.isNotEmpty(sigv4Params.getRoleArn())) {
      return assumeRole(connectionConfig, sigv4Params);
    }
    return storageCredentials(sigv4Params.getStorageName());
  }

  private ConnectionCredentials assumeRole(
      @NonNull ConnectionConfigInfoDpo connectionConfig,
      @NonNull SigV4AuthenticationParametersDpo sigv4Params) {
    AwsCredentialsProvider baseCredentials = baseCredentials(connectionConfig, sigv4Params);
    if (baseCredentials == null) {
      return ConnectionCredentials.EMPTY;
    }

    StsClient stsClient = getStsClient(sigv4Params);
    AssumeRoleRequest.Builder requestBuilder =
        AssumeRoleRequest.builder()
            .roleArn(sigv4Params.getRoleArn())
            .roleSessionName(
                Optional.ofNullable(sigv4Params.getRoleSessionName())
                    .orElse(DEFAULT_ROLE_SESSION_NAME))
            .externalId(sigv4Params.getExternalId());

    if (StringUtils.isNotEmpty(sigv4Params.getSessionPolicy())) {
      requestBuilder.policy(sigv4Params.getSessionPolicy());
    }

    requestBuilder.overrideConfiguration(config -> config.credentialsProvider(baseCredentials));

    AssumeRoleResponse response = stsClient.assumeRole(requestBuilder.build());

    Map<CatalogAccessProperty, String> props = new HashMap<>();
    props.put(CatalogAccessProperty.AWS_ACCESS_KEY_ID, response.credentials().accessKeyId());
    props.put(
        CatalogAccessProperty.AWS_SECRET_ACCESS_KEY, response.credentials().secretAccessKey());
    props.put(CatalogAccessProperty.AWS_SESSION_TOKEN, response.credentials().sessionToken());
    Optional.ofNullable(response.credentials().expiration())
        .ifPresent(expiration ->
            props.put(
                CatalogAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS,
                String.valueOf(expiration.toEpochMilli())));

    return ConnectionCredentials.of(props);
  }

  private AwsCredentialsProvider baseCredentials(
      @NonNull ConnectionConfigInfoDpo connectionConfig,
      @NonNull SigV4AuthenticationParametersDpo sigv4Params) {
    if (sigv4Params.usesStorageCredentials()) {
      return namedCredentialsProvider(sigv4Params.getStorageName());
    }

    Optional<ServiceIdentityCredential> serviceCredentialOpt =
        serviceIdentityProvider.getServiceIdentityCredential(connectionConfig.getServiceIdentity());
    if (serviceCredentialOpt.isEmpty()) {
      return null;
    }

    ServiceIdentityCredential serviceCredential = serviceCredentialOpt.get();
    Preconditions.checkArgument(
        serviceCredential instanceof AwsIamServiceIdentityCredential,
        "Expected AwsIamServiceIdentityCredential, got: %s",
        serviceCredential.getClass().getName());
    return ((AwsIamServiceIdentityCredential) serviceCredential).getAwsCredentialsProvider();
  }

  /** Long-lived keys handed to the signer as-is. They do not expire, so no expiry is reported. */
  private ConnectionCredentials storageCredentials(@NonNull String storageName) {
    AwsCredentials credentials = namedCredentialsProvider(storageName).resolveCredentials();
    return ConnectionCredentials.of(
        Map.of(
            CatalogAccessProperty.AWS_ACCESS_KEY_ID,
            credentials.accessKeyId(),
            CatalogAccessProperty.AWS_SECRET_ACCESS_KEY,
            credentials.secretAccessKey()));
  }

  private AwsCredentialsProvider namedCredentialsProvider(@NonNull String storageName) {
    // Re-checked here, not only at catalog creation, so turning the flag off takes effect for
    // catalogs that already reference a storage name.
    FeatureConfiguration.enforceFeatureEnabledOrThrow(
        realmConfig, FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME);
    return storageConfiguration.stsCredentials(storageName);
  }

  @VisibleForTesting
  StsClient getStsClient(@NonNull SigV4AuthenticationParametersDpo sigv4Params) {
    // Get STS client from the provider (potentially pooled)
    // The Polaris service identity credentials are set on the AssumeRole request via
    // overrideConfiguration, not on the STS client itself
    // A null endpoint means the SDK default STS endpoint is used
    String stsEndpoint = sigv4Params.getStsEndpoint();
    URI endpoint = stsEndpoint == null ? null : URI.create(stsEndpoint);
    return stsClientProvider.stsClient(
        StsClientProvider.StsDestination.of(endpoint, sigv4Params.getSigningRegion()));
  }
}
