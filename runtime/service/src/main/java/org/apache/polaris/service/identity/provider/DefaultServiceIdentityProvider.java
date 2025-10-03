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

package org.apache.polaris.service.identity.provider;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import jakarta.inject.Inject;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.credential.ServiceIdentityCredential;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.secrets.ServiceSecretReference;
import org.apache.polaris.service.identity.ServiceIdentityConfiguration;

/**
 * Default implementation of {@link ServiceIdentityProvider} that provides service identity
 * credentials from statically configured values.
 *
 * <p>This implementation loads service identity configurations at startup from Quarkus application
 * properties and maintains them in memory. It supports both multi-tenant and single-tenant
 * deployments:
 *
 * <ul>
 *   <li><b>Multi-tenant mode:</b> Each realm can define its own service identities. When allocating
 *       an identity to a catalog, the provider selects the appropriate identity based on the
 *       catalog's realm and authentication type.
 *   <li><b>Single-tenant mode:</b> A single default set of service identities is used for all
 *       catalogs.
 * </ul>
 *
 * <p>All service identities must be configured before server startup. This implementation does not
 * support dynamic credential rotation or runtime identity registration. Vendors requiring such
 * functionality should implement a custom {@link ServiceIdentityProvider}.
 */
public class DefaultServiceIdentityProvider implements ServiceIdentityProvider {
  public static final String DEFAULT_REALM_KEY = ServiceIdentityConfiguration.DEFAULT_REALM_KEY;
  public static final String DEFAULT_REALM_NSS = "system:default";
  private static final String IDENTITY_INFO_REFERENCE_URN_FORMAT =
      "urn:polaris-secret:default-identity-provider:%s:%s";

  /** Map of service identity types to their credentials. */
  private final EnumMap<ServiceIdentityType, ServiceIdentityCredential> serviceIdentityCredentials;

  /** Map of identity info references (URNs) to their service identity credentials. */
  private final Map<String, ServiceIdentityCredential> referenceToServiceIdentityCredential;

  public DefaultServiceIdentityProvider() {
    this(new EnumMap<>(ServiceIdentityType.class));
  }

  public DefaultServiceIdentityProvider(
      EnumMap<ServiceIdentityType, ServiceIdentityCredential> serviceIdentities) {
    this.serviceIdentityCredentials = serviceIdentities;
    this.referenceToServiceIdentityCredential =
        serviceIdentities.values().stream()
            .collect(
                Collectors.toMap(
                    identity -> identity.getIdentityInfoReference().getUrn(),
                    identity -> identity));
  }

  @Inject
  public DefaultServiceIdentityProvider(
      RealmContext realmContext, ServiceIdentityConfiguration serviceIdentityConfiguration) {
    this.serviceIdentityCredentials =
        serviceIdentityConfiguration.resolveServiceIdentityCredentials(realmContext).stream()
            .collect(
                // Collect to an EnumMap, grouping by ServiceIdentityType
                Collectors.toMap(
                    ServiceIdentityCredential::getIdentityType,
                    identity -> identity,
                    (a, b) -> b,
                    () -> new EnumMap<>(ServiceIdentityType.class)));

    this.referenceToServiceIdentityCredential =
        serviceIdentityCredentials.values().stream()
            .collect(
                Collectors.toMap(
                    identity -> identity.getIdentityInfoReference().getUrn(),
                    identity -> identity));
  }

  @Override
  public Optional<ServiceIdentityInfoDpo> allocateServiceIdentity(
      @Nonnull ConnectionConfigInfo connectionConfig) {
    // Determine the service identity type based on the authentication parameters
    if (connectionConfig.getAuthenticationParameters() == null) {
      return Optional.empty();
    }

    AuthenticationParameters.AuthenticationTypeEnum authenticationType =
        connectionConfig.getAuthenticationParameters().getAuthenticationType();

    ServiceIdentityType serviceIdentityType = null;
    if (authenticationType == AuthenticationParameters.AuthenticationTypeEnum.SIGV4) {
      serviceIdentityType = ServiceIdentityType.AWS_IAM;
    }
    // Add more authentication types and their corresponding service identity types as needed

    if (serviceIdentityType == null) {
      return Optional.empty();
    }

    ServiceIdentityCredential serviceIdentityCredential =
        serviceIdentityCredentials.get(serviceIdentityType);
    if (serviceIdentityCredential == null) {
      return Optional.empty();
    }
    return Optional.of(serviceIdentityCredential.asServiceIdentityInfoDpo());
  }

  @Override
  public Optional<ServiceIdentityInfo> getServiceIdentityInfo(
      @Nonnull ServiceIdentityInfoDpo serviceIdentityInfo) {
    ServiceIdentityCredential serviceIdentityCredential =
        referenceToServiceIdentityCredential.get(
            serviceIdentityInfo.getIdentityInfoReference().getUrn());
    if (serviceIdentityCredential == null) {
      return Optional.empty();
    }
    return Optional.of(serviceIdentityCredential.asServiceIdentityInfoModel());
  }

  @Override
  public Optional<ServiceIdentityCredential> getServiceIdentityCredential(
      @Nonnull ServiceIdentityInfoDpo serviceIdentityInfo) {
    ServiceIdentityCredential serviceIdentityCredential =
        referenceToServiceIdentityCredential.get(
            serviceIdentityInfo.getIdentityInfoReference().getUrn());
    return Optional.ofNullable(serviceIdentityCredential);
  }

  @VisibleForTesting
  public EnumMap<ServiceIdentityType, ServiceIdentityCredential> getServiceIdentityCredentials() {
    return serviceIdentityCredentials;
  }

  /**
   * Builds a {@link ServiceSecretReference} for the given realm and service identity type.
   *
   * <p>The URN format is:
   * urn:polaris-service-secret:default-identity-provider:&lt;realm&gt;:&lt;type&gt;
   *
   * <p>If the realm is the default realm key, it is replaced with "system:default" in the URN.
   *
   * @param realm the realm identifier
   * @param type the service identity type
   * @return the constructed service secret reference
   */
  public static ServiceSecretReference buildIdentityInfoReference(
      String realm, ServiceIdentityType type) {
    // urn:polaris-service-secret:default-identity-provider:<realm>:<type>
    return new ServiceSecretReference(
        IDENTITY_INFO_REFERENCE_URN_FORMAT.formatted(
            realm.equals(DEFAULT_REALM_KEY) ? DEFAULT_REALM_NSS : realm, type.name()),
        Map.of());
  }
}
