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
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.identity.resolved.ResolvedServiceIdentity;
import org.apache.polaris.core.secrets.ServiceSecretReference;
import org.apache.polaris.service.identity.ServiceIdentityConfiguration;

/**
 * Default implementation of {@link ServiceIdentityProvider} that resolves service identities from
 * statically configured values (typically defined via Quarkus server configuration).
 *
 * <p>This implementation supports both multi-tenant (e.g., SaaS) and self-managed (single-tenant)
 * Polaris deployments:
 *
 * <ul>
 *   <li>In multi-tenant mode, each tenant (realm) can have its own set of service identities
 *       defined in the configuration. The same identity will consistently be assigned for each
 *       {@link ServiceIdentityType} within a given tenant.
 *   <li>In single-tenant or self-managed deployments, a single set of service identities can be
 *       defined and used system-wide.
 * </ul>
 */
public class DefaultServiceIdentityProvider implements ServiceIdentityProvider {
  public static final String DEFAULT_REALM_KEY = ServiceIdentityConfiguration.DEFAULT_REALM_KEY;
  public static final String DEFAULT_REALM_NSS = "system:default";
  private static final String IDENTITY_INFO_REFERENCE_URN_FORMAT =
      "urn:polaris-secret:default-identity-provider:%s:%s";

  /** Map of service identity types to their resolved identities. */
  private final EnumMap<ServiceIdentityType, ResolvedServiceIdentity> resolvedServiceIdentities;

  /** Map of identity info references (URNs) to their resolved service identities. */
  private final Map<String, ResolvedServiceIdentity> referenceToResolvedServiceIdentity;

  public DefaultServiceIdentityProvider() {
    this(new EnumMap<>(ServiceIdentityType.class));
  }

  public DefaultServiceIdentityProvider(
      EnumMap<ServiceIdentityType, ResolvedServiceIdentity> serviceIdentities) {
    this.resolvedServiceIdentities = serviceIdentities;
    this.referenceToResolvedServiceIdentity =
        serviceIdentities.values().stream()
            .collect(
                Collectors.toMap(
                    identity -> identity.getIdentityInfoReference().getUrn(),
                    identity -> identity));
  }

  @Inject
  public DefaultServiceIdentityProvider(
      RealmContext realmContext, ServiceIdentityConfiguration serviceIdentityConfiguration) {
    this.resolvedServiceIdentities =
        serviceIdentityConfiguration.resolveServiceIdentities(realmContext).stream()
            .collect(
                // Collect to an EnumMap, grouping by ServiceIdentityType
                Collectors.toMap(
                    ResolvedServiceIdentity::getIdentityType,
                    identity -> identity,
                    (a, b) -> b,
                    () -> new EnumMap<>(ServiceIdentityType.class)));

    this.referenceToResolvedServiceIdentity =
        resolvedServiceIdentities.values().stream()
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

    ResolvedServiceIdentity resolvedServiceIdentity =
        resolvedServiceIdentities.get(serviceIdentityType);
    if (resolvedServiceIdentity == null) {
      return Optional.empty();
    }
    return Optional.of(resolvedServiceIdentity.asServiceIdentityInfoDpo());
  }

  @Override
  public Optional<ServiceIdentityInfo> getServiceIdentityInfo(
      @Nonnull ServiceIdentityInfoDpo serviceIdentityInfo) {
    ResolvedServiceIdentity resolvedServiceIdentity =
        referenceToResolvedServiceIdentity.get(
            serviceIdentityInfo.getIdentityInfoReference().getUrn());
    if (resolvedServiceIdentity == null) {
      return Optional.empty();
    }
    return Optional.of(resolvedServiceIdentity.asServiceIdentityInfoModel());
  }

  @Override
  public Optional<ResolvedServiceIdentity> resolveServiceIdentity(
      @Nonnull ServiceIdentityInfoDpo serviceIdentityInfo) {
    ResolvedServiceIdentity resolvedServiceIdentity =
        referenceToResolvedServiceIdentity.get(
            serviceIdentityInfo.getIdentityInfoReference().getUrn());
    return Optional.ofNullable(resolvedServiceIdentity);
  }

  @VisibleForTesting
  public EnumMap<ServiceIdentityType, ResolvedServiceIdentity> getResolvedServiceIdentities() {
    return resolvedServiceIdentities;
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
