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

package org.apache.polaris.core.identity.registry;

import com.google.common.annotations.VisibleForTesting;
import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.resolved.ResolvedServiceIdentity;

/**
 * Default implementation of {@link ServiceIdentityRegistry} that resolves service identities from
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
public class DefaultServiceIdentityRegistry implements ServiceIdentityRegistry {

  /** Map of service identity types to their resolved identities. */
  private final EnumMap<ServiceIdentityType, ResolvedServiceIdentity> resolvedServiceIdentities;

  /** Map of identity info references (URNs) to their resolved service identities. */
  private final Map<String, ResolvedServiceIdentity> referenceToResolvedServiceIdentity;

  public DefaultServiceIdentityRegistry(
      EnumMap<ServiceIdentityType, ResolvedServiceIdentity> serviceIdentities) {
    this.resolvedServiceIdentities = serviceIdentities;
    this.referenceToResolvedServiceIdentity =
        serviceIdentities.values().stream()
            .collect(
                Collectors.toMap(
                    identity -> identity.getIdentityInfoReference().getUrn(),
                    identity -> identity));
  }

  @Override
  public ServiceIdentityInfoDpo assignServiceIdentity(ServiceIdentityType serviceIdentityType) {
    ResolvedServiceIdentity resolvedServiceIdentity =
        resolvedServiceIdentities.get(serviceIdentityType);
    if (resolvedServiceIdentity == null) {
      throw new IllegalArgumentException(
          "Service identity type not supported: " + serviceIdentityType);
    }
    return resolvedServiceIdentity.asServiceIdentityInfoDpo();
  }

  @Override
  public ResolvedServiceIdentity resolveServiceIdentity(
      ServiceIdentityInfoDpo serviceIdentityInfo) {
    ResolvedServiceIdentity resolvedServiceIdentity =
        referenceToResolvedServiceIdentity.get(
            serviceIdentityInfo.getIdentityInfoReference().getUrn());
    return resolvedServiceIdentity;
  }

  @VisibleForTesting
  public EnumMap<ServiceIdentityType, ResolvedServiceIdentity> getResolvedServiceIdentities() {
    return resolvedServiceIdentities;
  }
}
