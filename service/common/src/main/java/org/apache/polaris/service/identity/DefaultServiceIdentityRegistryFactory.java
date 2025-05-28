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
package org.apache.polaris.service.identity;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.registry.DefaultServiceIdentityRegistry;
import org.apache.polaris.core.identity.registry.ServiceIdentityRegistry;
import org.apache.polaris.core.identity.registry.ServiceIdentityRegistryFactory;
import org.apache.polaris.core.identity.resolved.ResolvedServiceIdentity;
import org.apache.polaris.core.secrets.ServiceSecretReference;

@ApplicationScoped
@Identifier("default")
public class DefaultServiceIdentityRegistryFactory implements ServiceIdentityRegistryFactory {
  private static final String DEFAULT_REALM_KEY = ServiceIdentityConfiguration.DEFAULT_REALM_KEY;
  private static final String IDENTITY_INFO_REFERENCE_URN_FORMAT =
      "urn:polaris-service-secret:default-identity-registry:%s:%s";

  private final Map<String, DefaultServiceIdentityRegistry> realmServiceIdentityRegistries;

  @Inject
  public DefaultServiceIdentityRegistryFactory(
      ServiceIdentityConfiguration<?> serviceIdentityConfiguration) {
    realmServiceIdentityRegistries =
        serviceIdentityConfiguration.realms().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, // realm identifier
                    entry -> {
                      RealmServiceIdentityConfiguration realmConfig = entry.getValue();

                      // Resolve all the service identities for the realm
                      EnumMap<ServiceIdentityType, ResolvedServiceIdentity> resolvedIdentities =
                          realmConfig.serviceIdentityConfigurations().stream()
                              .map(ResolvableServiceIdentityConfiguration::resolve)
                              .flatMap(Optional::stream)
                              .peek(
                                  // Set the identity info reference for each resolved identity
                                  identity ->
                                      identity.setIdentityInfoReference(
                                          buildIdentityInfoReference(
                                              entry.getKey(), identity.getIdentityType())))
                              .collect(
                                  // Collect to an EnumMap, grouping by ServiceIdentityType
                                  Collectors.toMap(
                                      ResolvedServiceIdentity::getIdentityType,
                                      identity -> identity,
                                      (a, b) -> b,
                                      () -> new EnumMap<>(ServiceIdentityType.class)));
                      return new DefaultServiceIdentityRegistry(resolvedIdentities);
                    }));

    if (!realmServiceIdentityRegistries.containsKey(DEFAULT_REALM_KEY)) {
      // If no default realm is defined, create an empty registry
      realmServiceIdentityRegistries.put(
          DEFAULT_REALM_KEY,
          new DefaultServiceIdentityRegistry(new EnumMap<>(ServiceIdentityType.class)));
    }
  }

  public DefaultServiceIdentityRegistryFactory() {
    this(new DefaultServiceIdentityRegistry(new EnumMap<>(ServiceIdentityType.class)));
  }

  public DefaultServiceIdentityRegistryFactory(
      DefaultServiceIdentityRegistry defaultServiceIdentityRegistry) {
    this(Map.of(DEFAULT_REALM_KEY, defaultServiceIdentityRegistry));
  }

  public DefaultServiceIdentityRegistryFactory(
      Map<String, DefaultServiceIdentityRegistry> realmServiceIdentityRegistries) {
    this.realmServiceIdentityRegistries = realmServiceIdentityRegistries;

    if (!realmServiceIdentityRegistries.containsKey(DEFAULT_REALM_KEY)) {
      // If no default realm is defined, create an empty registry
      realmServiceIdentityRegistries.put(
          DEFAULT_REALM_KEY,
          new DefaultServiceIdentityRegistry(new EnumMap<>(ServiceIdentityType.class)));
    }
  }

  @Override
  public ServiceIdentityRegistry getOrCreateServiceIdentityRegistry(RealmContext realmContext) {
    return getServiceIdentityRegistryForRealm(realmContext);
  }

  protected DefaultServiceIdentityRegistry getServiceIdentityRegistryForRealm(
      RealmContext realmContext) {
    return getServiceIdentityRegistryForRealm(realmContext.getRealmIdentifier());
  }

  protected DefaultServiceIdentityRegistry getServiceIdentityRegistryForRealm(
      String realmIdentifier) {
    return realmServiceIdentityRegistries.getOrDefault(
        realmIdentifier, realmServiceIdentityRegistries.get(DEFAULT_REALM_KEY));
  }

  private ServiceSecretReference buildIdentityInfoReference(
      String realm, ServiceIdentityType type) {
    // urn:polaris-service-secret:default-identity-registry:<realm>:<type>
    return new ServiceSecretReference(
        IDENTITY_INFO_REFERENCE_URN_FORMAT.formatted(realm, type.name()), Map.of());
  }
}
