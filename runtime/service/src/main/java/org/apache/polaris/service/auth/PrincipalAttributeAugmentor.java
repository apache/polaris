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
package org.apache.polaris.service.auth;

import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.SecurityIdentityAugmentor;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributeNamespaces;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PrincipalEntity;

/**
 * A {@link SecurityIdentityAugmentor} that runs after {@link AuthenticatingAugmentor} and projects
 * selected {@link PrincipalEntity} fields onto namespaced {@link PolarisPrincipal} attributes.
 *
 * <p>User-defined properties become {@code polaris.user.*}. Selected internal properties become
 * {@code polaris.system.*}. If the principal has no {@code PRINCIPAL_ENTITY} attribute (federated
 * or detached principals), this augmentor is a no-op.
 */
@ApplicationScoped
public class PrincipalAttributeAugmentor implements SecurityIdentityAugmentor {

  /** Must run after {@link AuthenticatingAugmentor}. */
  public static final int PRIORITY = AuthenticatingAugmentor.PRIORITY - 100;

  @Override
  public int priority() {
    return PRIORITY;
  }

  @Override
  public Uni<SecurityIdentity> augment(
      SecurityIdentity identity, AuthenticationRequestContext context) {
    if (identity.isAnonymous() || !(identity.getPrincipal() instanceof PolarisPrincipal)) {
      return Uni.createFrom().item(identity);
    }
    return Uni.createFrom().item(enrich(identity));
  }

  static SecurityIdentity enrich(SecurityIdentity identity) {
    PolarisPrincipal principal = (PolarisPrincipal) identity.getPrincipal();
    PrincipalEntity entity =
        principal
            .getAttribute(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, PrincipalEntity.class)
            .orElse(null);
    if (entity == null) {
      return identity;
    }
    Map<String, Object> derived = deriveFromEntity(entity);
    if (derived.isEmpty()) {
      return identity;
    }
    Map<String, Object> attributes = new LinkedHashMap<>(principal.getAttributes());
    attributes.putAll(derived);
    PolarisPrincipal enriched =
        PolarisPrincipal.of(principal.getName(), attributes, principal.getRoles());
    return QuarkusSecurityIdentity.builder(identity).setPrincipal(enriched).build();
  }

  static Map<String, Object> deriveFromEntity(PrincipalEntity entity) {
    Map<String, Object> derived = new LinkedHashMap<>();
    Map<String, String> internal = entity.getInternalPropertiesAsMap();
    putIfPresent(
        derived,
        PolarisPrincipalAttributeNamespaces.SYSTEM_CLIENT_ID,
        internal.get(PolarisEntityConstants.getClientIdPropertyName()));
    putIfPresent(
        derived,
        PolarisPrincipalAttributeNamespaces.SYSTEM_CREDENTIAL_ROTATION_REQUIRED,
        internal.get(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE));
    for (Map.Entry<String, String> property : entity.getPropertiesAsMap().entrySet()) {
      String key = property.getKey();
      String value = property.getValue();
      // Keep non-null stored values, including empty strings. The Principal API does not
      // reject blank property values, so dropping them here would hide them from authorizers.
      if (key != null && !key.isBlank() && value != null) {
        derived.put(PolarisPrincipalAttributeNamespaces.USER_PREFIX + key, value);
      }
    }
    return derived;
  }

  private static void putIfPresent(Map<String, Object> derived, String key, String value) {
    if (value != null && !value.isBlank()) {
      derived.put(key, value);
    }
  }
}
