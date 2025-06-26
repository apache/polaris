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
package org.apache.polaris.service.quarkus.auth.external.mapping;

import static org.apache.polaris.service.quarkus.auth.external.OidcTenantResolvingAugmentor.getOidcTenantConfig;

import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.OptionalLong;
import org.eclipse.microprofile.jwt.JsonWebToken;

/**
 * A default implementation of {@link PrincipalMapper}. It maps the {@link SecurityIdentity} to a
 * Polaris principal by extracting the ID and name from the JWT claims, based on the configuration
 * provided in the OIDC tenant.
 */
@ApplicationScoped
@Identifier("default")
class DefaultPrincipalMapper implements PrincipalMapper {

  private final ClaimsLocator claimsLocator;

  @Inject
  public DefaultPrincipalMapper(ClaimsLocator claimsLocator) {
    this.claimsLocator = claimsLocator;
  }

  @Override
  public OptionalLong mapPrincipalId(SecurityIdentity identity) {
    var jwt = (JsonWebToken) identity.getPrincipal();
    var principalMapper = getOidcTenantConfig(identity).principalMapper();
    return principalMapper
        .idClaimPath()
        .map(claimPath -> claimsLocator.locateClaim(claimPath, jwt))
        .map(id -> id instanceof Number ? ((Number) id).longValue() : Long.parseLong(id.toString()))
        .map(OptionalLong::of)
        .orElse(OptionalLong.empty());
  }

  @Override
  public Optional<String> mapPrincipalName(SecurityIdentity identity) {
    var jwt = (JsonWebToken) identity.getPrincipal();
    var principalMapper = getOidcTenantConfig(identity).principalMapper();
    return principalMapper
        .nameClaimPath()
        .map(claimPath -> claimsLocator.locateClaim(claimPath, jwt))
        .map(Object::toString);
  }
}
