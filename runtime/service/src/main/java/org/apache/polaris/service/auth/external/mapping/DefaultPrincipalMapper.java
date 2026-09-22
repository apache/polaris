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
package org.apache.polaris.service.auth.external.mapping;

import static org.apache.polaris.service.auth.external.tenant.OidcTenantResolvingAugmentor.getOidcTenantConfig;

import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.polaris.service.auth.external.tenant.OidcTenantConfiguration;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default implementation of {@link PrincipalMapper}. It maps the {@link SecurityIdentity} to a
 * Polaris principal by extracting the ID and name from the JWT claims, based on the configuration
 * provided in the OIDC tenant.
 *
 * <p>The configured ID claim is only usable as a Polaris principal ID when it carries a numeric
 * value, since Polaris principal IDs are {@code long} values. Non-numeric identifiers, such as the
 * UUID-based {@code sub} claim issued by Keycloak, are ignored instead of failing the
 * authentication; in that case the configured name claim is used to look the principal up by name.
 */
@ApplicationScoped
@Identifier("default")
class DefaultPrincipalMapper implements PrincipalMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPrincipalMapper.class);

  private final ClaimsLocator claimsLocator;

  @Inject
  public DefaultPrincipalMapper(ClaimsLocator claimsLocator) {
    this.claimsLocator = claimsLocator;
  }

  @Override
  public OptionalLong mapPrincipalId(SecurityIdentity identity) {
    var jwt = (JsonWebToken) identity.getPrincipal();
    OidcTenantConfiguration.PrincipalMapper principalMapper =
        getOidcTenantConfig(identity).principalMapper();
    return principalMapper
        .idClaimPath()
        .map(claimPath -> claimsLocator.locateClaim(claimPath, jwt))
        .map(DefaultPrincipalMapper::toPrincipalId)
        .filter(OptionalLong::isPresent)
        .map(OptionalLong::getAsLong)
        .map(OptionalLong::of)
        .orElse(OptionalLong.empty());
  }

  /**
   * Converts an ID claim value to a Polaris principal ID.
   *
   * <p>Numeric claim values, whether they arrive as numbers or as strings, are converted the same
   * way as before. A string claim that does not parse as a {@code long}, or that cannot fit into
   * one, is not a valid Polaris principal ID and yields an empty result: the principal is then
   * resolved by name, if a name claim is configured.
   */
  private static OptionalLong toPrincipalId(Object claim) {
    if (claim instanceof Number number) {
      return OptionalLong.of(number.longValue());
    }
    if (claim == null) {
      return OptionalLong.empty();
    }
    String id = claim.toString();
    try {
      return OptionalLong.of(Long.parseLong(id));
    } catch (NumberFormatException e) {
      LOGGER.debug(
          "Ignoring non-numeric principal ID claim {}: Polaris principal IDs must be numeric, "
              + "the principal will be resolved by name instead",
          id);
      return OptionalLong.empty();
    }
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
