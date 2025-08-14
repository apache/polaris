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
package org.apache.polaris.service.auth.external;

import static org.apache.polaris.service.auth.external.tenant.OidcTenantResolvingAugmentor.getOidcTenantConfig;

import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.SecurityIdentityAugmentor;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.polaris.service.auth.AuthenticatingAugmentor;
import org.apache.polaris.service.auth.PolarisCredential;
import org.apache.polaris.service.auth.external.mapping.PrincipalMapper;
import org.apache.polaris.service.auth.external.mapping.PrincipalRolesMapper;
import org.apache.polaris.service.auth.external.tenant.OidcTenantConfiguration;
import org.eclipse.microprofile.jwt.JsonWebToken;

/**
 * A {@link SecurityIdentityAugmentor} that maps the access token claims, as provided by the OIDC
 * authentication mechanism, to a {@link PolarisCredential}.
 */
@ApplicationScoped
public class OidcPolarisCredentialAugmentor implements SecurityIdentityAugmentor {

  // must run before the authenticating augmentor
  public static final int PRIORITY = AuthenticatingAugmentor.PRIORITY + 100;

  private final Instance<PrincipalMapper> principalMappers;
  private final Instance<PrincipalRolesMapper> principalRoleMappers;

  @Inject
  public OidcPolarisCredentialAugmentor(
      @Any Instance<PrincipalMapper> principalMappers,
      @Any Instance<PrincipalRolesMapper> principalRoleMappers) {
    this.principalMappers = principalMappers;
    this.principalRoleMappers = principalRoleMappers;
  }

  @Override
  public int priority() {
    return PRIORITY;
  }

  @Override
  public Uni<SecurityIdentity> augment(
      SecurityIdentity identity, AuthenticationRequestContext context) {
    if (identity.isAnonymous() || !(identity.getPrincipal() instanceof JsonWebToken)) {
      return Uni.createFrom().item(identity);
    }
    OidcTenantConfiguration config = getOidcTenantConfig(identity);
    PrincipalMapper principalMapper =
        principalMappers.select(Identifier.Literal.of(config.principalMapper().type())).get();
    PrincipalRolesMapper principalRolesMapper =
        principalRoleMappers
            .select(Identifier.Literal.of(config.principalRolesMapper().type()))
            .get();
    // The mappers may do expensive work, hence we run within a blocking context
    return context.runBlocking(
        () -> setPolarisCredential(identity, principalMapper, principalRolesMapper));
  }

  protected SecurityIdentity setPolarisCredential(
      SecurityIdentity identity,
      PrincipalMapper principalMapper,
      PrincipalRolesMapper rolesMapper) {
    Long principalId =
        principalMapper.mapPrincipalId(identity).stream().boxed().findFirst().orElse(null);
    String principalName = principalMapper.mapPrincipalName(identity).orElse(null);
    Set<String> principalRoles = rolesMapper.mapPrincipalRoles(identity);
    PolarisCredential credential = PolarisCredential.of(principalId, principalName, principalRoles);
    // Note: we don't change the identity roles here, this will be done later on
    // by the AuthenticatingAugmentor, which will also validate them.
    return QuarkusSecurityIdentity.builder(identity).addCredential(credential).build();
  }
}
