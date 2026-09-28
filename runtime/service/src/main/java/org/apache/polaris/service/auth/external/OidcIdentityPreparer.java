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

import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration;
import org.apache.polaris.service.auth.CredentialMode;
import org.apache.polaris.service.auth.PolarisCredential;
import org.apache.polaris.service.auth.external.mapping.PrincipalMapper;
import org.apache.polaris.service.auth.external.mapping.PrincipalRolesMapper;
import org.apache.polaris.service.auth.external.tenant.OidcTenantConfiguration;
import org.apache.polaris.service.auth.external.tenant.OidcTenantResolver;
import org.eclipse.microprofile.jwt.JsonWebToken;

/**
 * Prepares an OIDC {@link SecurityIdentity} for authentication by resolving the OIDC tenant
 * configuration and mapping JWT claims to a {@link PolarisCredential}.
 *
 * <p>This bean consolidates the OIDC-specific preparation that must happen before {@link
 * org.apache.polaris.service.auth.Authenticator} is invoked. It:
 *
 * <ol>
 *   <li>Resolves the OIDC tenant configuration via {@link OidcTenantResolver} and attaches it as an
 *       identity attribute so that {@link
 *       org.apache.polaris.service.auth.external.mapping.PrincipalMapper} and {@link
 *       org.apache.polaris.service.auth.external.mapping.PrincipalRolesMapper} implementations can
 *       access it.
 *   <li>Maps the JWT claims to a {@link PolarisCredential} and adds it to the identity.
 * </ol>
 *
 * @see org.apache.polaris.service.auth.PolarisSecurityIdentityAugmentor
 */
@ApplicationScoped
public class OidcIdentityPreparer {

  public static final String TENANT_CONFIG_ATTRIBUTE =
      "org.apache.polaris.service.auth.TENANT_CONFIG";

  private final AuthenticationRealmConfiguration authConfig;
  private final OidcTenantResolver resolver;
  private final Instance<PrincipalMapper> principalMappers;
  private final Instance<PrincipalRolesMapper> principalRoleMappers;

  @Inject
  public OidcIdentityPreparer(
      AuthenticationRealmConfiguration authConfig,
      OidcTenantResolver resolver,
      @Any Instance<PrincipalMapper> principalMappers,
      @Any Instance<PrincipalRolesMapper> principalRoleMappers) {
    this.authConfig = authConfig;
    this.resolver = resolver;
    this.principalMappers = principalMappers;
    this.principalRoleMappers = principalRoleMappers;
  }

  public static OidcTenantConfiguration getOidcTenantConfig(SecurityIdentity identity) {
    return identity.getAttribute(TENANT_CONFIG_ATTRIBUTE);
  }

  /**
   * Prepares the given identity for authentication. For OIDC identities (where the principal is a
   * {@link JsonWebToken}), resolves the tenant configuration and maps JWT claims to a {@link
   * PolarisCredential}. Non-OIDC identities are returned unchanged.
   */
  public SecurityIdentity prepare(SecurityIdentity identity) {
    if (!(identity.getPrincipal() instanceof JsonWebToken)) {
      return identity;
    }
    OidcTenantConfiguration config = resolver.resolveConfig(identity);
    // Add the resolved config as an attribute so mappers can access it via getOidcTenantConfig()
    SecurityIdentity withConfig =
        QuarkusSecurityIdentity.builder(identity)
            .addAttribute(TENANT_CONFIG_ATTRIBUTE, config)
            .build();
    PrincipalMapper principalMapper =
        principalMappers.select(Identifier.Literal.of(config.principalMapper().type())).get();
    PrincipalRolesMapper rolesMapper =
        principalRoleMappers
            .select(Identifier.Literal.of(config.principalRolesMapper().type()))
            .get();
    String principalName = principalMapper.mapPrincipalName(withConfig).orElse(null);
    Set<String> principalRoles = rolesMapper.mapPrincipalRoles(withConfig);
    // Note: we build the credential even if it doesn't contain enough data to authenticate;
    // DefaultAuthenticator will reject it later on.
    PolarisCredential credential;
    if (authConfig.credentialMode() == CredentialMode.INTERNAL) {
      Long principalId =
          principalMapper.mapPrincipalId(withConfig).stream().boxed().findFirst().orElse(null);
      credential = PolarisCredential.of(principalId, principalName, principalRoles);
    } else {
      credential = PolarisCredential.ofExternal(principalName, principalRoles);
    }
    // Note: we don't change the identity roles here; this is done later by
    // AuthenticatingAugmentor, which also validates them.
    return QuarkusSecurityIdentity.builder(withConfig).addCredential(credential).build();
  }
}
