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
package org.apache.polaris.service.quarkus.auth.external;

import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.SecurityIdentityAugmentor;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.service.quarkus.auth.external.tenant.OidcTenantResolver;
import org.eclipse.microprofile.jwt.JsonWebToken;

/**
 * A {@link SecurityIdentityAugmentor} that resolves the Polaris OIDC tenant configuration for the
 * given identity and adds it as an attribute to the {@link SecurityIdentity}.
 */
@ApplicationScoped
public class OidcTenantResolvingAugmentor implements SecurityIdentityAugmentor {

  public static final String TENANT_CONFIG_ATTRIBUTE = "polaris-tenant-config";

  // must run before PrincipalAuthInfoAugmentor
  public static final int PRIORITY = PrincipalAuthInfoAugmentor.PRIORITY + 100;

  public static OidcTenantConfiguration getOidcTenantConfig(SecurityIdentity identity) {
    return identity.getAttribute(TENANT_CONFIG_ATTRIBUTE);
  }

  private final OidcTenantResolver resolver;

  @Inject
  public OidcTenantResolvingAugmentor(OidcTenantResolver resolver) {
    this.resolver = resolver;
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
    return Uni.createFrom().item(() -> setOidcTenantConfig(identity));
  }

  private SecurityIdentity setOidcTenantConfig(SecurityIdentity identity) {
    var config = resolver.resolveConfig(identity);
    return QuarkusSecurityIdentity.builder(identity)
        .addAttribute(TENANT_CONFIG_ATTRIBUTE, config)
        .build();
  }
}
