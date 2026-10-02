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
import jakarta.inject.Inject;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.auth.external.OidcIdentityPreparer;

/**
 * A custom {@link SecurityIdentityAugmentor} that augments the already-authenticated {@link
 * SecurityIdentity} with Polaris-specific requirements.
 *
 * <p>First, it prepares OIDC identities, if applicable, so that all identities expose a valid
 * {@link PolarisCredential}; and finally, it invokes the {@link Authenticator} and sets the
 * produced {@link PolarisPrincipal} as the identity's principal.
 *
 * @see OidcIdentityPreparer
 * @see Authenticator
 */
@ApplicationScoped
public class PolarisSecurityIdentityAugmentor implements SecurityIdentityAugmentor {

  public static final int PRIORITY = 1000;

  private final Authenticator authenticator;
  private final OidcIdentityPreparer oidcIdentityPreparer;

  @Inject
  public PolarisSecurityIdentityAugmentor(
      Authenticator authenticator, OidcIdentityPreparer oidcIdentityPreparer) {
    this.authenticator = authenticator;
    this.oidcIdentityPreparer = oidcIdentityPreparer;
  }

  @Override
  public int priority() {
    return PRIORITY;
  }

  @Override
  public Uni<SecurityIdentity> augment(
      SecurityIdentity identity, AuthenticationRequestContext context) {
    return identity.isAnonymous()
        ? Uni.createFrom().item(identity)
        : context.runBlocking(() -> authenticatePolarisPrincipal(identity));
  }

  private SecurityIdentity authenticatePolarisPrincipal(SecurityIdentity identity) {
    SecurityIdentity preparedIdentity = oidcIdentityPreparer.prepare(identity);
    PolarisPrincipal polarisPrincipal = authenticator.authenticate(preparedIdentity);
    // Do not merge the principal attributes into the security identity's attributes:
    // these must stay separate.
    return QuarkusSecurityIdentity.builder(preparedIdentity)
        .setAnonymous(false)
        .setPrincipal(polarisPrincipal)
        .addRoles(polarisPrincipal.getRoles())
        .build();
  }
}
