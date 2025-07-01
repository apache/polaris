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
package org.apache.polaris.service.quarkus.auth;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.SecurityIdentityAugmentor;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.service.auth.ActiveRolesProvider;

/**
 * A custom {@link SecurityIdentityAugmentor} that adds active roles to the {@link
 * SecurityIdentity}. This is used to augment the identity with valid active roles after
 * authentication.
 */
@ApplicationScoped
public class ActiveRolesAugmentor implements SecurityIdentityAugmentor {

  // must run after AuthenticatingAugmentor
  public static final int PRIORITY = AuthenticatingAugmentor.PRIORITY - 1;

  private final ActiveRolesProvider activeRolesProvider;

  @Inject
  public ActiveRolesAugmentor(ActiveRolesProvider activeRolesProvider) {
    this.activeRolesProvider = activeRolesProvider;
  }

  @Override
  public int priority() {
    return PRIORITY;
  }

  @Override
  public Uni<SecurityIdentity> augment(
      SecurityIdentity identity, AuthenticationRequestContext context) {
    if (identity.isAnonymous()) {
      return Uni.createFrom().item(identity);
    }
    return context.runBlocking(() -> validateActiveRoles(identity));
  }

  private SecurityIdentity validateActiveRoles(SecurityIdentity identity) {
    if (!(identity.getPrincipal() instanceof AuthenticatedPolarisPrincipal)) {
      throw new AuthenticationFailedException("No Polaris principal found");
    }
    AuthenticatedPolarisPrincipal polarisPrincipal =
        identity.getPrincipal(AuthenticatedPolarisPrincipal.class);
    Set<String> validRoleNames = activeRolesProvider.getActiveRoles(polarisPrincipal);
    return QuarkusSecurityIdentity.builder()
        .setAnonymous(false)
        .setPrincipal(polarisPrincipal)
        .addRoles(validRoleNames) // replace the current roles with valid ones
        .addCredentials(identity.getCredentials())
        .addAttributes(identity.getAttributes())
        .addPermissionChecker(identity::checkPermission)
        .build();
  }
}
