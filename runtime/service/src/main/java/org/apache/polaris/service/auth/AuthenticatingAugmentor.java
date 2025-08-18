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

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.SecurityIdentityAugmentor;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.auth.PolarisPrincipal;

/**
 * A custom {@link SecurityIdentityAugmentor} that, after Quarkus OIDC or Internal Auth extracted
 * and validated the principal credentials, augments the {@link SecurityIdentity} by authenticating
 * the principal and setting a {@link PolarisPrincipal} as the identity's principal.
 */
@ApplicationScoped
public class AuthenticatingAugmentor implements SecurityIdentityAugmentor {

  public static final int PRIORITY = 1000;

  private final Authenticator authenticator;

  @Inject
  public AuthenticatingAugmentor(Authenticator authenticator) {
    this.authenticator = authenticator;
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
    PolarisCredential authInfo = extractPolarisCredential(identity);
    return context.runBlocking(() -> authenticatePolarisPrincipal(identity, authInfo));
  }

  private PolarisCredential extractPolarisCredential(SecurityIdentity identity) {
    PolarisCredential credential = identity.getCredential(PolarisCredential.class);
    if (credential == null) {
      throw new AuthenticationFailedException("No token credential available");
    }
    return credential;
  }

  private SecurityIdentity authenticatePolarisPrincipal(
      SecurityIdentity identity, PolarisCredential polarisCredential) {
    try {
      PolarisPrincipal polarisPrincipal = authenticator.authenticate(polarisCredential);
      return QuarkusSecurityIdentity.builder(identity).setPrincipal(polarisPrincipal).build();
    } catch (RuntimeException e) {
      throw new AuthenticationFailedException(e);
    }
  }
}
