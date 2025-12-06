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

package org.apache.polaris.service.catalog;

import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.SecurityIdentityAugmentor;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@RequestScoped
public class RootPrincipalAugmentor implements SecurityIdentityAugmentor {

  @Inject PolarisMetaStoreManager innerMetaStoreManager;
  @Inject CallContext innerCallContext;

  @ConfigProperty(name = "test.augmentor.enabled", defaultValue = "false")
  boolean enabled;

  @Override
  public Uni<SecurityIdentity> augment(
      SecurityIdentity identity, AuthenticationRequestContext context) {
    if (!enabled) {
      return Uni.createFrom().item(identity);
    }

    PrincipalEntity rootPrincipal =
        innerMetaStoreManager
            .findRootPrincipal(innerCallContext.getPolarisCallContext())
            .orElseThrow();

    PolarisPrincipal principal = PolarisPrincipal.of(rootPrincipal, Set.of("service_admin"));

    return Uni.createFrom()
        .item(
            QuarkusSecurityIdentity.builder()
                .setPrincipal(principal)
                .addRole("service_admin")
                .setAnonymous(false)
                .build());
  }
}
