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

package org.apache.polaris.service.context.catalog;

import io.quarkus.security.identity.CurrentIdentityAssociation;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.UnsatisfiedResolutionException;
import java.security.Principal;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;

@RequestScoped
public class PolarisPrincipalHolder {

  private final AtomicReference<PolarisPrincipal> polarisPrincipal = new AtomicReference<>();

  @Produces
  @RequestScoped
  public PolarisPrincipal get(
      PolarisDiagnostics diagnostics, CurrentIdentityAssociation currentIdentityAssociation) {
    PolarisPrincipal setPrincipal = polarisPrincipal.get();

    if (setPrincipal != null) {
      return setPrincipal;
    }

    SecurityIdentity identity =
        currentIdentityAssociation.getDeferredIdentity().subscribeAsCompletionStage().getNow(null);

    if (identity == null) {
      throw new UnsatisfiedResolutionException("Not authenticated");
    }

    Principal userPrincipal = identity.getPrincipal();

    diagnostics.check(
        userPrincipal instanceof PolarisPrincipal,
        "unexpected_principal_type",
        "class={}",
        userPrincipal.getClass().getName());

    return (PolarisPrincipal) userPrincipal;
  }

  public void set(PolarisPrincipal rc) {
    if (!polarisPrincipal.compareAndSet(null, rc)) {
      throw new IllegalStateException("PolarisPrincipal already set");
    }
  }
}
