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
package org.apache.polaris.core.context;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores elements associated with an individual REST request such as RealmContext, caller
 * identity/role, authn/authz, etc. This class is distinct from RealmContext because implementations
 * may need to first independently resolve a RealmContext before resolving the identity/role
 * elements of the CallContext that reside exclusively within the resolved Realm. For example, the
 * principal/role entities may be defined within a Realm-specific persistence layer, and the
 * underlying nature of the persistence layer may differ between different realms.
 */
public interface CallContext extends AutoCloseable {
  InheritableThreadLocal<CallContext> CURRENT_CONTEXT = new InheritableThreadLocal<>();

  // For requests that make use of a Catalog instance, this holds the instance that was
  // created, scoped to the current call context.
  String REQUEST_PATH_CATALOG_INSTANCE_KEY = "REQUEST_PATH_CATALOG_INSTANCE";

  // Authenticator filters should populate this field alongside resolving a SecurityContext.
  // Value type: AuthenticatedPolarisPrincipal
  String AUTHENTICATED_PRINCIPAL = "AUTHENTICATED_PRINCIPAL";
  String CLOSEABLES = "closeables";

  static CallContext setCurrentContext(CallContext context) {
    CURRENT_CONTEXT.set(context);
    return context;
  }

  static CallContext getCurrentContext() {
    return CURRENT_CONTEXT.get();
  }

  static PolarisDiagnostics getDiagnostics() {
    return CURRENT_CONTEXT.get().getPolarisCallContext().getDiagServices();
  }

  static AuthenticatedPolarisPrincipal getAuthenticatedPrincipal() {
    return (AuthenticatedPolarisPrincipal)
        CallContext.getCurrentContext().contextVariables().get(CallContext.AUTHENTICATED_PRINCIPAL);
  }

  static void unsetCurrentContext() {
    CURRENT_CONTEXT.remove();
  }

  static CallContext of(
      final RealmContext realmContext, final PolarisCallContext polarisCallContext) {
    Map<String, Object> map = new HashMap<>();
    return new CallContext() {
      @Override
      public RealmContext getRealmContext() {
        return realmContext;
      }

      @Override
      public PolarisCallContext getPolarisCallContext() {
        return polarisCallContext;
      }

      @Override
      public Map<String, Object> contextVariables() {
        return map;
      }
    };
  }

  /**
   * Copy the {@link CallContext}. {@link #contextVariables()} will be copied except for {@link
   * #closeables()}. The original {@link #contextVariables()} map is untouched and {@link
   * #closeables()} in the original {@link CallContext} should be closed along with the {@link
   * CallContext}.
   */
  static CallContext copyOf(CallContext base) {
    RealmContext realmContext = base.getRealmContext();
    PolarisCallContext polarisCallContext = base.getPolarisCallContext();
    Map<String, Object> contextVariables =
        base.contextVariables().entrySet().stream()
            .filter(e -> !e.getKey().equals(CLOSEABLES))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return new CallContext() {
      @Override
      public RealmContext getRealmContext() {
        return realmContext;
      }

      @Override
      public PolarisCallContext getPolarisCallContext() {
        return polarisCallContext;
      }

      @Override
      public Map<String, Object> contextVariables() {
        return contextVariables;
      }
    };
  }

  RealmContext getRealmContext();

  /**
   * @return the inner context used for delegating services
   */
  PolarisCallContext getPolarisCallContext();

  Map<String, Object> contextVariables();

  default @Nonnull CloseableGroup closeables() {
    return (CloseableGroup)
        contextVariables().computeIfAbsent(CLOSEABLES, key -> new CloseableGroup());
  }

  @Override
  default void close() {
    if (CURRENT_CONTEXT.get() == this) {
      unsetCurrentContext();
      CloseableGroup closeables = closeables();
      try {
        closeables.close();
      } catch (IOException e) {
        Logger logger = LoggerFactory.getLogger(CallContext.class);
        logger
            .atWarn()
            .addKeyValue("closeableGroup", closeables)
            .log("Unable to close closeable group", e);
      }
    }
  }
}
