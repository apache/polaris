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
package org.apache.polaris.service.idempotency;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.core.persistence.IdempotencyStoreFactory;

/**
 * Produces the request-scoped {@link IdempotencyStore} that {@link IdempotencyHandlerSupport}
 * injects directly — so the handler has no factory reference and no lazy-init logic.
 *
 * <p>This producer lives in the service runtime (which has a {@link RealmContext}), not in the
 * persistence backends: backends expose a realm-agnostic {@link IdempotencyStoreFactory} (realm as
 * a method argument) so they stay deployable where there is no request scope (e.g. the Admin Tool).
 * This producer binds the selected backend to the current request's realm.
 *
 * <p>When idempotency is disabled it yields {@link NoOpIdempotencyStore}; handlers short-circuit on
 * {@link IdempotencyHandlerSupport#isEnabled()} and never touch it. Otherwise the factory whose
 * {@link Identifier} matches {@link IdempotencyConfiguration#type()} is selected.
 */
@ApplicationScoped
public class IdempotencyStoreProducer {

  @Produces
  @RequestScoped
  public IdempotencyStore idempotencyStore(
      IdempotencyConfiguration configuration,
      RealmContext realmContext,
      @Any Instance<IdempotencyStoreFactory> factories) {
    if (!configuration.enabled()) {
      return NoOpIdempotencyStore.INSTANCE;
    }
    Instance<IdempotencyStoreFactory> selected =
        factories.select(Identifier.Literal.of(configuration.type()));
    if (!selected.isResolvable()) {
      throw new IllegalStateException(
          "No IdempotencyStoreFactory registered for polaris.idempotency.type='"
              + configuration.type()
              + "'");
    }
    return selected.get().getOrCreateIdempotencyStore(realmContext);
  }
}
