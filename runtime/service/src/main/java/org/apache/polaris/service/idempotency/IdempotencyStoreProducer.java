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
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import org.apache.polaris.core.persistence.IdempotencyStore;

/**
 * Produces the unqualified {@link IdempotencyStore} that {@link IdempotencyHandlerSupport} injects
 * — selecting the backend whose {@link Identifier} matches {@link IdempotencyConfiguration#type()}.
 *
 * <p>The selection runs once at startup: this is {@link ApplicationScoped}, and {@code
 * selected.get()} returns the client proxy of the chosen {@code @RequestScoped} backend store,
 * which delegates to the current request's realm-bound instance on every call (see {@code
 * InMemoryIdempotencyStoreProducer} and {@code RelationalJdbcIdempotencyStoreProducer}). This
 * mirrors how {@code AdminToolProducers} resolves the {@code MetaStoreManagerFactory} from
 * configuration.
 *
 * <p>When idempotency is disabled it yields {@link DisabledIdempotencyStore}; handlers
 * short-circuit on {@link IdempotencyHandlerSupport#isEnabled()} and never touch it.
 */
@ApplicationScoped
public class IdempotencyStoreProducer {

  @Produces
  @ApplicationScoped
  public IdempotencyStore idempotencyStore(
      IdempotencyConfiguration configuration, @Any Instance<IdempotencyStore> stores) {
    if (!configuration.enabled()) {
      return DisabledIdempotencyStore.INSTANCE;
    }
    Instance<IdempotencyStore> selected =
        stores.select(Identifier.Literal.of(configuration.type()));
    if (!selected.isResolvable()) {
      throw new IllegalStateException(
          "No IdempotencyStore registered for polaris.idempotency.type='"
              + configuration.type()
              + "'");
    }
    return selected.get();
  }
}
