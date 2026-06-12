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
import org.apache.polaris.core.persistence.IdempotencyStoreFactory;

/**
 * Produces the active {@link IdempotencyStoreFactory} by selecting the registered backend whose
 * {@link Identifier} matches {@link IdempotencyConfiguration#type()}.
 */
@ApplicationScoped
public class IdempotencyStoreFactoryProducer {

  @Produces
  @ApplicationScoped
  public IdempotencyStoreFactory idempotencyStoreFactory(
      IdempotencyConfiguration configuration, @Any Instance<IdempotencyStoreFactory> factories) {
    if (!configuration.enabled()) {
      // When idempotency is disabled the handlers short-circuit on isEnabled() and never request a
      // store, so backend selection must not run: otherwise a misspelled/unavailable
      // polaris.idempotency.type would fail application startup even though the feature is off.
      return realmContext -> {
        throw new IllegalStateException(
            "Idempotency is disabled; no idempotency store should be requested");
      };
    }
    Instance<IdempotencyStoreFactory> selected =
        factories.select(Identifier.Literal.of(configuration.type()));
    if (!selected.isResolvable()) {
      throw new IllegalStateException(
          "No IdempotencyStoreFactory registered for polaris.idempotency.type='"
              + configuration.type()
              + "'");
    }
    return selected.get();
  }
}
