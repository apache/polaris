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
import jakarta.inject.Inject;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.core.persistence.InMemoryIdempotencyStore;
import org.apache.polaris.service.persistence.PersistenceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produces the application-scoped default {@link IdempotencyStore}.
 *
 * <p>The persistence-specific stores are produced in their own modules with the {@link Identifier}
 * qualifier matching {@code polaris.persistence.type} (for example, {@code "relational-jdbc"}).
 * This producer selects the right one at runtime, falling back to {@link InMemoryIdempotencyStore}
 * when no matching backend is wired.
 */
@ApplicationScoped
public class IdempotencyStoreProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdempotencyStoreProducer.class);

  @Inject PersistenceConfiguration persistenceConfiguration;
  @Inject @Any Instance<IdempotencyStore> backends;

  @Produces
  @ApplicationScoped
  IdempotencyStore idempotencyStore() {
    String type = persistenceConfiguration.type();
    Instance<IdempotencyStore> typed = backends.select(Identifier.Literal.of(type));
    if (!typed.isUnsatisfied()) {
      LOGGER.info("Selected IdempotencyStore for persistence type '{}'", type);
      return typed.get();
    }
    LOGGER.info(
        "No IdempotencyStore wired for persistence type '{}'; using InMemoryIdempotencyStore",
        type);
    return new InMemoryIdempotencyStore();
  }
}
