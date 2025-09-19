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
package org.apache.polaris.persistence.nosql.cdi.persistence;

import static com.google.common.base.Preconditions.checkState;

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.RealmPersistenceFactory;
import org.apache.polaris.persistence.nosql.api.backend.Backend;

@ApplicationScoped
class RealmPersistence implements RealmPersistenceFactory {
  private final PersistenceParams persistenceConfig;
  private final Backend backend;
  private final IdGenerator idGenerator;
  private final MonotonicClock monotonicClock;
  private final PersistenceDecorators persistenceDecorators;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  RealmPersistence(
      PersistenceParams persistenceConfig,
      Backend backend,
      IdGenerator idGenerator,
      MonotonicClock monotonicClock,
      PersistenceDecorators persistenceDecorators) {
    this.persistenceConfig = persistenceConfig;
    this.backend = backend;
    this.idGenerator = idGenerator;
    this.monotonicClock = monotonicClock;
    this.persistenceDecorators = persistenceDecorators;
  }

  @Override
  public RealmPersistenceBuilder newBuilder() {
    return new RealmPersistenceBuilder() {
      private boolean skipDecorators;
      private String realmId;
      private boolean consumed;

      @Override
      public RealmPersistenceBuilder realmId(@Nonnull String realmId) {
        checkState(this.realmId == null, "RealmPersistenceBuilder can only be used once");
        this.realmId = realmId;
        return this;
      }

      @Override
      public RealmPersistenceBuilder skipDecorators() {
        this.skipDecorators = true;
        return this;
      }

      @Override
      public Persistence build() {
        checkState(!consumed, "RealmPersistenceBuilder can only be used once");
        checkState(realmId != null, "Must call RealmPersistenceBuilder.setRealmId() before .build");
        consumed = true;

        var persistence =
            backend.newPersistence(
                x -> backend, persistenceConfig, realmId, monotonicClock, idGenerator);
        return skipDecorators ? persistence : persistenceDecorators.decorate(persistence);
      }
    };
  }
}
