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

import static org.apache.polaris.persistence.nosql.api.Realms.SYSTEM_REALM_ID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.StartupPersistence;
import org.apache.polaris.persistence.nosql.api.SystemPersistence;
import org.apache.polaris.persistence.nosql.api.backend.Backend;

@ApplicationScoped
class PersistenceProducers {

  private final Backend backend;
  private final IdGenerator idGenerator;
  private final MonotonicClock monotonicClock;
  private final PersistenceDecorators persistenceDecorators;
  private final PersistenceParams persistenceParams;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  PersistenceProducers(
      Backend backend,
      IdGenerator idGenerator,
      MonotonicClock monotonicClock,
      PersistenceDecorators persistenceDecorators,
      PersistenceParams persistenceParams) {
    this.backend = backend;
    this.idGenerator = idGenerator;
    this.monotonicClock = monotonicClock;
    this.persistenceDecorators = persistenceDecorators;
    this.persistenceParams = persistenceParams;
  }

  @ApplicationScoped
  @Produces
  @StartupPersistence
  Persistence startupPersistence() {
    var persistence =
        backend.newPersistence(
            x -> backend,
            PersistenceParams.BuildablePersistenceParams.builder().build(),
            SYSTEM_REALM_ID,
            monotonicClock,
            IdGenerator.NONE);
    return persistenceDecorators.decorate(persistence);
  }

  @ApplicationScoped
  @Produces
  @SystemPersistence
  Persistence systemPersistence() {
    var persistence =
        backend.newPersistence(
            x -> backend, persistenceParams, SYSTEM_REALM_ID, monotonicClock, idGenerator);
    return persistenceDecorators.decorate(persistence);
  }
}
