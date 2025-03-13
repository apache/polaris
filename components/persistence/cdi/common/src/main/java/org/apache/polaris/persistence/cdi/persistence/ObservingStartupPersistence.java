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
package org.apache.polaris.persistence.cdi.persistence;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.api.Persistence;
import org.apache.polaris.persistence.api.PersistenceParams;
import org.apache.polaris.persistence.api.StartupPersistence;
import org.apache.polaris.persistence.api.backend.Backend;
import org.apache.polaris.realms.id.RealmId;

@ApplicationScoped
@StartupPersistence
public class ObservingStartupPersistence extends ObservingPersistence {
  private final Persistence delegate;

  @Inject
  ObservingStartupPersistence(
      Backend backend, MonotonicClock monotonicClock, PersistenceDecorators persistenceDecorators) {
    var persistence =
        backend.newPersistence(
            PersistenceParams.BuildablePersistenceParams.builder().build(),
            RealmId.SYSTEM,
            monotonicClock,
            IdGenerator.NONE);
    this.delegate = persistenceDecorators.decorate(persistence);
  }

  @Override
  Persistence delegate() {
    return delegate;
  }
}
