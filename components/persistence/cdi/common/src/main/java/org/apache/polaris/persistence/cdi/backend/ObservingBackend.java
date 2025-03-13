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
package org.apache.polaris.persistence.cdi.backend;

import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.api.Persistence;
import org.apache.polaris.persistence.api.PersistenceParams;
import org.apache.polaris.persistence.api.backend.Backend;
import org.apache.polaris.persistence.api.backend.PersistId;
import org.apache.polaris.realms.id.RealmId;

@ApplicationScoped
@Observed
class ObservingBackend implements Backend {
  private static final String PREFIX = "polaris.persistenceBackend";

  private final Backend backend;

  @Inject
  ObservingBackend(@NotObserved Backend backend) {
    this.backend = backend;
  }

  @Nonnull
  @Override
  public String name() {
    return backend.name();
  }

  @Nonnull
  @Override
  public Persistence newPersistence(
      @Nonnull PersistenceParams persistenceParams,
      RealmId realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator) {
    return backend.newPersistence(persistenceParams, realmId, monotonicClock, idGenerator);
  }

  @Override
  public boolean supportsRealmDeletion() {
    return backend.supportsRealmDeletion();
  }

  @Override
  public void close() throws Exception {
    backend.close();
  }

  @WithSpan
  @Counted(PREFIX + ".setupSchema")
  @Timed(value = PREFIX + ".setupSchema", histogram = true)
  @Override
  public Optional<String> setupSchema() {
    return backend.setupSchema();
  }

  @WithSpan
  @Counted(PREFIX + ".deleteRealms")
  @Timed(value = PREFIX + ".deleteRealms", histogram = true)
  @Override
  public void deleteRealms(Set<RealmId> realmIds) {
    backend.deleteRealms(realmIds);
  }

  @WithSpan
  @Counted(PREFIX + ".batchDeleteRefs")
  @Timed(value = PREFIX + ".batchDeleteRefs", histogram = true)
  @Override
  public void batchDeleteRefs(Map<RealmId, Set<String>> realmRefs) {
    backend.batchDeleteRefs(realmRefs);
  }

  @WithSpan
  @Counted(PREFIX + ".batchDeleteObjs")
  @Timed(value = PREFIX + ".batchDeleteObjs", histogram = true)
  @Override
  public void batchDeleteObjs(Map<RealmId, Set<PersistId>> realmObjs) {
    backend.batchDeleteObjs(realmObjs);
  }

  @WithSpan
  @Counted(PREFIX + ".scanBackend")
  @Timed(value = PREFIX + ".scanBackend", histogram = true)
  @Override
  public void scanBackend(
      @Nonnull ReferenceScanCallback referenceConsumer, @Nonnull ObjScanCallback objConsumer) {
    backend.scanBackend(referenceConsumer, objConsumer);
  }
}
