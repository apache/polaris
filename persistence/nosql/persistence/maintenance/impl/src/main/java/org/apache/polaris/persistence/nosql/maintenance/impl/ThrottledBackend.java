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
package org.apache.polaris.persistence.nosql.maintenance.impl;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.FetchedObj;
import org.apache.polaris.persistence.nosql.api.backend.PersistId;
import org.apache.polaris.persistence.nosql.api.backend.WriteObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.jspecify.annotations.NonNull;

/** Paces maintenance reads, counting requested references and object parts. */
final class ThrottledBackend implements Backend {
  private final Backend backend;

  private final RateLimit objectReadLimit;
  private final RateLimit referenceReadLimit;

  ThrottledBackend(Backend backend, RateLimit objectReadLimit, RateLimit referenceReadLimit) {
    this.backend = backend;
    this.objectReadLimit = objectReadLimit;
    this.referenceReadLimit = referenceReadLimit;
  }

  @NonNull
  @Override
  public String type() {
    return backend.type();
  }

  @NonNull
  @Override
  public Persistence newPersistence(
      Function<Backend, Backend> backendWrapper,
      @NonNull PersistenceParams persistenceParams,
      String realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator) {
    return backend.newPersistence(
        backendWrapper, persistenceParams, realmId, monotonicClock, idGenerator);
  }

  @Override
  public boolean supportsRealmDeletion() {
    return backend.supportsRealmDeletion();
  }

  @Override
  public void close() {
    // The delegate is shared and owned by CDI, not this maintenance-scoped wrapper.
  }

  @Override
  public Optional<String> setupSchema() {
    return backend.setupSchema();
  }

  @Override
  public void deleteRealms(Set<String> realmIds) {
    backend.deleteRealms(realmIds);
  }

  @Override
  public void batchDeleteRefs(Map<String, Set<String>> realmRefs) {
    backend.batchDeleteRefs(realmRefs);
  }

  @Override
  public void batchDeleteObjs(Map<String, Set<PersistId>> realmObjs) {
    backend.batchDeleteObjs(realmObjs);
  }

  @Override
  public void scanBackend(
      @NonNull ReferenceScanCallback referenceConsumer, @NonNull ObjScanCallback objConsumer) {
    backend.scanBackend(referenceConsumer, objConsumer);
  }

  @Override
  public boolean createReference(@NonNull String realmId, @NonNull Reference newRef) {
    return backend.createReference(realmId, newRef);
  }

  @Override
  public void createReferences(@NonNull String realmId, @NonNull List<Reference> newRefs) {
    backend.createReferences(realmId, newRefs);
  }

  @Override
  public boolean updateReference(
      @NonNull String realmId,
      @NonNull Reference updatedRef,
      @NonNull Optional<ObjRef> expectedPointer) {
    return backend.updateReference(realmId, updatedRef, expectedPointer);
  }

  @NonNull
  @Override
  public Reference fetchReference(@NonNull String realmId, @NonNull String name) {
    referenceReadLimit.acquire();
    return backend.fetchReference(realmId, name);
  }

  @NonNull
  @Override
  public Map<PersistId, FetchedObj> fetch(@NonNull String realmId, @NonNull Set<PersistId> ids) {
    if (!ids.isEmpty()) {
      objectReadLimit.acquire(ids.size());
    }
    return backend.fetch(realmId, ids);
  }

  @Override
  public void write(@NonNull String realmId, @NonNull List<WriteObj> writes) {
    backend.write(realmId, writes);
  }

  @Override
  public void delete(@NonNull String realmId, @NonNull Set<PersistId> ids) {
    backend.delete(realmId, ids);
  }

  @Override
  public boolean conditionalInsert(
      @NonNull String realmId,
      String objTypeId,
      @NonNull PersistId persistId,
      long createdAtMicros,
      @NonNull String versionToken,
      @NonNull byte[] serializedValue) {
    return backend.conditionalInsert(
        realmId, objTypeId, persistId, createdAtMicros, versionToken, serializedValue);
  }

  @Override
  public boolean conditionalUpdate(
      @NonNull String realmId,
      String objTypeId,
      @NonNull PersistId persistId,
      long createdAtMicros,
      @NonNull String updateToken,
      @NonNull String expectedToken,
      @NonNull byte[] serializedValue) {
    return backend.conditionalUpdate(
        realmId,
        objTypeId,
        persistId,
        createdAtMicros,
        updateToken,
        expectedToken,
        serializedValue);
  }

  @Override
  public boolean conditionalDelete(
      @NonNull String realmId, @NonNull PersistId persistId, @NonNull String expectedToken) {
    return backend.conditionalDelete(realmId, persistId, expectedToken);
  }
}
