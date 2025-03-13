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
package org.apache.polaris.persistence.inmemory;

import static org.apache.polaris.persistence.api.backend.PersistId.persistId;
import static org.apache.polaris.persistence.inmemory.ObjKey.objKey;
import static org.apache.polaris.persistence.inmemory.RefKey.refKey;

import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.api.Persistence;
import org.apache.polaris.persistence.api.PersistenceParams;
import org.apache.polaris.persistence.api.backend.Backend;
import org.apache.polaris.persistence.api.backend.PersistId;
import org.apache.polaris.persistence.api.ref.Reference;
import org.apache.polaris.realms.id.RealmId;

final class InMemoryBackend implements Backend {
  final ConcurrentMap<RefKey, Reference> refs = new ConcurrentHashMap<>();
  final ConcurrentMap<ObjKey, SerializedObj> objs = new ConcurrentHashMap<>();

  @Override
  @Nonnull
  public String name() {
    return InMemoryBackendFactory.NAME;
  }

  @Nonnull
  @Override
  public Persistence newPersistence(
      @Nonnull PersistenceParams persistenceParams,
      RealmId realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator) {
    return new InMemoryPersistence(this, persistenceParams, realmId, monotonicClock, idGenerator);
  }

  @Override
  public Optional<String> setupSchema() {
    return Optional.of("FOR LOCAL TESTING ONLY, NO INFORMATION WILL BE PERSISTED!");
  }

  @Override
  public void deleteRealms(Set<RealmId> realmIds) {
    objs.entrySet().removeIf(e -> realmIds.contains(e.getKey().realmId()));
    refs.entrySet().removeIf(e -> realmIds.contains(e.getKey().realmId()));
  }

  @Override
  public void batchDeleteRefs(Map<RealmId, Set<String>> realmRefs) {
    realmRefs.forEach(
        (realmId, refNames) -> refNames.forEach(ref -> refs.remove(refKey(realmId, ref))));
  }

  @Override
  public void batchDeleteObjs(Map<RealmId, Set<PersistId>> realmObjs) {
    realmObjs.forEach(
        ((realmId, objIds) ->
            objIds.forEach(obj -> objs.remove(objKey(realmId, obj.id(), obj.part())))));
  }

  @Override
  public boolean supportsRealmDeletion() {
    return true;
  }

  @Override
  public void scanBackend(
      @Nonnull ReferenceScanCallback referenceConsumer, @Nonnull ObjScanCallback objConsumer) {
    refs.forEach(
        (key, ref) -> referenceConsumer.call(key.realmId(), key.name(), ref.createdAtMicros()));
    objs.forEach(
        (key, serObj) ->
            objConsumer.call(
                key.realmId(),
                serObj.type(),
                persistId(key.id(), key.part()),
                serObj.createdAtMicros()));
  }

  @Override
  public void close() {}
}
