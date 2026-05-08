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
package org.apache.polaris.persistence.nosql.inmemory;

import static org.apache.polaris.persistence.nosql.api.backend.PersistId.persistId;
import static org.apache.polaris.persistence.nosql.inmemory.ObjKey.objKey;
import static org.apache.polaris.persistence.nosql.inmemory.RefKey.refKey;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.FetchedObj;
import org.apache.polaris.persistence.nosql.api.backend.PersistId;
import org.apache.polaris.persistence.nosql.api.backend.WriteObj;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.impl.PersistenceImplementation;
import org.jspecify.annotations.NonNull;

final class InMemoryBackend implements Backend {

  /**
   * For testing purposes, add a random sleep within the given bound in milliseconds for each
   * operation. This value can be useful when debugging concurrency issues.
   */
  private static final int RANDOM_SLEEP_BOUND =
      Integer.getInteger("x-polaris.persistence.inmemory.random.sleep-bound", 0);

  final ConcurrentMap<RefKey, Reference> refs = new ConcurrentHashMap<>();
  final ConcurrentMap<ObjKey, SerializedObj> objs = new ConcurrentHashMap<>();

  @Override
  @NonNull
  public String type() {
    return InMemoryBackendFactory.NAME;
  }

  @Override
  public boolean supportsRealmDeletion() {
    return true;
  }

  @Override
  public void close() {}

  @NonNull
  @Override
  public Persistence newPersistence(
      Function<Backend, Backend> backendWrapper,
      @NonNull PersistenceParams persistenceParams,
      String realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator) {
    return new PersistenceImplementation(
        backendWrapper.apply(this), persistenceParams, realmId, monotonicClock, idGenerator);
  }

  @Override
  public Optional<String> setupSchema() {
    return Optional.of("FOR LOCAL TESTING ONLY, NO INFORMATION WILL BE PERSISTED!");
  }

  @Override
  public void deleteRealms(Set<String> realmIds) {
    objs.entrySet().removeIf(e -> realmIds.contains(e.getKey().realmId()));
    refs.entrySet().removeIf(e -> realmIds.contains(e.getKey().realmId()));
  }

  @Override
  public void batchDeleteRefs(Map<String, Set<String>> realmRefs) {
    realmRefs.forEach(
        (realmId, refNames) -> refNames.forEach(ref -> refs.remove(refKey(realmId, ref))));
  }

  @Override
  public void batchDeleteObjs(Map<String, Set<PersistId>> realmObjs) {
    realmObjs.forEach(
        ((realmId, objIds) ->
            objIds.forEach(obj -> objs.remove(objKey(realmId, obj.id(), obj.part())))));
  }

  @Override
  public void scanBackend(
      @NonNull ReferenceScanCallback referenceConsumer, @NonNull ObjScanCallback objConsumer) {
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

  // For testing purposes only
  private void randomDelay() {
    if (RANDOM_SLEEP_BOUND == 0) {
      return;
    }

    var i = ThreadLocalRandom.current().nextInt(RANDOM_SLEEP_BOUND);
    if (i > 0) {
      try {
        Thread.sleep(i);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean createReference(@NonNull String realmId, @NonNull Reference newRef) {
    randomDelay();
    var key = refKey(realmId, newRef.name());
    return refs.putIfAbsent(key, newRef) == null;
  }

  @Override
  public void createReferences(@NonNull String realmId, @NonNull List<Reference> newRefs) {
    newRefs.forEach(ref -> createReference(realmId, ref));
  }

  @Override
  public boolean updateReference(
      @NonNull String realmId,
      @NonNull Reference updatedRef,
      @NonNull Optional<ObjRef> expectedPointer) {
    randomDelay();
    var key = refKey(realmId, updatedRef.name());
    return refs.compute(
            key,
            (k, ref) -> {
              if (ref == null) {
                throw new ReferenceNotFoundException(updatedRef.name());
              }
              return ref.pointer().equals(expectedPointer) ? updatedRef : ref;
            })
        == updatedRef;
  }

  @Override
  @NonNull
  public Reference fetchReference(@NonNull String realmId, @NonNull String name) {
    randomDelay();
    var key = refKey(realmId, name);
    var ref = refs.get(key);
    if (ref == null) {
      throw new ReferenceNotFoundException(name);
    }
    return ref;
  }

  @Override
  @NonNull
  public Map<PersistId, FetchedObj> fetch(@NonNull String realmId, @NonNull Set<PersistId> ids) {
    randomDelay();
    var r = Maps.<PersistId, FetchedObj>newHashMapWithExpectedSize(ids.size());
    for (var id : ids) {
      var key = objKey(realmId, id);
      var val = objs.get(key);
      if (val != null) {
        r.put(
            id,
            new FetchedObj(
                val.type(),
                val.createdAtMicros(),
                val.versionToken(),
                val.serializedValue(),
                val.partNum()));
      }
    }
    return r;
  }

  @Override
  public void write(@NonNull String realmId, @NonNull List<WriteObj> writes) {
    randomDelay();
    for (var write : writes) {
      var key = objKey(realmId, write.id(), write.part());
      var val =
          new SerializedObj(
              write.type(), write.createdAtMicros(), null, write.serialized(), write.partNum());
      objs.put(key, val);
    }
  }

  @Override
  public void delete(@NonNull String realmId, @NonNull Set<PersistId> ids) {
    randomDelay();
    for (var id : ids) {
      var key = objKey(realmId, id.id(), id.part());
      objs.remove(key);
    }
  }

  @Override
  public boolean conditionalInsert(
      @NonNull String realmId,
      String objTypeId,
      @NonNull PersistId persistId,
      long createdAtMicros,
      @NonNull String versionToken,
      @NonNull byte[] serializedValue) {
    randomDelay();
    var key = objKey(realmId, persistId.id(), 0);
    var val = new SerializedObj(objTypeId, createdAtMicros, versionToken, serializedValue, 1);
    var ex = objs.putIfAbsent(key, val);
    return ex == null;
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
    randomDelay();
    var key = objKey(realmId, persistId);
    var val = new SerializedObj(objTypeId, createdAtMicros, updateToken, serializedValue, 1);
    return objs.computeIfPresent(
            key,
            (k, ex) -> {
              var exToken = ex.versionToken();
              if (!expectedToken.equals(exToken)) {
                return ex;
              }
              return val;
            })
        == val;
  }

  @Override
  public boolean conditionalDelete(
      @NonNull String realmId, @NonNull PersistId persistId, @NonNull String expectedToken) {
    randomDelay();
    var key = objKey(realmId, persistId);
    var r = new boolean[1];
    try {
      objs.computeIfPresent(
          key,
          (k, ex) -> {
            var exToken = ex.versionToken();
            if (exToken == null || !exToken.equals(expectedToken)) {
              throw new VersionMismatchInternalException();
            }
            r[0] = true;
            return null;
          });
    } catch (VersionMismatchInternalException e) {
      //
    }
    return r[0];
  }

  static final class VersionMismatchInternalException extends RuntimeException {}
}
