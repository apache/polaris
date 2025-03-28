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

import static org.apache.polaris.persistence.inmemory.ObjKey.objKey;
import static org.apache.polaris.persistence.inmemory.RefKey.refKey;

import com.google.common.collect.Maps;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.api.PersistenceParams;
import org.apache.polaris.persistence.api.backend.PersistId;
import org.apache.polaris.persistence.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.api.obj.ObjRef;
import org.apache.polaris.persistence.api.ref.Reference;
import org.apache.polaris.persistence.base.AbstractPersistence;
import org.apache.polaris.persistence.base.FetchedObj;
import org.apache.polaris.persistence.base.WriteObj;
import org.apache.polaris.realms.id.RealmId;

final class InMemoryPersistence extends AbstractPersistence {
  private final InMemoryBackend backend;

  /**
   * For testing purposes, add a random sleep within the given bound in milliseconds for each
   * operation. This value can be useful when debugging concurrency issues.
   */
  private static final int RANDOM_SLEEP_BOUND =
      Integer.getInteger("x-polaris.persistence.inmemory.random.sleep-bound", 0);

  InMemoryPersistence(
      InMemoryBackend backend,
      PersistenceParams params,
      RealmId realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator) {
    super(params, realmId, monotonicClock, idGenerator);
    this.backend = backend;
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
  protected boolean doCreateReference(@Nonnull Reference newRef) {
    randomDelay();
    var key = refKey(realmId(), newRef.name());
    return backend.refs.putIfAbsent(key, newRef) == null;
  }

  @Override
  protected boolean doUpdateReference(
      @Nonnull Reference updatedRef, @Nonnull Optional<ObjRef> expectedPointer) {
    randomDelay();
    var key = refKey(realmId(), updatedRef.name());
    return backend.refs.compute(
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
  @Nonnull
  protected Reference doFetchReference(@Nonnull String name) {
    randomDelay();
    var key = refKey(realmId(), name);
    var ref = backend.refs.get(key);
    if (ref == null) {
      throw new ReferenceNotFoundException(name);
    }
    return ref;
  }

  @Override
  @Nonnull
  protected Map<PersistId, FetchedObj> doFetch(@Nonnull Set<PersistId> ids) {
    randomDelay();
    var r = Maps.<PersistId, FetchedObj>newHashMapWithExpectedSize(ids.size());
    for (var id : ids) {
      var key = objKey(realmId(), id);
      var val = backend.objs.get(key);
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
  protected void doWrite(@Nonnull List<WriteObj> writes) {
    randomDelay();
    for (var write : writes) {
      var key = objKey(realmId(), write.id(), write.part());
      var val =
          new SerializedObj(
              write.type(), write.createdAtMicros(), null, write.serialized(), write.partNum());
      backend.objs.put(key, val);
    }
  }

  @Override
  protected void doDelete(@Nonnull Set<PersistId> ids) {
    randomDelay();
    for (var id : ids) {
      var key = objKey(realmId(), id.id(), id.part());
      backend.objs.remove(key);
    }
  }

  @Override
  protected boolean doConditionalInsert(
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String versionToken,
      @Nonnull byte[] serializedValue) {
    randomDelay();
    var key = objKey(realmId(), persistId.id(), 0);
    var val = new SerializedObj(objTypeId, createdAtMicros, versionToken, serializedValue, 1);
    var ex = backend.objs.putIfAbsent(key, val);
    return ex == null;
  }

  @Override
  protected boolean doConditionalUpdate(
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String updateToken,
      @Nonnull String expectedToken,
      @Nonnull byte[] serializedValue) {
    randomDelay();
    var key = objKey(realmId(), persistId);
    var val = new SerializedObj(objTypeId, createdAtMicros, updateToken, serializedValue, 1);
    return backend.objs.computeIfPresent(
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
  protected boolean doConditionalDelete(
      @Nonnull PersistId persistId, @Nonnull String expectedToken) {
    randomDelay();
    var key = objKey(realmId(), persistId);
    var r = new boolean[1];
    try {
      backend.objs.computeIfPresent(
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
