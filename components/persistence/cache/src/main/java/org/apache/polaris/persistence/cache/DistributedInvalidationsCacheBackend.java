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
package org.apache.polaris.persistence.cache;

import static org.apache.polaris.persistence.api.obj.ObjRef.objRef;

import jakarta.annotation.Nonnull;
import org.apache.polaris.persistence.api.Persistence;
import org.apache.polaris.persistence.api.obj.Obj;
import org.apache.polaris.persistence.api.obj.ObjRef;
import org.apache.polaris.persistence.api.ref.Reference;
import org.apache.polaris.realms.id.RealmId;

final class DistributedInvalidationsCacheBackend implements CacheBackend {
  private final CacheBackend local;
  private final DistributedCacheInvalidation sender;

  DistributedInvalidationsCacheBackend(
      DistributedCacheInvalidations distributedCacheInvalidations) {
    this.local = distributedCacheInvalidations.localBackend();
    this.sender = distributedCacheInvalidations.invalidationSender();
    distributedCacheInvalidations
        .invalidationListenerReceiver()
        .applyDistributedCacheInvalidation(
            new DistributedCacheInvalidation() {
              @Override
              public void evictObj(@Nonnull RealmId realmId, @Nonnull ObjRef objRef) {
                local.remove(realmId, objRef);
              }

              @Override
              public void evictReference(@Nonnull RealmId realmId, @Nonnull String refName) {
                local.removeReference(realmId, refName);
              }
            });
  }

  @Override
  public Persistence wrap(@Nonnull Persistence persist) {
    return new CachingPersistenceImpl(persist, this);
  }

  @Override
  public Obj get(@Nonnull RealmId realmId, @Nonnull ObjRef id) {
    return local.get(realmId, id);
  }

  @Override
  public void put(@Nonnull RealmId realmId, @Nonnull Obj obj) {
    // Note: .put() vs .putLocal() doesn't matter here, because 'local' is the local cache.
    local.putLocal(realmId, obj);
    sender.evictObj(realmId, objRef(obj));
  }

  @Override
  public void putLocal(@Nonnull RealmId realmId, @Nonnull Obj obj) {
    local.putLocal(realmId, obj);
  }

  @Override
  public void putNegative(@Nonnull RealmId realmId, @Nonnull ObjRef id) {
    local.putNegative(realmId, id);
  }

  @Override
  public void remove(@Nonnull RealmId realmId, @Nonnull ObjRef id) {
    local.remove(realmId, id);
    sender.evictObj(realmId, id);
  }

  @Override
  public void clear(@Nonnull RealmId realmId) {
    local.clear(realmId);
  }

  @Override
  public Reference getReference(@Nonnull RealmId realmId, @Nonnull String name) {
    return local.getReference(realmId, name);
  }

  @Override
  public void removeReference(@Nonnull RealmId realmId, @Nonnull String name) {
    local.removeReference(realmId, name);
    sender.evictReference(realmId, name);
  }

  @Override
  public void putReferenceLocal(@Nonnull RealmId realmId, @Nonnull Reference reference) {
    local.putReferenceLocal(realmId, reference);
  }

  @Override
  public void putReference(@Nonnull RealmId realmId, @Nonnull Reference reference) {
    // Note: .putReference() vs .putReferenceLocal() doesn't matter here, because 'local' is the
    // local cache.
    local.putReferenceLocal(realmId, reference);
    sender.evictReference(realmId, reference.name());
  }

  @Override
  public void putReferenceNegative(@Nonnull RealmId realmId, @Nonnull String name) {
    local.putReferenceNegative(realmId, name);
  }
}
