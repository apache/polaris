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
package org.apache.polaris.persistence.nosql.impl.cache;

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.cache.CacheBackend;
import org.apache.polaris.persistence.nosql.api.cache.DistributedCacheInvalidation;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.jspecify.annotations.NonNull;

final class DistributedInvalidationsCacheBackend implements CacheBackend {
  private final CacheBackend local;
  private final DistributedCacheInvalidation.Sender sender;

  DistributedInvalidationsCacheBackend(
      CacheBackend localBackend, DistributedCacheInvalidation.Sender invalidationSender) {
    this.local = localBackend;
    this.sender = invalidationSender;
  }

  @Override
  public Persistence wrap(@NonNull Persistence persist) {
    return new CachingPersistenceImpl(persist, this);
  }

  @Override
  public Obj get(@NonNull String realmId, @NonNull ObjRef id) {
    return local.get(realmId, id);
  }

  @Override
  public void put(@NonNull String realmId, @NonNull Obj obj) {
    // Note: .put() vs .putLocal() doesn't matter here, because 'local' is the local cache.
    local.putLocal(realmId, obj);
    sender.evictObj(realmId, objRef(obj));
  }

  @Override
  public void putLocal(@NonNull String realmId, @NonNull Obj obj) {
    local.putLocal(realmId, obj);
  }

  @Override
  public void putNegative(@NonNull String realmId, @NonNull ObjRef id) {
    local.putNegative(realmId, id);
  }

  @Override
  public void remove(@NonNull String realmId, @NonNull ObjRef id) {
    local.remove(realmId, id);
    sender.evictObj(realmId, id);
  }

  @Override
  public void clear(@NonNull String realmId) {
    local.clear(realmId);
  }

  @Override
  public void purge() {
    local.purge();
  }

  @Override
  public long estimatedSize() {
    return local.estimatedSize();
  }

  @Override
  public Reference getReference(@NonNull String realmId, @NonNull String name) {
    return local.getReference(realmId, name);
  }

  @Override
  public void removeReference(@NonNull String realmId, @NonNull String name) {
    local.removeReference(realmId, name);
    sender.evictReference(realmId, name);
  }

  @Override
  public void putReferenceLocal(@NonNull String realmId, @NonNull Reference reference) {
    local.putReferenceLocal(realmId, reference);
  }

  @Override
  public void putReference(@NonNull String realmId, @NonNull Reference reference) {
    // Note: .putReference() vs .putReferenceLocal() doesn't matter here, because 'local' is the
    // local cache.
    local.putReferenceLocal(realmId, reference);
    sender.evictReference(realmId, reference.name());
  }

  @Override
  public void putReferenceNegative(@NonNull String realmId, @NonNull String name) {
    local.putReferenceNegative(realmId, name);
  }
}
