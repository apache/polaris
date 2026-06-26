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
package org.apache.polaris.persistence.nosql.api.cache;

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.jspecify.annotations.NonNull;

/**
 * Provides the cache primitives for a caching {@link Persistence} facade, suitable for multiple
 * repositories. It is advisable to have one {@link CacheBackend} per {@link Backend}.
 */
public interface CacheBackend {
  /**
   * Special sentinel reference instance to indicate that a reference object has been marked as "not
   * found". This object is only for cache-internal purposes.
   */
  Reference NON_EXISTENT_REFERENCE_SENTINEL =
      Reference.builder()
          .name("NON_EXISTENT")
          .pointer(objRef("CACHE_SENTINEL", 0L))
          .createdAtMicros(0L)
          .previousPointers()
          .build();

  /**
   * Special sentinel object instance to indicate that an object has been marked as "not found".
   * This object is only for cache-internal purposes.
   */
  Obj NOT_FOUND_OBJ_SENTINEL =
      new Obj() {
        @Override
        public ObjType type() {
          throw new UnsupportedOperationException();
        }

        @Override
        public long id() {
          throw new UnsupportedOperationException();
        }

        @Override
        public int numParts() {
          throw new UnsupportedOperationException();
        }

        @Override
        public String versionToken() {
          throw new UnsupportedOperationException();
        }

        @Override
        public long createdAtMicros() {
          throw new UnsupportedOperationException();
        }

        @Override
        @NonNull
        public Obj withCreatedAtMicros(long createdAtMicros) {
          throw new UnsupportedOperationException();
        }

        @Override
        @NonNull
        public Obj withNumParts(int numParts) {
          throw new UnsupportedOperationException();
        }
      };

  /** Returns the {@link Obj} for the given {@link ObjRef id}. */
  Obj get(@NonNull String realmId, @NonNull ObjRef id);

  /**
   * Adds the given object to the local cache and sends a cache-invalidation message to Polaris
   * peers.
   */
  void put(@NonNull String realmId, @NonNull Obj obj);

  /** Adds the given object only to the local cache, does not send a cache-invalidation message. */
  void putLocal(@NonNull String realmId, @NonNull Obj obj);

  /** Record the "not found" sentinel for the given {@link ObjRef id} and {@link ObjType type}. */
  void putNegative(@NonNull String realmId, @NonNull ObjRef id);

  void remove(@NonNull String realmId, @NonNull ObjRef id);

  void clear(@NonNull String realmId);

  void purge();

  long estimatedSize();

  Persistence wrap(@NonNull Persistence persist);

  Reference getReference(@NonNull String realmId, @NonNull String name);

  void removeReference(@NonNull String realmId, @NonNull String name);

  /**
   * Adds the given reference to the local cache and sends a cache-invalidation message to Polaris
   * peers.
   */
  void putReference(@NonNull String realmId, @NonNull Reference reference);

  /**
   * Adds the given reference only to the local cache, does not send a cache-invalidation message.
   */
  void putReferenceLocal(@NonNull String realmId, @NonNull Reference reference);

  void putReferenceNegative(@NonNull String realmId, @NonNull String name);
}
