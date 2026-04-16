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

import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.cache.CacheBackend;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.jspecify.annotations.NonNull;

final class NoopCacheBackend implements CacheBackend {

  static final NoopCacheBackend INSTANCE = new NoopCacheBackend();

  private NoopCacheBackend() {}

  @Override
  public Persistence wrap(@NonNull Persistence persistence) {
    return persistence;
  }

  @Override
  public void putReferenceNegative(@NonNull String repositoryId, @NonNull String name) {}

  @Override
  public void putReference(@NonNull String repositoryId, @NonNull Reference r) {}

  @Override
  public void putReferenceLocal(@NonNull String repositoryId, @NonNull Reference r) {}

  @Override
  public void removeReference(@NonNull String repositoryId, @NonNull String name) {}

  @Override
  public Reference getReference(@NonNull String repositoryId, @NonNull String name) {
    return null;
  }

  @Override
  public void clear(@NonNull String repositoryId) {}

  @Override
  public void purge() {}

  @Override
  public long estimatedSize() {
    return 0;
  }

  @Override
  public void remove(@NonNull String repositoryId, @NonNull ObjRef id) {}

  @Override
  public void putNegative(@NonNull String repositoryId, @NonNull ObjRef id) {}

  @Override
  public void put(@NonNull String repositoryId, @NonNull Obj obj) {}

  @Override
  public void putLocal(@NonNull String repositoryId, @NonNull Obj obj) {}

  @Override
  public Obj get(@NonNull String repositoryId, @NonNull ObjRef id) {
    return null;
  }
}
