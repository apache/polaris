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

import jakarta.annotation.Nonnull;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.cache.CacheBackend;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;

final class NoopCacheBackend implements CacheBackend {

  static final NoopCacheBackend INSTANCE = new NoopCacheBackend();

  private NoopCacheBackend() {}

  @Override
  public Persistence wrap(@Nonnull Persistence persistence) {
    return persistence;
  }

  @Override
  public void putReferenceNegative(@Nonnull String repositoryId, @Nonnull String name) {}

  @Override
  public void putReference(@Nonnull String repositoryId, @Nonnull Reference r) {}

  @Override
  public void putReferenceLocal(@Nonnull String repositoryId, @Nonnull Reference r) {}

  @Override
  public void removeReference(@Nonnull String repositoryId, @Nonnull String name) {}

  @Override
  public Reference getReference(@Nonnull String repositoryId, @Nonnull String name) {
    return null;
  }

  @Override
  public void clear(@Nonnull String repositoryId) {}

  @Override
  public void purge() {}

  @Override
  public long estimatedSize() {
    return 0;
  }

  @Override
  public void remove(@Nonnull String repositoryId, @Nonnull ObjRef id) {}

  @Override
  public void putNegative(@Nonnull String repositoryId, @Nonnull ObjRef id) {}

  @Override
  public void put(@Nonnull String repositoryId, @Nonnull Obj obj) {}

  @Override
  public void putLocal(@Nonnull String repositoryId, @Nonnull Obj obj) {}

  @Override
  public Obj get(@Nonnull String repositoryId, @Nonnull ObjRef id) {
    return null;
  }
}
