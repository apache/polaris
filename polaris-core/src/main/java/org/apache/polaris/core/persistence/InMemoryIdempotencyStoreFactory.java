/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
package org.apache.polaris.core.persistence;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.polaris.core.context.RealmContext;

/**
 * {@link IdempotencyStoreFactory} that vends a per-realm {@link InMemoryIdempotencyStore}.
 *
 * <p>This is the default factory used by the in-memory Polaris deployment and by unit tests. Each
 * realm gets its own {@link InMemoryIdempotencyStore} instance, cached for the lifetime of the
 * factory.
 */
public class InMemoryIdempotencyStoreFactory implements IdempotencyStoreFactory {

  private final ConcurrentMap<String, IdempotencyStore> perRealm = new ConcurrentHashMap<>();

  @Override
  public IdempotencyStore getOrCreateIdempotencyStore(RealmContext realmContext) {
    return perRealm.computeIfAbsent(
        realmContext.getRealmIdentifier(), InMemoryIdempotencyStore::new);
  }
}
