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
package org.apache.polaris.service.idempotency;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.core.persistence.InMemoryIdempotencyStore;

/**
 * Produces the {@code "in-memory"} {@link IdempotencyStore}, the default for dev/test and the
 * in-memory Polaris deployment.
 *
 * <p>Each realm gets its own {@link InMemoryIdempotencyStore}, cached for the lifetime of this
 * {@link ApplicationScoped} bean so that state survives across requests. The produced store is
 * request-scoped and bound to the current request's realm.
 */
@ApplicationScoped
public class InMemoryIdempotencyStoreProducer {

  private final ConcurrentMap<String, IdempotencyStore> perRealm = new ConcurrentHashMap<>();

  @Produces
  @RequestScoped
  @Identifier("in-memory")
  public IdempotencyStore idempotencyStore(RealmContext realmContext) {
    return perRealm.computeIfAbsent(
        realmContext.getRealmIdentifier(), InMemoryIdempotencyStore::new);
  }
}
