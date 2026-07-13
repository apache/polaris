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

package org.apache.polaris.service.config;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.service.context.catalog.RealmContextHolder;

/** Utility class for running per-realm bootstrap tasks each in a fresh Request Context. */
@ApplicationScoped
class Bootstrapper {
  private final ExecutorService executor;
  private final RealmContextHolder realmContextHolder;
  private final MetaStoreManagerFactory factory;

  @Inject
  Bootstrapper(
      // Note: this executor is expected to NOT propagate CDI contexts to tasks.
      @Identifier("task-executor") ExecutorService executor,
      RealmContextHolder realmContextHolder,
      MetaStoreManagerFactory factory) {
    this.executor = executor;
    this.realmContextHolder = realmContextHolder;
    this.factory = factory;
  }

  Map<String, PrincipalSecretsResult> bootstrapRealms(
      Iterable<String> realmIds, RootCredentialsSet rootCredentialsSet) {
    HashMap<String, PrincipalSecretsResult> result = new HashMap<>();
    for (String realmId : realmIds) {
      Task t = new Task(realmContextHolder, realmId, rootCredentialsSet, factory);
      try {
        // Submit an async task per realm to ensure it runs in a fresh RequestContext.
        // Note: simultaneous bootstrap of multiple realms is an edge case - no need
        // to optimize for fast concurrent completion.
        result.putAll(executor.submit(t).get(2, TimeUnit.MINUTES));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return result;
  }

  private record Task(
      RealmContextHolder realmContextHolder,
      String realmId,
      RootCredentialsSet rootCredentialsSet,
      MetaStoreManagerFactory factory)
      implements Callable<Map<String, PrincipalSecretsResult>> {

    @Override
    @ActivateRequestContext
    public Map<String, PrincipalSecretsResult> call() {
      // Note: each call to this method runs in a new CDI request context.
      // Make the realm ID effective in the current request context.
      realmContextHolder.set(() -> realmId);
      return factory.bootstrapRealms(Collections.singleton(realmId), rootCredentialsSet);
    }
  }
}
