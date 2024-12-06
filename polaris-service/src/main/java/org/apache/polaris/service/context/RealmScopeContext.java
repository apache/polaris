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
package org.apache.polaris.service.context;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.context.RealmScoped;
import org.glassfish.hk2.api.ActiveDescriptor;
import org.glassfish.hk2.api.Context;
import org.glassfish.hk2.api.IterableProvider;
import org.glassfish.hk2.api.ServiceHandle;
import org.glassfish.hk2.api.ServiceLocator;

@Singleton
public class RealmScopeContext implements Context<RealmScoped> {
  private final Map<String, Map<ActiveDescriptor<?>, Object>> contexts = new ConcurrentHashMap<>();

  @Inject private ServiceLocator locator;
  @Inject private IterableProvider<RealmContext> realmContextProvider;

  @Override
  public Class<? extends Annotation> getScope() {
    return RealmScoped.class;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <U> U findOrCreate(ActiveDescriptor<U> activeDescriptor, ServiceHandle<?> root) {
    RealmContext realmContext = realmContextProvider.iterator().next();
    Map<ActiveDescriptor<?>, Object> contextMap =
        contexts.computeIfAbsent(realmContext.getRealmIdentifier(), k -> new ConcurrentHashMap<>());
    return (U) contextMap.computeIfAbsent(activeDescriptor, k -> activeDescriptor.create(root));
  }

  @Override
  public boolean containsKey(ActiveDescriptor<?> descriptor) {
    RealmContext realmContext = realmContextProvider.iterator().next();
    Map<ActiveDescriptor<?>, Object> contextMap =
        contexts.computeIfAbsent(realmContext.getRealmIdentifier(), k -> new HashMap<>());
    return contextMap.containsKey(descriptor);
  }

  @Override
  public void destroyOne(ActiveDescriptor<?> descriptor) {
    RealmContext realmContext = realmContextProvider.iterator().next();
    Map<ActiveDescriptor<?>, Object> contextMap =
        contexts.computeIfAbsent(realmContext.getRealmIdentifier(), k -> new HashMap<>());
    contextMap.remove(descriptor);
  }

  @Override
  public boolean supportsNullCreation() {
    return false;
  }

  @Override
  public boolean isActive() {
    Optional<Context> first =
        locator.getAllServices(Context.class).stream()
            .filter(
                context ->
                    context
                        .getScope()
                        .equals(
                            realmContextProvider
                                .getHandle()
                                .getActiveDescriptor()
                                .getScopeAnnotation()))
            .findFirst();
    return first.map(Context::isActive).orElse(false);
  }

  @Override
  public void shutdown() {
    contexts.clear();
  }
}
