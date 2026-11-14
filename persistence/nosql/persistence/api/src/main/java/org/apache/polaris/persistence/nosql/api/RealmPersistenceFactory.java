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
package org.apache.polaris.persistence.nosql.api;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.function.Function;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.jspecify.annotations.NonNull;

/**
 * Builder factory to generate "realm-scoped" {@link Persistence} instances.
 *
 * <p>{@link RealmPersistenceFactory} instance is available as an {@link ApplicationScoped} bean.
 */
public interface RealmPersistenceFactory {
  /**
   * Return a new builder for per-realm persistence.
   *
   * <p>Builders must only be used once.
   */
  RealmPersistenceBuilder newBuilder();

  interface RealmPersistenceBuilder {
    RealmPersistenceBuilder realmId(@NonNull String realmId);

    RealmPersistenceBuilder skipDecorators();

    /**
     * Wraps the injected backend for this persistence instance, preserving backend observation. The
     * wrapper is applied once during {@link #build()}, before persistence decorators. This option
     * is independent of {@link #skipDecorators()} and may only be set once.
     *
     * <p>The wrapper must not close the shared backend.
     */
    RealmPersistenceBuilder backendWrapper(@NonNull Function<Backend, Backend> backendWrapper);

    Persistence build();
  }
}
