/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.polaris.core.persistence;

import com.google.common.annotations.Beta;
import org.apache.polaris.core.context.RealmContext;

/**
 * Factory for per-realm {@link IdempotencyStore} instances.
 *
 * <p>The realm is taken as a method argument rather than injected, mirroring {@code
 * JdbcMetaStoreManagerFactory}. This keeps backend beans free of any {@link RealmContext}
 * dependency, so they remain deployable in contexts that have no request scope (e.g. the Admin
 * Tool). Resolving the store for the current request's realm is the job of the request-scoped
 * producer in the service runtime.
 *
 * <p>Backends register implementations with {@link io.smallrye.common.annotation.Identifier}
 * matching the {@code polaris.idempotency.type} configuration value. Returned instances must be
 * safe to share across threads.
 */
@Beta
public interface IdempotencyStoreFactory {

  /** Returns the {@link IdempotencyStore} to use for the given realm. */
  IdempotencyStore getOrCreateIdempotencyStore(RealmContext realmContext);
}
