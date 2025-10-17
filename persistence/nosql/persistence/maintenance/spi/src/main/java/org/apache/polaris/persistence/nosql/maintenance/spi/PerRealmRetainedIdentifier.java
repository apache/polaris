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
package org.apache.polaris.persistence.nosql.maintenance.spi;

import jakarta.annotation.Nonnull;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.ref.Reference;

/**
 * Implementations of this interface are called by the maintenance service for every realm to
 * retain.
 *
 * <p>Implementation must be annotated as {@link
 * jakarta.enterprise.context.ApplicationScoped @ApplicationScoped} for CDI-usage.
 */
public interface PerRealmRetainedIdentifier {
  /** Human-readable name. */
  String name();

  /**
   * Called to identify "live" references and objects for a realm.
   *
   * <p>The given {@linkplain RetainedCollector collector} must be invoked for every {@linkplain
   * Reference reference} and {@linkplain Obj object} to retain. The maintenance service is allowed
   * to purge references and objects that were not passed to the {@linkplain RetainedCollector
   * collector's} {@code retain*()} functions.
   *
   * <p>Any exception thrown from this function aborts the whole maintenance run. Exceptions thrown
   * from functionality called by the implementation must be properly handled.
   *
   * <p>The purpose of the {@code boolean} return value is meant as a safety net in case to not
   * accidentally purge a realm.
   *
   * @param collector consumer of "live" references and objects
   * @return {@code true} if this function was able to handle the realm, {@code false} if the
   *     implementation did not process the realm or wants to defer the decision to another
   *     implementation.
   */
  boolean identifyRetained(@Nonnull RetainedCollector collector);
}
