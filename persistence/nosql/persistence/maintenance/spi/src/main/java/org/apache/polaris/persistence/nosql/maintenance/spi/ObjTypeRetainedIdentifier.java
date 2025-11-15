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
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.api.ref.Reference;

/**
 * Implementations of this interface are called for objects that have been identified to be
 * <em>retained</em> either by a {@link PerRealmRetainedIdentifier} or another {@link
 * ObjTypeRetainedIdentifier}.
 *
 * <p>Polaris extensions and plugins that persist non-standard {@linkplain Reference references} or
 * {@linkplain Obj objects} must provide an implementation of this interface to ensure that the
 * required references and objects are not purged.
 *
 * <p>Implementation must be annotated as {@link
 * jakarta.enterprise.context.ApplicationScoped @ApplicationScoped} for CDI usage.
 */
public interface ObjTypeRetainedIdentifier {
  /** Human-readable name. */
  String name();

  /** The object type that the implementation handles. */
  @Nonnull
  ObjType handledObjType();

  /**
   * Called for every scanned object with the ID {@code objRef} having the object type yielded by
   * {@link #handledObjType()}.
   *
   * <p>Any exception thrown from this function aborts the whole maintenance run. Exceptions thrown
   * from functionality called by the implementation must be properly handled.
   *
   * @param collector instance that collects the objects and references to retain
   * @param objRef ID of the object that has been scanned
   */
  void identifyRelatedObj(@Nonnull RetainedCollector collector, @Nonnull ObjRef objRef);
}
