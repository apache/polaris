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
package org.apache.polaris.persistence.nosql.api.index;

import jakarta.annotation.Nonnull;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

/** Represents an {@link Index} that can be modified using put and remove functions. */
public interface ModifiableIndex<V> extends Index<V> {

  /**
   * Adds the given key element or updates the {@link ObjRef} if the {@link IndexKey} already
   * existed.
   *
   * @return {@code true} if the {@link IndexKey} didn't exist or {@code false} if the key was
   *     already present and the operation only updated the {@link ObjRef}.
   */
  boolean put(@Nonnull IndexKey key, @Nonnull V value);

  /**
   * Removes the index element for the given key.
   *
   * @return {@code true} if the {@link IndexKey} did exist and was removed, {@code false}
   *     otherwise.
   */
  boolean remove(@Nonnull IndexKey key);
}
