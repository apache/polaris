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
import java.util.function.BiConsumer;
import org.apache.polaris.persistence.nosql.api.obj.Obj;

public interface UpdatableIndex<V> extends ModifiableIndex<V> {
  /**
   * Build a serializable index container from this index object. This updatable index may no longer
   * be accessible after this function has been called, runtime exception may be thrown if the index
   * is accessed after calling this function.
   *
   * @param prefix prefix to pass to the string argument of the {@code persistObj} consumer.
   * @param persistObj callback invoked to persist object, to be delegated to {@code
   *     CommitterState.writeOrReplace()}
   * @return the updated {@link IndexContainer}
   */
  IndexContainer<V> toIndexed(
      @Nonnull String prefix, @Nonnull BiConsumer<String, ? super Obj> persistObj);
}
