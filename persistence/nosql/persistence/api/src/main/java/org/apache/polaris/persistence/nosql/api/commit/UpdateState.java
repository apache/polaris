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
package org.apache.polaris.persistence.nosql.api.commit;

import jakarta.annotation.Nonnull;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

public interface UpdateState {

  /**
   * Use this instance of {@link Persistence} instead for operations related to this state,
   * especially from {@link CommitRetryable#attempt(CommitterState, Supplier)}.
   */
  Persistence persistence();

  /**
   * Add {@code obj} to the list of objects to be persisted, using {@code key} to {@linkplain
   * #getWrittenByKey(Object) identify/reuse} an already persisted object in a retried attempt.
   *
   * <p>Prefer this function over {@link #writeIntent(Object, Obj)} and {@link
   * #getWrittenByKey(Object)}.
   *
   * <p>Note that objects will not be immediately persisted, but after the {@linkplain
   * CommitRetryable#attempt(CommitterState, Supplier) attempt returns}, but before the {@linkplain
   * Committer#commit(CommitRetryable) commit returns}.
   *
   * <p>A {@linkplain Committer#commit(CommitRetryable) failed commit} will delete objects passed to
   * this function.
   *
   * @param key key identifying {@code obj}
   * @param obj object to persist
   * @return returns the given {@code obj}, if {@code key} is new, or the previous {@linkplain Obj},
   *     if {@code key} was already used in a call to this function or {@link #writeIntent(Object,
   *     Obj)}.
   */
  <O extends Obj> O writeIfNew(@Nonnull Object key, @Nonnull O obj, @Nonnull Class<O> type);

  default Obj writeIfNew(@Nonnull Object key, @Nonnull Obj obj) {
    return writeIfNew(key, obj, Obj.class);
  }

  /**
   * Add {@code obj} to the list of objects to be persisted, using {@code key} to {@linkplain
   * #getWrittenByKey(Object) identify/reuse} an already persisted object in a retried attempt.
   *
   * <p>If an object was already associated with the same {@code key}, the previous object will be
   * eventually deleted.
   *
   * @param key key identifying {@code obj}
   * @param obj object to persist
   * @return returns {@code obj}
   */
  <O extends Obj> O writeOrReplace(@Nonnull Object key, @Nonnull O obj, @Nonnull Class<O> type);

  default Obj writeOrReplace(@Nonnull Object key, @Nonnull Obj obj) {
    return writeOrReplace(key, obj, Obj.class);
  }

  /**
   * Get an already present object by a use-case defined key.
   *
   * @return the already present object or {@code null}, if no object is associated with the {@code
   *     key}
   */
  Obj getWrittenByKey(@Nonnull Object key);

  /**
   * Get an already present object by its {@link ObjRef}.
   *
   * @return the already present object or {@code null}, if no object is associated with the {@code
   *     id}
   */
  <C extends Obj> C getWrittenById(ObjRef id, Class<C> clazz);

  /**
   * Add {@code obj} to the list of objects to be persisted, using {@code key} to {@linkplain
   * #getWrittenByKey(Object) identify/reuse} an already persisted object in a retried attempt.
   *
   * <p>Note that objects will not be immediately persisted, but after the {@linkplain
   * CommitRetryable#attempt(CommitterState, Supplier) attempt returns}, but before the {@linkplain
   * Committer#commit(CommitRetryable) commit returns}.
   *
   * <p>A {@linkplain Committer#commit(CommitRetryable) failed commit} will delete objects passed to
   * this function.
   *
   * <p>Prefer {@link #writeIfNew(Object, Obj)}, if possible.
   *
   * @param key key identifying {@code obj}, must be unique across all objects. Throws an {@link
   *     IllegalStateException}, if the {@code key} has already been used.
   * @param obj object to persist
   */
  void writeIntent(@Nonnull Object key, @Nonnull Obj obj);
}
