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
package org.apache.polaris.persistence.nosql.metastore.committers;

/**
 * Optional capability of the NoSQL metastore: commits a caller-supplied {@link
 * CatalogChangeCommitterWrapper} against a catalog's content, using the metastore's existing commit
 * and retry handling.
 *
 * <p>The wrapper is a callback that receives the catalog state and indexes for the current commit
 * attempt and applies its changes to them. If the attempt loses a compare-and-swap race, the
 * callback is invoked again with the latest state. Any checks the callback performs are therefore
 * evaluated against the same state its changes are committed on. This complements the built-in
 * entity mutations, whose set of changes is fixed before the first attempt.
 *
 * <p>Only the NoSQL metastore implements this interface. Callers should test for it with {@code
 * instanceof} and handle persistence backends that do not support it.
 */
public interface SupportsCatalogChangeCommit {

  /**
   * Commits {@code wrapper} against the given catalog's content, invoking it again with the latest
   * state on each retry.
   *
   * <p>The callback may run several times, and the result of an earlier invocation may never be
   * committed. The catalog state builder and the indexes it receives are rebuilt for each attempt,
   * but the {@code CommitterState} is shared across attempts: objects must be written with {@code
   * writeOrReplace} using stable keys, because {@code writeIntent} fails and {@code writeIfNew}
   * returns the earlier attempt's object when a key is reused. External side effects, such as
   * object-storage writes, event dispatch or calls to other services, are not undone between
   * attempts: perform them before calling this method, or make them idempotent.
   *
   * <p>If an attempt fails with an {@code UnknownOperationResultException}, its changes may have
   * been committed, and the next attempt may observe them. Whether to treat that as success or
   * failure is up to the callback.
   *
   * <p>The callback is responsible for keeping the name, id, change and location indexes consistent
   * with each other. None of the conflict or location-overlap checks of the built-in entity
   * mutations are applied.
   *
   * <p>The callback must return quickly and must not block. Each attempt holds an in-process lock
   * on the catalog's content reference, so a slow callback delays other local commits to that
   * catalog.
   *
   * @param catalogId ID of an existing catalog; must be positive
   * @param resultType the reified result type required by the commit machinery; must match {@code
   *     T}
   * @return the result of the callback, which must not be {@code null}
   */
  <T> T commitCatalogChange(
      long catalogId, Class<T> resultType, CatalogChangeCommitterWrapper<T> wrapper);
}
