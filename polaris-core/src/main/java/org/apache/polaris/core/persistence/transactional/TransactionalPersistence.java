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
package org.apache.polaris.core.persistence.transactional;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.IntegrationPersistence;

/**
 * Extends BasePersistence to express a more "transaction-oriented" control flow for backing stores
 * which can support a runInTransaction semantic, while providing default implementations of some of
 * the BasePersistence methods in terms of lower-level methods that subclasses must implement.
 */
public interface TransactionalPersistence extends BasePersistence, IntegrationPersistence {

  /**
   * Run the specified transaction code (a Supplier lambda type) in a database read/write
   * transaction. If the code of the transaction does not throw any exception and returns normally,
   * the transaction will be committed, else the transaction will be automatically rolled-back on
   * error. The result of the supplier lambda is returned if success, else the error will be
   * re-thrown.
   *
   * @param callCtx call context
   * @param transactionCode code of the transaction being executed, a supplier lambda
   */
  <T> T runInTransaction(@Nonnull PolarisCallContext callCtx, @Nonnull Supplier<T> transactionCode);

  /**
   * Run the specified transaction code (a runnable lambda type) in a database read/write
   * transaction. If the code of the transaction does not throw any exception and returns normally,
   * the transaction will be committed, else the transaction will be automatically rolled-back on
   * error.
   *
   * @param callCtx call context
   * @param transactionCode code of the transaction being executed, a runnable lambda
   */
  void runActionInTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Runnable transactionCode);

  /**
   * Run the specified transaction code (a Supplier lambda type) in a database read transaction. If
   * the code of the transaction does not throw any exception and returns normally, the transaction
   * will be committed, else the transaction will be automatically rolled-back on error. The result
   * of the supplier lambda is returned if success, else the error will be re-thrown.
   *
   * @param callCtx call context
   * @param transactionCode code of the transaction being executed, a supplier lambda
   */
  <T> T runInReadTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Supplier<T> transactionCode);

  /**
   * Run the specified transaction code (a runnable lambda type) in a database read transaction. If
   * the code of the transaction does not throw any exception and returns normally, the transaction
   * will be committed, else the transaction will be automatically rolled-back on error.
   *
   * @param callCtx call context
   * @param transactionCode code of the transaction being executed, a runnable lambda
   */
  void runActionInReadTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Runnable transactionCode);

  /**
   * Same as {@link org.apache.polaris.core.persistence.BasePersistence#writeEntity}, except expects
   * to be run as part of a larger caller-defined transactional action, and so doesn't use
   * transactions internally.
   *
   * @param callCtx call context
   * @param entity entity to persist
   * @param nameOrParentChanged if true, also write it to by-name lookups if applicable
   * @param originalEntity original state of the entity to use for compare-and-swap purposes, or
   *     null if this is expected to be a brand-new entity
   */
  void writeEntityInOuterTransaction(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity);

  /**
   * Lookup the specified set of entities by entityActiveKeys Return the result, a parallel list of
   * active records. A record in that list will be null if its associated lookup failed
   *
   * @return the list of entityActiveKeys for the specified lookup operation
   */
  @Nonnull
  List<EntityNameLookupRecord> lookupEntityActiveBatch(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntitiesActiveKey> entityActiveKeys);

  /** Rollback the current transaction */
  void rollback();
}
