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
package org.apache.polaris.persistence.relational.jdbc;

/**
 * Deletes a single realm's rows from one purgeable store in bounded batches. Implementations are
 * discovered by the background purge drainer, so a new store (e.g. relocated metrics persistence)
 * can plug in by providing an implementation without changing the drainer.
 */
public interface RealmDataPurger {

  /**
   * Deletes up to {@code batchSize} rows belonging to {@code realmId} that are older than {@code
   * cutoffMs}. Rows at or after the cutoff belong to a realm of the same name bootstrapped after
   * the purge was requested, and must be left alone.
   *
   * <p>This compares the time a row records against the time a purge was requested. The two are
   * taken from different clocks whenever the node that purged the realm is not the node that wrote
   * the row, so a row written within the clock offset of the purge can fall on either side of the
   * cutoff. Separating two incarnations of a realm name does not need better resolution than that:
   * a realm is bootstrapped again by an operator, long after the purge in every practical case.
   *
   * @return the number of rows deleted; {@code 0} means the store is fully drained for the realm.
   */
  long purgeBatch(String realmId, long cutoffMs, int batchSize);
}
