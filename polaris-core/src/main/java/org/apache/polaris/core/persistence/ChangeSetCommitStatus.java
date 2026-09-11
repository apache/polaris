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
package org.apache.polaris.core.persistence;

/**
 * Outcome of {@link BasePersistence#commitChangeSet} for one change set.
 *
 * <p>Atomicity is a property of the resolved placement of <em>this</em> change set, not a
 * backend-wide capability flag. A backend that co-locates the records in the change set behind one
 * transactional store returns {@link #APPLIED}. A backend that cannot commit this particular change
 * set atomically — mixed stores, an unsupported record kind, or no transactional primitive —
 * returns {@link #UNSUPPORTED} and must not apply any mutation. Best-effort sequencing is a
 * separate manager contract ({@link PolarisMetaStoreManager#applyChangeSetBestEffort}).
 */
public enum ChangeSetCommitStatus {
  /** All mutations in the change set were applied in one atomic step. */
  APPLIED,

  /**
   * This backend cannot atomically apply this change set. No mutations were applied; the caller may
   * use {@link PolarisMetaStoreManager#applyChangeSetBestEffort} or dedicated per-record APIs.
   */
  UNSUPPORTED
}
