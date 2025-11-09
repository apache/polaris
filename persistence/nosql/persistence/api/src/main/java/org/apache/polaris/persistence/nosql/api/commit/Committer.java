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

import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.ref.Reference;

/**
 * A {@link Committer} performs an atomic change against a named reference. This is a higher-level
 * functionality building on top of the low-level {@code RetryLoop}.
 *
 * <p>Committing use cases ensure that a {@linkplain Reference#pointer() reference} always points to
 * a consistent state, and that the change is atomic.
 *
 * <p>Committing use cases usually need to write more {@linkplain Obj objects} than just the
 * {@linkplain Reference#pointer() referenced} one. Implementations must use {@link
 * CommitterState#writeIntent(Object, Obj)} to get those objects being persisted. Retries can
 * {@linkplain CommitterState#getWrittenByKey(Object) check} whether an object has already been
 * written to prevent unnecessary write operations against the backend database.
 *
 * <p>A committing use case {@linkplain Persistence#createCommitter(String, Class, Class) creates} a
 * {@link Committer} instance using a {@link CommitRetryable} implementation, which {@linkplain
 * CommitRetryable#attempt(CommitterState, Supplier) receives} the {@linkplain Obj object} pointed
 * in the {@linkplain Reference reference} and returns the new object to which the reference shall
 * point to.
 *
 * @param <REF_OBJ> type of the {@link Obj} {@linkplain Reference#pointer() referenced}
 * @param <RESULT> the commit result type for successful commits including non-changing
 */
public interface Committer<REF_OBJ extends BaseCommitObj, RESULT> {

  /**
   * When called, commits to the same reference will be synchronized locally.
   *
   * <p>Using local reference-synchronization prevents commit retries. When using this feature, the
   * actual {@link CommitRetryable#attempt(CommitterState, Supplier)} implementation must not block
   * and complete quickly.
   */
  Committer<REF_OBJ, RESULT> synchronizingLocally();

  /**
   * Perform an atomic change.
   *
   * <p>The given {@link CommitRetryable} is called to perform the actual change. The implementation
   * of the {@link CommitRetryable} must be side-effect-free and prepared to be called multiple
   * times.
   *
   * @param commitRetryable performs the state change, must be side-effect-free
   * @return the result as returned via {@link CommitterState#commitResult(Object,
   *     BaseCommitObj.Builder, Optional)} or an empty optional if {@linkplain
   *     CommitterState#noCommit() no change happened}
   */
  Optional<RESULT> commit(CommitRetryable<REF_OBJ, RESULT> commitRetryable)
      throws CommitException, RetryTimeoutException;

  /**
   * Same as {@link #commit(CommitRetryable)}, but wraps the checked exceptions in a {@link
   * RuntimeException}.
   */
  default Optional<RESULT> commitRuntimeException(
      CommitRetryable<REF_OBJ, RESULT> commitRetryable) {
    try {
      return commit(commitRetryable);
    } catch (RetryTimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
