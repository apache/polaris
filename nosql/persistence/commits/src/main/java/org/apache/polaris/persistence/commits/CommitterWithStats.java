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
package org.apache.polaris.persistence.commits;

import java.util.Optional;
import org.apache.polaris.persistence.api.commit.CommitException;
import org.apache.polaris.persistence.api.commit.CommitRetryable;
import org.apache.polaris.persistence.api.commit.Committer;
import org.apache.polaris.persistence.api.commit.RetryTimeoutException;
import org.apache.polaris.persistence.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.commits.retry.RetryStatsConsumer;

/**
 * Extension of {@link Committer} that provides retry-information callbacks, used for testing
 * purposes.
 */
public interface CommitterWithStats<REF_OBJ extends BaseCommitObj, RESULT>
    extends Committer<REF_OBJ, RESULT> {

  Optional<RESULT> commit(
      CommitRetryable<REF_OBJ, RESULT> commitRetryable, RetryStatsConsumer retryStatsConsumer)
      throws CommitException, RetryTimeoutException;

  default Optional<RESULT> commitRuntimeException(
      CommitRetryable<REF_OBJ, RESULT> commitRetryable, RetryStatsConsumer retryStatsConsumer) {
    try {
      return commit(commitRetryable, retryStatsConsumer);
    } catch (RetryTimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
