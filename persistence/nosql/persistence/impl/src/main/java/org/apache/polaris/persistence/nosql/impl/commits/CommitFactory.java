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
package org.apache.polaris.persistence.nosql.impl.commits;

import jakarta.annotation.Nonnull;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.commit.Commits;
import org.apache.polaris.persistence.nosql.api.commit.Committer;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.ref.Reference;

public final class CommitFactory {
  private CommitFactory() {}

  /**
   * Create a new {@link Commits} instance for the given {@link Persistence} instance.
   *
   * <p>Note: In a CDI container {@link Commits} can be directly injected, a call to this function
   * is not required.
   */
  public static Commits newCommits(Persistence persistence) {
    return new CommitsImpl(persistence);
  }

  /**
   * Creates a new {@link Committer} instance.
   *
   * @param persistence persistence used
   * @param refName name of the reference
   * @param referencedObjType type of the {@linkplain Reference#pointer() referenced} object
   * @return new committer
   * @param <REF_OBJ> type of the {@linkplain Reference#pointer() referenced} object
   * @param <RESULT> the commit result type, for successful commits including non-changing
   */
  public static <REF_OBJ extends BaseCommitObj, RESULT> Committer<REF_OBJ, RESULT> newCommitter(
      @Nonnull Persistence persistence,
      @Nonnull String refName,
      @Nonnull Class<REF_OBJ> referencedObjType,
      @Nonnull Class<RESULT> resultType) {
    return new CommitterImpl<>(persistence, refName, referencedObjType, resultType);
  }
}
