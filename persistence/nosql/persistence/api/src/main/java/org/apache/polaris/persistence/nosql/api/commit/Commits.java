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

import java.util.Iterator;
import java.util.OptionalLong;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;

/** Provides iterator-based access to the history of a named reference. */
public interface Commits {

  /**
   * Retrieves the commit log in the natural, chronologically reverse order - most recent commit
   * first.
   *
   * <p>If {@code offsetCommitId} is empty, iteration starts at the current head of {@code refName}.
   *
   * <p>If {@code offsetCommitId} is present, the returned iterator is inclusive of that commit ID.
   * Resolving the offset commit ID intentionally performs a linear walk from the current head
   * toward older commits until the commit ID is encountered or visible history ends. This lookup is
   * not internally bounded.
   *
   * <p>If the offset commit ID is not encountered while walking the visible history, the
   * implementation attempts to resume directly from the commit object identified by that ID. If
   * that commit object is not available either, iteration resumes from the oldest still-visible
   * commit reached by the scan, if any.
   *
   * @param offsetCommitId optional commit ID to resume from; not a sequential position in the
   *     commit log
   */
  <C extends BaseCommitObj> Iterator<C> commitLog(
      String refName, OptionalLong offsetCommitId, Class<C> clazz);

  /**
   * Retrieves the commit log in chronological order starting after the given offset commit ID.
   *
   * <p>The supplied {@code offsetCommitId} is exclusive. Resolving the offset commit ID
   * intentionally performs a linear walk from the current head toward older commits until the
   * commit ID is encountered or visible history ends. This lookup is not internally bounded.
   *
   * <p>If the offset commit ID is not encountered in the visible history, the full visible history
   * is returned in chronological order.
   *
   * <p>This function is useful when retrieving commits to serve events/notification use cases.
   *
   * @param offsetCommitId commit ID to resume after; not a sequential position in the commit log
   */
  <C extends BaseCommitObj> Iterator<C> commitLogReversed(
      String refName, long offsetCommitId, Class<C> clazz);
}
