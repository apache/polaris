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
   */
  <C extends BaseCommitObj> Iterator<C> commitLog(
      String refName, OptionalLong offset, Class<C> clazz);

  /**
   * Retrieves the commit log in chronological order starting at the given offset.
   *
   * <p>This function is useful when retrieving commits to serve events/notification use cases.
   */
  <C extends BaseCommitObj> Iterator<C> commitLogReversed(
      String refName, long offset, Class<C> clazz);
}
