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
package org.apache.polaris.persistence.nosql.coretypes.content;

public enum ContentStatus {
  /**
   * The initial state of a content object is "created", which means that the object name is
   * reserved, but it is not yet usable. This state can transition to {@link #ACTIVE} or {@link
   * #INACTIVE} or the content object can be directly deleted.
   */
  CREATED,
  /**
   * When a content object is fully setup, its state is "active". This state can only transition to
   * {@link #INACTIVE}.
   */
  ACTIVE,
  /**
   * An {@link #ACTIVE} content object can be put into "inactive" state, which means that it cannot
   * be used, but it can be put back into {@link #ACTIVE} state.
   */
  INACTIVE,
  /**
   * An {@link #INACTIVE} content object can be put into "purging" state, which means that it's data
   * is being purged. This is next to the final and terminal state {@link #PURGED}. Once all data of
   * the information has been purged, it must be set into {@link #PURGED} status or be entirely
   * removed.
   */
  PURGING,
  /**
   * "Purged" is the terminal state of every content object. A purged object can be safely deleted.
   * The difference between a "purged" and a non-existing (deleted) object is that the same name
   * cannot be (re)used.
   */
  PURGED,
}
