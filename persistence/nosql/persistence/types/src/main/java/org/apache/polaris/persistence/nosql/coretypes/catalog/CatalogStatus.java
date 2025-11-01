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
package org.apache.polaris.persistence.nosql.coretypes.catalog;

public enum CatalogStatus {
  /**
   * The initial state of a catalog is "created", which means that the catalog name is reserved, but
   * the catalog is not yet usable. This state can transition to {@link #ACTIVE} or {@link
   * #INACTIVE} or the catalog can be directly deleted.
   */
  CREATED,
  /**
   * When a catalog is fully setup, its state is "active". This state can only transition to {@link
   * #INACTIVE}.
   */
  ACTIVE,
  /**
   * An {@link #ACTIVE} catalog can be put into "inactive" state, which means that the catalog
   * cannot be used, but it can be put back into {@link #ACTIVE} state.
   */
  INACTIVE,
  /**
   * An {@link #INACTIVE} catalog can be put into "purging" state, which means that the catalog's
   * data is being purged from the persistence database. This is next to the final and terminal
   * state {@link #PURGED} of a catalog. Once all data of the catalog has been purged, it must at
   * least be set into {@link #PURGED} status or be entirely removed.
   */
  PURGING,
  /**
   * "Purged" is the terminal state of every catalog. A purged catalog can be safely deleted. The
   * difference between a "purged" catalog and a non-existing (deleted) catalog is that the name of
   * a purged catalog name cannot be (re)used.
   */
  PURGED,
}
