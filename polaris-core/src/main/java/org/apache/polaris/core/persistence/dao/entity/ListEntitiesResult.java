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
package org.apache.polaris.core.persistence.dao.entity;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.persistence.pagination.Page;

/** the return the result for a list entities call */
public class ListEntitiesResult extends BaseResult {

  // null if not success. Else the list of entities being returned
  private final Page<EntityNameLookupRecord> entities;

  /** Create a {@link ListEntitiesResult} from a {@link Page} */
  public static ListEntitiesResult fromPage(Page<EntityNameLookupRecord> page) {
    return new ListEntitiesResult(page);
  }

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public ListEntitiesResult(@Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.entities = null;
  }

  /**
   * Constructor for success
   *
   * @param entities list of entities being returned, implies success
   */
  public ListEntitiesResult(Page<EntityNameLookupRecord> entities) {
    super(ReturnStatus.SUCCESS);
    this.entities = entities;
  }

  public @Nullable List<EntityNameLookupRecord> getEntities() {
    return entities == null ? null : entities.items();
  }

  public Page<EntityNameLookupRecord> getPage() {
    return entities == null ? Page.fromItems(List.of()) : entities;
  }
}
