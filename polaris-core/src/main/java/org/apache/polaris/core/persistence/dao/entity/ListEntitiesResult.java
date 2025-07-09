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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;

/** the return the result for a list entities call */
public class ListEntitiesResult extends BaseResult {

  // null if not success. Else the list of entities being returned
  private final List<EntityNameLookupRecord> entities;
  private final Optional<PageToken> pageTokenOpt;

  /** Create a {@link ListEntitiesResult} from a {@link Page} */
  public static ListEntitiesResult fromPage(Page<EntityNameLookupRecord> page) {
    return new ListEntitiesResult(page.items, Optional.ofNullable(page.pageToken));
  }

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public ListEntitiesResult(
      @Nonnull ReturnStatus errorCode,
      @Nullable String extraInformation,
      @Nonnull Optional<PageToken> pageTokenOpt) {
    super(errorCode, extraInformation);
    this.entities = null;
    this.pageTokenOpt = pageTokenOpt;
  }

  /**
   * Constructor for success
   *
   * @param entities list of entities being returned, implies success
   */
  public ListEntitiesResult(
      @Nonnull List<EntityNameLookupRecord> entities, @Nonnull Optional<PageToken> pageTokenOpt) {
    super(ReturnStatus.SUCCESS);
    this.entities = entities;
    this.pageTokenOpt = pageTokenOpt;
  }

  @JsonCreator
  private ListEntitiesResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @JsonProperty("entities") List<EntityNameLookupRecord> entities,
      @JsonProperty("pageToken") Optional<PageToken> pageTokenOpt) {
    super(returnStatus, extraInformation);
    this.entities = entities;
    this.pageTokenOpt = pageTokenOpt;
  }

  public List<EntityNameLookupRecord> getEntities() {
    return entities;
  }

  public Optional<PageToken> getPageToken() {
    return pageTokenOpt;
  }
}
