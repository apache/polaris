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
package org.apache.polaris.core.persistence.pagination;

import java.util.List;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;

public class EntityIdPageToken extends PageToken implements HasPageSize {

  public static final String PREFIX = "entity-id";

  /** The minimum ID that could be attached to an entity */
  private static final long MINIMUM_ID = 0;

  /** The entity ID to use to start with. */
  public static final long BASE_ID = MINIMUM_ID - 1;

  private final long entityId;
  private final int pageSize;

  public EntityIdPageToken(int pageSize) {
    this.entityId = BASE_ID;
    this.pageSize = pageSize;
  }

  public EntityIdPageToken(long entityId, int pageSize) {
    this.entityId = entityId;
    this.pageSize = pageSize;
  }

  public long getId() {
    return entityId;
  }

  @Override
  public int getPageSize() {
    return this.pageSize;
  }

  @Override
  public String toTokenString() {
    return String.format("%s/%d/%d", PREFIX, entityId, pageSize);
  }

  /**
   * Builds a new page token to reflect new data that's been read. This implementation assumes that
   * the input list is sorted, and checks that it's a list of `PolarisBaseEntity` or
   * `EntityNameLookupRecord`
   */
  @Override
  public PageToken updated(List<?> newData) {
    if (newData == null || newData.size() < this.pageSize) {
      return new DonePageToken();
    } else {
      // Assumed to be sorted with the greatest entity ID last
      var tail = newData.get(newData.size() - 1);
      if (tail instanceof PolarisBaseEntity) {
        return new EntityIdPageToken(((PolarisBaseEntity) tail).getId(), this.pageSize);
      } else if (tail instanceof EntityNameLookupRecord) {
        return new EntityIdPageToken(((EntityNameLookupRecord) tail).getId(), this.pageSize);
      } else {
        throw new IllegalArgumentException(
            "Cannot build a page token from: " + tail.getClass().getSimpleName());
      }
    }
  }
}
