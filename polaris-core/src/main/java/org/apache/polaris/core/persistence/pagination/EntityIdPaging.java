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

import static com.google.common.base.Preconditions.checkArgument;

import jakarta.annotation.Nullable;
import org.apache.polaris.core.entity.PolarisBaseEntity;

/** Utility class for processing pagination of streams of entities ordered by some ID property. */
public final class EntityIdPaging {
  private EntityIdPaging() {}

  /**
   * Produces the reference to the next page of data in the form of a numerical entity ID. Entities
   * in the associated stream of data are assumed to be ordered by ID.
   */
  public static @Nullable String encodedDataReference(PolarisBaseEntity entity) {
    if (entity == null) {
      return null;
    }
    return Long.toString(entity.getId());
  }

  /**
   * Extracts the entity ID from a {@link PageToken} assuming the request is a continuation of the
   * API call that previously produced an {@link #encodedDataReference(PolarisBaseEntity) entity ID
   * page token}. This ID is meant to be used as a boundary between the previous and the next pages
   * in a stream of entities ordered by their Entity IDs.
   */
  public static long entityIdBoundary(PageToken pageToken) {
    String encodedId = pageToken.encodedDataReference();
    checkArgument(encodedId != null, "Encoded data reference is null in the page request");
    return Long.parseLong(encodedId);
  }
}
