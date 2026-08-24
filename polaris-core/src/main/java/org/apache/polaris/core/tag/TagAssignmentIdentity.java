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
package org.apache.polaris.core.tag;

import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityId;

/**
 * Which target and which field one assignment row of a tag definition names: the part of {@link
 * TagAssignmentRecord}'s identity that distinguishes the rows of a single definition from each
 * other. The definition's own (tagCatalogId, tagId) is left out because every use of this type is
 * already scoped to one definition, and the selected value is left out because it is content: a row
 * whose value another request replaced is still the same row.
 *
 * <p>That distinction is the reason this type exists rather than comparing records. {@link
 * TagAssignmentRecord#equals} includes the value, so comparing records would read a concurrent
 * value replacement as a different row.
 */
public record TagAssignmentIdentity(long targetCatalogId, long targetId, int fieldId) {

  public static TagAssignmentIdentity of(TagAssignmentRecord record) {
    return new TagAssignmentIdentity(
        record.getTargetCatalogId(), record.getTargetId(), record.getFieldId());
  }

  /**
   * The catalog id to look the target entity up by, which is not always {@link #targetCatalogId()}.
   * An assignment row on a catalog stores that catalog's own id as its containing catalog id, while
   * the catalog entity itself lives under the root container, so the mapping has to be inverted for
   * the lookup. This is the same inversion the assignment enumeration performs.
   */
  public long targetEntityCatalogId() {
    return targetCatalogId == targetId ? PolarisEntityConstants.getNullId() : targetCatalogId;
  }

  /** The id pair that reads this row's target entity, whatever type that entity is. */
  public PolarisEntityId targetEntityId() {
    return new PolarisEntityId(targetEntityCatalogId(), targetId);
  }
}
