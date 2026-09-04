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
package org.apache.polaris.core.persistence;

import java.util.List;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.jspecify.annotations.Nullable;

/** Validation shared by the {@link PolarisMetaStoreManager#renameEntity} implementations. */
public final class RenameEntityUtil {

  private RenameEntityUtil() {}

  /**
   * Reject a rename that would move an entity out of the catalog it currently lives in.
   *
   * <p>Such a move is not a one-field change: the entity's children, its grant records, and its
   * policy mappings all still reference the catalog it started in, and a rename updates none of
   * them. Persisting it would leave a row whose catalog and parent disagree, which by-name lookups
   * match from neither side. A caller that wants a real cross-catalog move needs a new, explicit
   * contract for it rather than a way around this check.
   *
   * @param entityToRename the entity being renamed, whose catalog id is the authoritative source
   *     for where it lives today
   * @param newCatalogPath the destination path. {@code null} means no re-parenting was requested
   *     and nothing is checked. An empty list means the top-level realm scope, which is a different
   *     catalog than any catalog-contained entity's and so is rejected.
   * @throws IllegalArgumentException if the destination is in a different catalog
   */
  public static void checkRenameStaysWithinCatalog(
      PolarisEntityCore entityToRename, @Nullable List<PolarisEntityCore> newCatalogPath) {
    if (newCatalogPath == null) {
      return;
    }
    long destinationCatalogId =
        newCatalogPath.isEmpty()
            ? PolarisEntityConstants.getNullId()
            : newCatalogPath.get(0).getId();
    if (destinationCatalogId != entityToRename.getCatalogId()) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot rename entity %s into a different catalog: it lives in catalog %d and the "
                  + "destination is in catalog %d",
              entityToRename.getName(), entityToRename.getCatalogId(), destinationCatalogId));
    }
  }
}
