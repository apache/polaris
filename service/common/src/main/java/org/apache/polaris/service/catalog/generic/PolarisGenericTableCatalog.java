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

package org.apache.polaris.service.catalog.generic;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;

public class PolarisGenericTableCatalog {
    /**
     * Caller must fill in all entity fields except parentId, since the caller may not want to
     * duplicate the logic to try to resolve parentIds before constructing the proposed entity. This
     * method will fill in the parentId if needed upon resolution.
     */
    private void createTableLike(TableIdentifier identifier, PolarisEntity entity) {
        PolarisResolvedPathWrapper resolvedParent =
            resolvedEntityView.getResolvedPath(identifier.namespace());
        if (resolvedParent == null) {
            // Illegal state because the namespace should've already been in the static resolution set.
            throw new IllegalStateException(
                String.format("Failed to fetch resolved parent for TableIdentifier '%s'", identifier));
        }

        createTableLike(identifier, entity, resolvedParent);
    }

}
