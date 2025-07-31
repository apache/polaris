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
package org.apache.polaris.service.catalog.policy;

import jakarta.annotation.Nonnull;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.service.types.PolicyAttachmentTarget;

public class PolicyCatalogUtils {

  public static PolarisResolvedPathWrapper getResolvedPathWrapper(
      @Nonnull PolarisResolutionManifest resolutionManifest,
      @Nonnull PolicyAttachmentTarget target) {
    return switch (target.getType()) {
      // get the current catalog entity, since policy cannot apply across catalog at this moment
      case CATALOG -> resolutionManifest.getResolvedReferenceCatalogEntity();
      case NAMESPACE -> {
        var namespace = Namespace.of(target.getPath().toArray(new String[0]));
        var resolvedTargetEntity = resolutionManifest.getResolvedPath(namespace);
        if (resolvedTargetEntity == null) {
          throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
        yield resolvedTargetEntity;
      }
      case TABLE_LIKE -> {
        var tableIdentifier = TableIdentifier.of(target.getPath().toArray(new String[0]));
        // only Iceberg tables are supported
        var resolvedTableEntity =
            resolutionManifest.getResolvedPath(
                tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE);
        if (resolvedTableEntity == null) {
          throw new NoSuchTableException("Iceberg Table does not exist: %s", tableIdentifier);
        }
        yield resolvedTableEntity;
      }
      default -> throw new IllegalArgumentException("Unsupported target type: " + target.getType());
    };
  }
}
