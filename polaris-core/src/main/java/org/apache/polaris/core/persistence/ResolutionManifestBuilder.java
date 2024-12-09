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

import java.util.function.Function;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.resolver.ResolverPath;

public interface ResolutionManifestBuilder {
  ResolutionManifestBuilder withRootContainerEntity(
      ResolvedPolarisEntity simulatedResolvedRootContainerEntity);

  ResolutionManifestBuilder addTopLevelName(
      String topLevelEntityName, PolarisEntityType entityType, boolean optional);

  ResolutionManifestBuilder addPath(ResolverPath resolverPath, String catalogRoleName);

  ResolutionManifestBuilder addPath(ResolverPath resolverPath, Namespace namespace);

  ResolutionManifestBuilder addPath(ResolverPath resolverPath, TableIdentifier tableIdentifier);

  ResolutionManifestBuilder addPassthroughPath(
      ResolverPath resolverPath, TableIdentifier tableIdentifier);

  ResolutionManifestBuilder addPassthroughPath(ResolverPath resolverPath, Namespace namespace);

  ResolutionManifestBuilder notFoundExceptionMapper(
      Function<EntityNotFoundException, RuntimeException> notFoundExceptionMapper);

  ResolutionManifest buildResolved(PolarisEntitySubType subType);

  ResolutionManifest buildResolved();
}
