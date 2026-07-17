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
package org.apache.polaris.service.catalog.iceberg;

import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.jspecify.annotations.NonNull;

/**
 * Buffers the persistence and metadata-cleanup effects of table commits until they can be applied.
 */
final class PendingTableCommit {
  private final List<EntityWithPath> pendingUpdates = new ArrayList<>();
  private final List<MetadataFileCleanup> pendingMetadataFileCleanups = new ArrayList<>();

  void addPendingUpdate(
      @NonNull List<PolarisEntityCore> catalogPath, @NonNull PolarisBaseEntity entity) {
    pendingUpdates.add(new EntityWithPath(catalogPath, entity));
  }

  void addPendingMetadataFileCleanup(@NonNull MetadataFileCleanup cleanup) {
    pendingMetadataFileCleanups.add(cleanup);
  }

  List<EntityWithPath> pendingUpdates() {
    return List.copyOf(pendingUpdates);
  }

  List<MetadataFileCleanup> pendingMetadataFileCleanups() {
    return List.copyOf(pendingMetadataFileCleanups);
  }
}
