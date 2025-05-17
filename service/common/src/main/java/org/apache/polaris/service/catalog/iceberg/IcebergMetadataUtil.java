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

import java.util.HashSet;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;

/** A collection of util methods related to handling Iceberg table & view metadata */
public class IcebergMetadataUtil {

  public static Set<String> getLocationsAllowedToBeAccessed(TableMetadata tableMetadata) {
    Set<String> locations = new HashSet<>();
    locations.add(tableMetadata.location());
    if (tableMetadata
        .properties()
        .containsKey(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY)) {
      locations.add(
          tableMetadata
              .properties()
              .get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY));
    }
    if (tableMetadata
        .properties()
        .containsKey(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY)) {
      locations.add(
          tableMetadata
              .properties()
              .get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY));
    }
    return locations;
  }

  public static Set<String> getLocationsAllowedToBeAccessed(ViewMetadata viewMetadata) {
    return Set.of(viewMetadata.location());
  }
}
