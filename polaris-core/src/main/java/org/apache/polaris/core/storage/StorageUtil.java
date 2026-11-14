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
package org.apache.polaris.core.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.jspecify.annotations.NonNull;

public class StorageUtil {
  /**
   * Concatenating two file paths by making sure one and only one path separator is placed between
   * the two paths.
   *
   * @param leftPath left path
   * @param rightPath right path
   * @param fileSep File separator to use.
   * @return Well formatted file path.
   */
  public static @NonNull String concatFilePrefixes(
      @NonNull String leftPath, String rightPath, String fileSep) {
    if (leftPath.endsWith(fileSep) && rightPath.startsWith(fileSep)) {
      return leftPath + rightPath.substring(1);
    } else if (!leftPath.endsWith(fileSep) && !rightPath.startsWith(fileSep)) {
      return leftPath + fileSep + rightPath;
    } else {
      return leftPath + rightPath;
    }
  }

  /**
   * Given a path, extract the bucket (authority).
   *
   * @param path A path to parse
   * @return The bucket/authority of the path
   */
  public static @NonNull String getBucket(String path) {
    int schemeSeparator = path.indexOf("://");
    if (schemeSeparator <= 0) {
      return null;
    }
    return StorageUri.parse(path).authority();
  }

  /** Given a TableMetadata, extracts the locations where the table's [meta]data might be found. */
  public static @NonNull Set<String> getLocationsUsedByTable(TableMetadata tableMetadata) {
    return getLocationsUsedByTable(tableMetadata.location(), tableMetadata.properties());
  }

  /** Given a baseLocation and entity (table?) properties, extracts the relevant locations */
  public static @NonNull Set<String> getLocationsUsedByTable(
      String baseLocation, Map<String, String> properties) {
    // The base location goes first so that it wins over an equivalent data or metadata location.
    List<String> candidates = new ArrayList<>();
    candidates.add(baseLocation);
    candidates.add(properties.get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY));
    candidates.add(
        properties.get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY));
    candidates.removeIf(Objects::isNull);
    return removeRedundantLocations(candidates);
  }

  /** Given a ViewMetadata, extracts the locations where the view's [meta]data might be found. */
  public static @NonNull Set<String> getLocationsUsedByTable(ViewMetadata viewMetadata) {
    return Set.of(viewMetadata.location());
  }

  /**
   * Removes "redundant" locations, so {/a/b/, /a/b/c, /a/b/d} will be reduced to just {/a/b/}.
   *
   * <p>Candidates are considered in order. When several candidates denote the same location (for
   * example, they differ only in scheme or trailing slash), the first one is kept. The raw location
   * strings are returned as-is, because {@link StorageLocation} normalizes local paths and callers
   * expect the original strings back.
   */
  private static @NonNull Set<String> removeRedundantLocations(List<String> candidates) {
    List<String> kept = new ArrayList<>();
    for (String candidate : candidates) {
      StorageLocation location = StorageLocation.of(candidate);
      // Skip the candidate when a kept location already covers it (is equal to or an ancestor of).
      if (kept.stream().map(StorageLocation::of).anyMatch(location::isChildOf)) {
        continue;
      }
      // Otherwise drop any kept location that this candidate covers, then keep the candidate.
      kept.removeIf(keptLocation -> StorageLocation.of(keptLocation).isChildOf(location));
      kept.add(candidate);
    }
    return Set.copyOf(kept);
  }
}
