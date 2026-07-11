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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
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
    Set<String> locations = new HashSet<>();
    locations.add(baseLocation);
    locations.add(properties.get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY));
    locations.add(
        properties.get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY));
    locations.remove(null);
    return removeRedundantLocations(locations);
  }

  /** Given a ViewMetadata, extracts the locations where the view's [meta]data might be found. */
  public static @NonNull Set<String> getLocationsUsedByTable(ViewMetadata viewMetadata) {
    return Set.of(viewMetadata.location());
  }

  /**
   * Returns true if the set of locations used by {@code current} differs from the set used by
   * {@code base}. Covers the table location as well as write.data.path and write.metadata.path
   * property overrides.
   */
  public static boolean locationsChanged(TableMetadata base, TableMetadata current) {
    return !getLocationsUsedByTable(base).equals(getLocationsUsedByTable(current));
  }

  /**
   * Checks that no two distinct tables in {@code batch} use overlapping locations (pairwise
   * containment check across all data locations returned by {@link #getLocationsUsedByTable}).
   * Same-identifier pairs are skipped so a table's own write.data.path and write.metadata.path
   * don't count as self-overlap.
   *
   * @throws BadRequestException if any two distinct tables have overlapping locations
   */
  public static void validateNoOverlapWithinBatch(
      List<Map.Entry<TableIdentifier, TableMetadata>> batch) {
    List<Map.Entry<TableIdentifier, StorageLocation>> entries =
        batch.stream()
            .flatMap(
                e ->
                    getLocationsUsedByTable(e.getValue()).stream()
                        .map(loc -> Map.entry(e.getKey(), StorageLocation.of(loc))))
            .toList();
    for (int i = 0; i < entries.size(); i++) {
      for (int j = i + 1; j < entries.size(); j++) {
        TableIdentifier idA = entries.get(i).getKey();
        TableIdentifier idB = entries.get(j).getKey();
        if (idA.equals(idB)) {
          continue;
        }
        StorageLocation a = entries.get(i).getValue();
        StorageLocation b = entries.get(j).getValue();
        if (a.isChildOf(b) || b.isChildOf(a)) {
          throw new BadRequestException(
              "Transaction contains changes whose locations overlap: '%s' for table '%s' and"
                  + " '%s' for table '%s'",
              a, idA, b, idB);
        }
      }
    }
  }

  /** Removes "redundant" locations, so {/a/b/, /a/b/c, /a/b/d} will be reduced to just {/a/b/} */
  private static @NonNull Set<String> removeRedundantLocations(Set<String> locationStrings) {
    HashSet<String> result = new HashSet<>(locationStrings);

    for (String potentialParent : locationStrings) {
      StorageLocation potentialParentLocation = StorageLocation.of(potentialParent);
      for (String potentialChild : locationStrings) {
        if (!potentialParent.equals(potentialChild)) {
          StorageLocation potentialChildLocation = StorageLocation.of(potentialChild);
          if (potentialChildLocation.isChildOf(potentialParentLocation)) {
            result.remove(potentialChild);
          }
        }
      }
    }
    return result;
  }
}
