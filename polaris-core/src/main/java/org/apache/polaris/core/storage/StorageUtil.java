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

import jakarta.annotation.Nonnull;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;

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
  public static @Nonnull String concatFilePrefixes(
      @Nonnull String leftPath, String rightPath, String fileSep) {
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
  public static @Nonnull String getBucket(String path) {
    URI uri = URI.create(path);
    return getBucket(uri);
  }

  /**
   * Given a URI, extract the bucket (authority).
   *
   * @param uri A path to parse
   * @return The bucket/authority of the URI
   */
  public static @Nonnull String getBucket(URI uri) {
    return uri.getAuthority();
  }

  /** Given a TableMetadata, extracts the locations where the table's [meta]data might be found. */
  public static @Nonnull Set<String> getLocationsAllowedToBeAccessed(TableMetadata tableMetadata) {
    return getLocationsAllowedToBeAccessed(tableMetadata.location(), tableMetadata.properties());
  }

  /** Given a baseLocation and entity (table?) properties, extracts the relevant locations */
  public static @Nonnull Set<String> getLocationsAllowedToBeAccessed(
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
  public static @Nonnull Set<String> getLocationsAllowedToBeAccessed(ViewMetadata viewMetadata) {
    return Set.of(viewMetadata.location());
  }

  /** Removes "redundant" locations, so {/a/b/, /a/b/c, /a/b/d} will be reduced to just {/a/b/} */
  private static @Nonnull Set<String> removeRedundantLocations(Set<String> locationStrings) {
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
