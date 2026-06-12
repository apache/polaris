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
package org.apache.polaris.service.storage;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.polaris.core.storage.StorageLocationPreparer;
import org.apache.polaris.core.storage.StorageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for cloud-storage folder preparers whose backing store has hierarchical-namespace
 * semantics (folders are first-class resources that must exist before nested files can be written).
 * Handles URI parsing, folder hierarchy resolution, and bucket-level grouping; subclasses implement
 * the storage-specific folder-creation call.
 *
 * <p>Today there is one concrete implementation (GCS HNS). Azure ADLS Gen2 is the next likely reuse
 * case, hence the abstraction lives in this package rather than inside the GCS class.
 *
 * <p>The template method {@link #prepareLocations(List)} drives the pipeline: filter by scheme,
 * resolve folder hierarchies, group by bucket, and delegate to {@link
 * #createFoldersForBucket(String, List)}.
 */
public abstract class HierarchicalFolderLocationPreparer implements StorageLocationPreparer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(HierarchicalFolderLocationPreparer.class);
  private static final Predicate<String> NOT_BLANK = Predicate.not(String::isEmpty);
  private static final String PATH_SEPARATOR = "/";
  private static final String TRAILING_SLASHES_REGEX = "/+$";

  /** Represents a single folder that needs to be created, with its bucket and path. */
  protected record FolderToCreate(String bucketName, String folderPath) {}

  /** Returns the URI scheme prefix for this storage type (e.g. {@code "gs://"} for GCS). */
  protected abstract String schemePrefix();

  /**
   * Creates the given folders in the specified bucket. Called once per unique bucket after
   * grouping. Implementations must be idempotent against folders that already exist.
   */
  protected abstract void createFoldersForBucket(String bucketName, List<FolderToCreate> folders);

  @Override
  public void prepareLocations(List<String> storageLocations) {
    List<FolderToCreate> allFolders =
        storageLocations.stream()
            .filter(loc -> loc != null && loc.startsWith(schemePrefix()))
            .flatMap(loc -> extractFoldersFromPath(loc).stream())
            .distinct()
            .toList();

    if (allFolders.isEmpty()) {
      return;
    }

    allFolders.stream()
        .collect(Collectors.groupingBy(FolderToCreate::bucketName))
        .forEach(this::createFoldersForBucket);
  }

  protected List<FolderToCreate> extractFoldersFromPath(String storagePath) {
    String bucketName = StorageUtil.getBucket(storagePath);
    if (bucketName == null || bucketName.isEmpty()) {
      LOGGER
          .atWarn()
          .addKeyValue("path", storagePath)
          .log("Could not extract bucket name from storage path; skipping folder preparation");
      return List.of();
    }

    String objectPath = extractObjectPath(storagePath);
    if (objectPath == null || objectPath.isEmpty()) {
      return List.of();
    }

    return buildPathHierarchy(objectPath).stream()
        .map(folder -> new FolderToCreate(bucketName, folder))
        .toList();
  }

  /** Strips the scheme, bucket, and slashes from a storage URI, returning the object path. */
  protected static String extractObjectPath(String storageUri) {
    return Optional.of(URI.create(storageUri))
        .map(URI::getPath)
        .filter(NOT_BLANK)
        .filter(path -> !PATH_SEPARATOR.equals(path))
        .map(path -> path.startsWith(PATH_SEPARATOR) ? path.substring(1) : path)
        .map(path -> path.replaceAll(TRAILING_SLASHES_REGEX, ""))
        .filter(NOT_BLANK)
        .orElse("");
  }

  /**
   * Builds the parent hierarchy for an object path: hierarchical storage systems require every
   * parent folder to exist before a child can be created.
   *
   * <p>For example, {@code "warehouse/ns1/table1"} produces {@code ["warehouse", "warehouse/ns1",
   * "warehouse/ns1/table1"]}.
   */
  public static List<String> buildPathHierarchy(String objectPath) {
    String[] segments =
        Arrays.stream(objectPath.split(PATH_SEPARATOR)).filter(NOT_BLANK).toArray(String[]::new);

    if (segments.length == 0) {
      return List.of();
    }

    return IntStream.range(1, segments.length + 1)
        .mapToObj(i -> String.join(PATH_SEPARATOR, Arrays.copyOf(segments, i)))
        .toList();
  }
}
