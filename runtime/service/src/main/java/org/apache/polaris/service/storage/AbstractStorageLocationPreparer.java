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
import org.apache.polaris.core.storage.StorageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for storage location preparers that handles URI parsing, folder hierarchy resolution,
 * and bucket-level grouping. Subclasses implement the storage-specific folder creation logic.
 *
 * <p>The template method {@link #prepareLocations(List)} drives the pipeline: filter by scheme,
 * resolve folder hierarchies, group by bucket, and delegate to {@link
 * #createFoldersForBucket(String, List)} for storage-specific operations.
 */
public abstract class AbstractStorageLocationPreparer implements StorageLocationPreparer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractStorageLocationPreparer.class);
  private static final Predicate<String> NOT_BLANK = Predicate.not(String::isEmpty);
  private static final String PATH_SEPARATOR = "/";
  private static final String TRAILING_SLASHES_REGEX = "/+$";

  /** Represents a single folder that needs to be created, with its bucket and path. */
  protected record FolderToCreate(String bucketName, String folderPath) {}

  /** Returns the URI scheme prefix for this storage type (e.g., {@code "gs://"} for GCS). */
  protected abstract String schemePrefix();

  /**
   * Creates the given folders in the specified bucket. Called once per unique bucket after
   * grouping. Implementations should handle idempotency (e.g., folders that already exist).
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

  /**
   * Extracts all folders that need to be created for a given storage path, building the full parent
   * hierarchy since hierarchical storage systems require every parent to exist before a child can
   * be created.
   */
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

  /** Extracts the object path from a storage URI, stripping the scheme, bucket, and slashes. */
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
   * Builds the full parent hierarchy for an object path. Hierarchical storage systems require every
   * parent folder to exist before a child folder can be created.
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
