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
package org.apache.polaris.service.storage.gcs;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.storage.control.v2.BucketName;
import com.google.storage.control.v2.CreateFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.StorageControlSettings;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.TableProperties;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo.StorageType;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.service.storage.StorageLocationPreparer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GCS implementation of {@link StorageLocationPreparer}. Detects whether the target bucket uses
 * Hierarchical Namespace (HNS) and, if so, creates the full folder hierarchy required before
 * Iceberg metadata and data files can be written. For flat-namespace buckets, this is a no-op.
 *
 * <p>The main flow uses an {@link Optional} pipeline pattern to replace imperative guard clauses
 * with a declarative chain: parse location -> resolve HNS status -> create folders.
 */
public class GcsStorageLocationPreparer implements StorageLocationPreparer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GcsStorageLocationPreparer.class);
  private static final String GCS_SCHEME_PREFIX = StorageType.GCS.getPrefixes().getFirst();
  private static final Predicate<String> NOT_BLANK = Predicate.not(String::isEmpty);
  private static final String PATH_SEPARATOR = "/";
  private static final String TRAILING_SLASHES_REGEX = "/+$";
  private static final String DEFAULT_METADATA_FOLDER = "metadata";
  private static final String DEFAULT_DATA_FOLDER = "data";
  private static final String GCS_STORAGE_LAYOUT = "_";

  private final Supplier<GoogleCredentials> credentialsSupplier;

  /** Represents a single folder that needs to be created, with its bucket and path. */
  private record FolderToCreate(String bucketName, String folderPath) {}

  public GcsStorageLocationPreparer(Supplier<GoogleCredentials> credentialsSupplier) {
    this.credentialsSupplier = credentialsSupplier;
  }

  // ── Main Pipeline ──────────────────────────────────────────────────────────

  @Override
  public void prepareTableLocation(String tableLocation, Map<String, String> tableProperties) {
    if (tableLocation == null || !tableLocation.startsWith(GCS_SCHEME_PREFIX)) {
      return;
    }

    List<FolderToCreate> allFolders = resolveAllFoldersToCreate(tableLocation, tableProperties);
    if (allFolders.isEmpty()) {
      return;
    }

    // Group folders by bucket and create only for HNS-enabled buckets
    allFolders.stream()
        .collect(Collectors.groupingBy(FolderToCreate::bucketName))
        .forEach(this::createFoldersForBucketIfHnsEnabled);
  }

  // ── Multi-Bucket Folder Resolution ────────────────────────────────────────

  /**
   * Resolves all folders that need to be created across all buckets (table location + custom
   * metadata/data paths). Each folder is tagged with its bucket for HNS checking and creation.
   */
  private List<FolderToCreate> resolveAllFoldersToCreate(
      String tableLocation, Map<String, String> tableProperties) {
    List<String> allPaths = resolveAllPaths(tableLocation, tableProperties);
    List<FolderToCreate> allFolders = new ArrayList<>();

    // Table location gets full hierarchy
    allFolders.addAll(extractFoldersFromPath(tableLocation, true));

    // Metadata and data paths get just their own folder
    String metadataPath = allPaths.get(1); // Second element is metadata path
    String dataPath = allPaths.get(2); // Third element is data path

    if (!metadataPath.equals(tableLocation + PATH_SEPARATOR + DEFAULT_METADATA_FOLDER)) {
      // Custom metadata path - create just this folder
      allFolders.addAll(extractFoldersFromPath(metadataPath, false));
    }

    if (!dataPath.equals(tableLocation + PATH_SEPARATOR + DEFAULT_DATA_FOLDER)) {
      // Custom data path - create just this folder
      allFolders.addAll(extractFoldersFromPath(dataPath, false));
    }

    return allFolders;
  }

  /**
   * Resolves all storage paths: table location + metadata path + data path. Custom write paths from
   * table properties take precedence over defaults.
   */
  private List<String> resolveAllPaths(String tableLocation, Map<String, String> tableProperties) {
    String metadataPath =
        Optional.ofNullable(tableProperties.get(TableProperties.WRITE_METADATA_LOCATION))
            .or(
                () ->
                    Optional.ofNullable(
                        tableProperties.get(
                            IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY)))
            .orElse(tableLocation + PATH_SEPARATOR + DEFAULT_METADATA_FOLDER);

    String dataPath =
        Optional.ofNullable(
                tableProperties.get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY))
            .orElse(tableLocation + PATH_SEPARATOR + DEFAULT_DATA_FOLDER);

    return List.of(tableLocation, metadataPath, dataPath);
  }

  /**
   * Extracts all folders that need to be created for a given GCS path.
   *
   * @param gcsPath the GCS path to process
   * @param includeHierarchy if true, creates full hierarchy; if false, creates just the path itself
   */
  private List<FolderToCreate> extractFoldersFromPath(String gcsPath, boolean includeHierarchy) {
    // Only process GCS paths
    if (!gcsPath.startsWith(GCS_SCHEME_PREFIX)) {
      return List.of();
    }

    String bucketName = StorageUtil.getBucket(gcsPath);
    if (bucketName == null || bucketName.isEmpty()) {
      LOGGER
          .atWarn()
          .addKeyValue("path", gcsPath)
          .log("Could not extract bucket name from GCS path; skipping folder preparation");
      return List.of();
    }

    String objectPath = extractObjectPath(gcsPath);
    if (objectPath == null || objectPath.isEmpty()) {
      return List.of(); // Bucket-only path, no folders to create
    }

    if (includeHierarchy) {
      // Create full hierarchy for table location (includes metadata/ and data/ subfolders)
      return buildTableHierarchy(objectPath).stream()
          .map(folder -> new FolderToCreate(bucketName, folder))
          .toList();
    } else {
      // Custom paths still need full parent hierarchy on HNS buckets
      return buildPathHierarchy(objectPath).stream()
          .map(folder -> new FolderToCreate(bucketName, folder))
          .toList();
    }
  }

  private String extractObjectPath(String gcsUri) {
    return Optional.of(URI.create(gcsUri))
        .map(URI::getPath)
        .filter(NOT_BLANK)
        .filter(path -> !PATH_SEPARATOR.equals(path))
        .map(path -> path.startsWith(PATH_SEPARATOR) ? path.substring(1) : path)
        .map(path -> path.replaceAll(TRAILING_SLASHES_REGEX, ""))
        .filter(NOT_BLANK)
        .orElse("");
  }

  /** Creates folders for all paths in the bucket, but only if the bucket is HNS-enabled. */
  private void createFoldersForBucketIfHnsEnabled(String bucketName, List<FolderToCreate> folders) {
    boolean hnsEnabled = resolveHnsStatus(bucketName);
    LOGGER
        .atInfo()
        .addKeyValue("bucket", bucketName)
        .addKeyValue("hnsEnabled", hnsEnabled)
        .addKeyValue("folderCount", folders.size())
        .log("GCS bucket HNS status for table creation");

    if (hnsEnabled) {
      createFoldersInBucket(bucketName, folders);
    }
  }

  /** Creates all folders in a bucket using a single StorageControlClient instance. */
  private void createFoldersInBucket(String bucketName, List<FolderToCreate> folders) {
    try (StorageControlClient controlClient = createStorageControlClient()) {
      String parent = BucketName.format(GCS_STORAGE_LAYOUT, bucketName);
      folders.forEach(
          folder -> createFolderIfNotExists(controlClient, parent, folder.folderPath()));
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Failed to initialize StorageControlClient for HNS folder creation in bucket '%s'",
              bucketName),
          e);
    } catch (RuntimeException e) {
      // Check if this is a gRPC/networking configuration issue
      if (isGrpcConfigurationError(e)) {
        LOGGER
            .atWarn()
            .addKeyValue("bucket", bucketName)
            .addKeyValue("folderCount", folders.size())
            .addKeyValue("error", e.getMessage())
            .log(
                "Failed to create HNS folders due to gRPC/networking configuration issue; table creation may fail if folders don't exist",
                e);

        // Don't throw - allow table creation to proceed and potentially fail later with a clearer
        // error
        // This prevents blocking all table operations due to infrastructure issues
      } else {
        // Re-throw other runtime exceptions (like actual folder creation failures)
        throw e;
      }
    }
  }

  /**
   * Checks if the exception is related to gRPC configuration issues that should be handled
   * gracefully.
   */
  private boolean isGrpcConfigurationError(RuntimeException e) {
    String message = e.getMessage();
    Throwable cause = e.getCause();

    // Check for known gRPC configuration issues
    return (message != null
            && (message.contains("census")
                || message.contains("JNDI")
                || message.contains("grpc")
                || message.contains("Unable to apply census stats")))
        || (cause != null
            && (cause instanceof ClassNotFoundException
                || cause instanceof javax.naming.NamingException));
  }

  /** Builds the complete hierarchy for a table location path. Package-private for testing. */
  static List<String> buildTableHierarchy(String objectPath) {
    String[] segments =
        Arrays.stream(objectPath.split(PATH_SEPARATOR)).filter(NOT_BLANK).toArray(String[]::new);

    if (segments.length == 0) {
      return List.of();
    }

    String canonicalPath = String.join(PATH_SEPARATOR, segments);
    List<String> folders = new ArrayList<>();

    // Add all intermediate folders
    folders.addAll(
        IntStream.range(1, segments.length + 1)
            .mapToObj(i -> String.join(PATH_SEPARATOR, Arrays.copyOf(segments, i)))
            .toList());

    // Add metadata and data folders
    folders.add(canonicalPath + PATH_SEPARATOR + DEFAULT_METADATA_FOLDER);
    folders.add(canonicalPath + PATH_SEPARATOR + DEFAULT_DATA_FOLDER);

    return folders;
  }

  /**
   * Builds the full parent hierarchy for a custom path (e.g., write.metadata.path or
   * write.data.path) without appending default metadata/data subfolders. HNS buckets require every
   * parent folder to exist before a child folder can be created. Package-private for testing.
   */
  static List<String> buildPathHierarchy(String objectPath) {
    String[] segments =
        Arrays.stream(objectPath.split(PATH_SEPARATOR)).filter(NOT_BLANK).toArray(String[]::new);

    if (segments.length == 0) {
      return List.of();
    }

    return IntStream.range(1, segments.length + 1)
        .mapToObj(i -> String.join(PATH_SEPARATOR, Arrays.copyOf(segments, i)))
        .toList();
  }

  private boolean resolveHnsStatus(String bucketName) {
    try {
      Bucket bucket = fetchBucketMetadata(bucketName);
      return Optional.ofNullable(bucket.getHierarchicalNamespace())
          .map(BucketInfo.HierarchicalNamespace::getEnabled)
          .orElse(false);
    } catch (RuntimeException e) {
      throw new RuntimeException(
          String.format(
              "Failed to check HNS status for GCS bucket '%s'. "
                  + "Table creation is blocked until the bucket status can be verified.",
              bucketName),
          e);
    }
  }

  Bucket fetchBucketMetadata(String bucketName) {
    Storage storage =
        StorageOptions.newBuilder().setCredentials(credentialsSupplier.get()).build().getService();

    return Optional.ofNullable(
            storage.get(
                bucketName,
                Storage.BucketGetOption.fields(Storage.BucketField.HIERARCHICAL_NAMESPACE)))
        .orElseThrow(
            () ->
                new RuntimeException(
                    String.format(
                        "GCS bucket '%s' not found. Cannot proceed with table creation.",
                        bucketName)));
  }

  // ── HNS Folder Creation ────────────────────────────────────────────────────

  private void createFolderIfNotExists(
      StorageControlClient controlClient, String parent, String folderPath) {
    try {
      CreateFolderRequest request =
          CreateFolderRequest.newBuilder().setParent(parent).setFolderId(folderPath).build();

      controlClient.createFolder(request);
      LOGGER.atDebug().addKeyValue("folder", folderPath).log("Created HNS folder");
    } catch (AlreadyExistsException e) {
      LOGGER.atDebug().addKeyValue("folder", folderPath).log("HNS folder already exists, skipping");
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to create HNS folder '%s': %s", folderPath, e.getMessage()), e);
    }
  }

  StorageControlClient createStorageControlClient() throws IOException {
    GoogleCredentials credentials = credentialsSupplier.get();
    StorageControlSettings settings =
        StorageControlSettings.newBuilder().setCredentialsProvider(() -> credentials).build();
    return StorageControlClient.create(settings);
  }
}
