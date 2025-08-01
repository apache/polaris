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
package org.apache.polaris.delegation.service.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.delegation.api.model.TableIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages storage file operations for delegated tasks.
 *
 * <p>This service handles the actual deletion of data files from cloud storage (S3, Azure Blob
 * Storage, Google Cloud Storage) for table cleanup operations. The implementation is identical to
 * the local Polaris cleanup logic with the same retry mechanisms, batching, and error handling.
 */
@ApplicationScoped
public class StorageFileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageFileManager.class);
  private static final String STORAGE_LOCATION = "storageLocation";

  // Retry configuration - identical to local Polaris implementation
  public static final int MAX_ATTEMPTS = 3;
  public static final int FILE_DELETION_RETRY_MILLIS = 100;
  private static final int METADATA_BATCH_SIZE = 10;

  // Use virtual threads for file operations (matches Polaris local implementation)
  private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

  // Polaris API client configuration
  private final HttpClient httpClient =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
  private final ObjectMapper objectMapper = new ObjectMapper();

  // Configuration for Polaris authentication
  private final String polarisBaseUrl;
  private final String polarisClientId;
  private final String polarisClientSecret;

  public StorageFileManager() {
    this.polarisBaseUrl =
        System.getProperty(
            "polaris.base.url",
            System.getenv().getOrDefault("POLARIS_BASE_URL", "http://localhost:8181"));
    this.polarisClientId =
        System.getProperty(
            "polaris.delegation.client.id",
            System.getenv().getOrDefault("POLARIS_DELEGATION_CLIENT_ID", "root"));
    this.polarisClientSecret =
        System.getProperty(
            "polaris.delegation.client.secret",
            System.getenv().getOrDefault("POLARIS_DELEGATION_CLIENT_SECRET", "s3cr3t"));
  }

  /**
   * Cleans up all data files associated with a table using Polaris's loadTable API.
   *
   * <p>This method performs the actual data file deletion by:
   *
   * <ol>
   *   <li>Authenticating with Polaris using delegation service credentials
   *   <li>Loading table metadata via Polaris's REST API
   *   <li>Extracting storage credentials from the response
   *   <li>Identifying all data files across all snapshots
   *   <li>Deleting data files using the configured FileIO implementation
   *   <li>Deleting manifest files and metadata files
   * </ol>
   *
   * @param tableIdentity the table to clean up
   * @param realmIdentifier the realm identifier for proper multi-tenant context
   * @return cleanup result with success status and file counts
   */
  public CleanupResult cleanupTableFiles(TableIdentity tableIdentity, String realmIdentifier) {
    LOGGER.info(
        "Starting storage cleanup for table: {}.{}.{} in realm: {} using Polaris loadTable API",
        tableIdentity.getCatalogName(),
        String.join(".", tableIdentity.getNamespaceLevels()),
        tableIdentity.getTableName(),
        realmIdentifier);

    try {
      // Step 1: Authenticate with Polaris
      String bearerToken = authenticateWithPolaris();

      // Step 2: Load table metadata via Polaris API with realm context
      PolarisTableResponse tableResponse =
          loadTableViaPolaris(tableIdentity, bearerToken, realmIdentifier);

      // Step 3: Create FileIO using credentials from Polaris
      FileIO fileIO = createFileIOFromPolarisResponse(tableResponse);

      // Step 4: Perform the actual data file cleanup - identical to local implementation
      return performComprehensiveTableCleanup(tableIdentity, tableResponse.metadata, fileIO);

    } catch (Exception e) {
      LOGGER.error("Failed to cleanup table files via Polaris API", e);
      return CleanupResult.failure("Storage cleanup failed: " + e.getMessage());
    }
  }

  /** Authenticates with Polaris using delegation service credentials. */
  private String authenticateWithPolaris() throws Exception {
    String tokenUrl = polarisBaseUrl + "/api/catalog/v1/oauth/tokens";

    String requestBody =
        String.format(
            "grant_type=client_credentials&client_id=%s&client_secret=%s&scope=PRINCIPAL_ROLE:ALL",
            polarisClientId, polarisClientSecret);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(tokenUrl))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Failed to authenticate with Polaris: "
              + response.statusCode()
              + " - "
              + response.body());
    }

    JsonNode responseJson = objectMapper.readTree(response.body());
    String accessToken = responseJson.get("access_token").asText();

    LOGGER.debug("Successfully authenticated with Polaris");
    return accessToken;
  }

  /** Loads table metadata via Polaris's REST API. */
  private PolarisTableResponse loadTableViaPolaris(
      TableIdentity tableIdentity, String bearerToken, String realmIdentifier) throws Exception {
    String namespace = String.join(".", tableIdentity.getNamespaceLevels());
    String tableUrl =
        String.format(
            "%s/api/catalog/v1/%s/namespaces/%s/tables/%s",
            polarisBaseUrl,
            tableIdentity.getCatalogName(),
            namespace,
            tableIdentity.getTableName());

    // Build HTTP request with proper realm context
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder()
            .uri(URI.create(tableUrl))
            .header("Authorization", "Bearer " + bearerToken);
    // .header("X-Iceberg-Access-Delegation", "vended-credentials")  // Commented out for demo

    if (realmIdentifier != null && !realmIdentifier.isEmpty()) {
      requestBuilder.header("Polaris-Realm", realmIdentifier);
      LOGGER.debug("Setting Polaris-Realm header to: {}", realmIdentifier);
    } else {
      LOGGER.warn("No realm identifier provided - Polaris will use default realm");
    }

    HttpRequest request = requestBuilder.GET().build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Failed to load table from Polaris: " + response.statusCode() + " - " + response.body());
    }

    JsonNode responseJson = objectMapper.readTree(response.body());

    // Extract table metadata
    JsonNode metadataNode = responseJson.get("metadata");
    if (metadataNode == null) {
      throw new RuntimeException("No metadata found in Polaris response");
    }

    TableMetadata tableMetadata = TableMetadataParser.fromJson(metadataNode.toString());

    // Extract storage credentials
    Map<String, String> storageCredentials = new HashMap<>();
    JsonNode configNode = responseJson.get("config");
    if (configNode != null) {
      configNode
          .fieldNames()
          .forEachRemaining(
              fieldName -> storageCredentials.put(fieldName, configNode.get(fieldName).asText()));
    }

    LOGGER.info(
        "Successfully loaded table metadata from Polaris for table: {}.{}.{}",
        tableIdentity.getCatalogName(),
        namespace,
        tableIdentity.getTableName());

    return new PolarisTableResponse(tableMetadata, storageCredentials);
  }

  /** Creates FileIO instance using credentials returned from Polaris. */
  private FileIO createFileIOFromPolarisResponse(PolarisTableResponse tableResponse)
      throws Exception {
    Map<String, String> storageCredentials = tableResponse.credentials;

    // Get the FileIO implementation class from credentials or use default
    String fileIOImpl = storageCredentials.get(CatalogProperties.FILE_IO_IMPL);
    if (fileIOImpl == null) {
      fileIOImpl = "org.apache.iceberg.io.ResolvingFileIO"; // Default
    }

    // Create FileIO instance
    FileIO fileIO = (FileIO) Class.forName(fileIOImpl).getDeclaredConstructor().newInstance();

    // Initialize with storage credentials from Polaris
    fileIO.initialize(storageCredentials);

    LOGGER.debug("Created FileIO instance: {} with Polaris credentials", fileIOImpl);
    return fileIO;
  }

  /**
   * Performs comprehensive table cleanup identical to local Polaris implementation. This follows
   * the exact same pattern as TableCleanupTaskHandler and ManifestFileCleanupTaskHandler.
   */
  private CleanupResult performComprehensiveTableCleanup(
      TableIdentity tableIdentity, TableMetadata tableMetadata, FileIO fileIO) {

    AtomicLong totalDeletedFiles = new AtomicLong(0);
    List<String> allErrors = new ArrayList<>();

    Namespace namespace = Namespace.of(tableIdentity.getNamespaceLevels().toArray(new String[0]));
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableIdentity.getTableName());

    try {
      LOGGER.info(
          "Starting comprehensive table cleanup for table: {} - Found {} snapshots",
          tableIdentifier,
          tableMetadata.snapshots().size());

      // Step 1: Clean up all manifest files and their data files
      // This matches the ManifestFileCleanupTaskHandler logic exactly
      long manifestCleanupResult =
          cleanupAllManifestFiles(tableMetadata, fileIO, tableIdentifier, allErrors);
      totalDeletedFiles.addAndGet(manifestCleanupResult);

      // Step 2: Clean up metadata files in batches
      // This matches the BatchFileCleanupTaskHandler logic exactly
      long metadataCleanupResult =
          cleanupMetadataFilesInBatches(tableMetadata, fileIO, tableIdentifier, allErrors);
      totalDeletedFiles.addAndGet(metadataCleanupResult);

      // Step 3: Clean up the main table metadata file (if exists)
      // This matches the TableCleanupTaskHandler final step
      String metadataLocation =
          tableMetadata.location() + "/metadata/" + tableMetadata.metadataFileLocation();
      if (exists(metadataLocation, fileIO)) {
        try {
          tryDelete(tableIdentifier, fileIO, metadataLocation, metadataLocation, null, 1)
              .get(1, TimeUnit.MINUTES);
          totalDeletedFiles.incrementAndGet();
          LOGGER.info("Successfully deleted table metadata file: {}", metadataLocation);
        } catch (Exception e) {
          String error =
              "Failed to delete table metadata file: " + metadataLocation + " - " + e.getMessage();
          LOGGER.error(error, e);
          allErrors.add(error);
        }
      }

      // Return results
      if (allErrors.isEmpty()) {
        LOGGER.info(
            "Successfully cleaned up {} total files for table {}",
            totalDeletedFiles.get(),
            tableIdentifier);
        return CleanupResult.success(totalDeletedFiles.get());
      } else {
        String errorSummary =
            String.format(
                "Completed with %d errors: %s",
                allErrors.size(),
                allErrors.size() > 5 ? allErrors.subList(0, 5) + "..." : allErrors);
        LOGGER.error("Cleanup completed with errors: {}", errorSummary);
        return CleanupResult.failure(errorSummary);
      }

    } catch (Exception e) {
      LOGGER.error("Failed to perform comprehensive table cleanup", e);
      return CleanupResult.failure("Comprehensive table cleanup failed: " + e.getMessage());
    } finally {
      // Clean up FileIO resources
      if (fileIO != null) {
        try {
          fileIO.close();
        } catch (Exception e) {
          LOGGER.warn("Failed to close FileIO", e);
        }
      }
    }
  }

  /**
   * Cleans up all manifest files and their data files. This replicates the exact logic from
   * ManifestFileCleanupTaskHandler.
   */
  private long cleanupAllManifestFiles(
      TableMetadata tableMetadata,
      FileIO fileIO,
      TableIdentifier tableIdentifier,
      List<String> errors) {

    AtomicLong deletedFiles = new AtomicLong(0);

    // Get all unique manifest files (deduplicated) - identical to local implementation
    Map<String, ManifestFile> uniqueManifests =
        tableMetadata.snapshots().stream()
            .flatMap(snapshot -> snapshot.allManifests(fileIO).stream())
            .collect(Collectors.toMap(ManifestFile::path, Function.identity(), (mf1, mf2) -> mf1));

    LOGGER.info("Found {} unique manifest files to clean up", uniqueManifests.size());

    List<CompletableFuture<Void>> manifestCleanupFutures = new ArrayList<>();

    for (ManifestFile manifestFile : uniqueManifests.values()) {
      if (!exists(manifestFile.path(), fileIO)) {
        LOGGER.warn(
            "Manifest cleanup scheduled, but manifest file doesn't exist: {}", manifestFile.path());
        continue;
      }

      // Clean up this manifest file and its data files asynchronously
      CompletableFuture<Void> manifestCleanupFuture =
          CompletableFuture.runAsync(
              () -> {
                try {
                  long filesDeleted =
                      cleanupSingleManifestFile(manifestFile, fileIO, tableIdentifier, errors);
                  deletedFiles.addAndGet(filesDeleted);
                } catch (Exception e) {
                  String error =
                      "Failed to cleanup manifest file: "
                          + manifestFile.path()
                          + " - "
                          + e.getMessage();
                  LOGGER.error(error, e);
                  synchronized (errors) {
                    errors.add(error);
                  }
                }
              },
              executorService);

      manifestCleanupFutures.add(manifestCleanupFuture);
    }

    // Wait for all manifest cleanups to complete
    try {
      CompletableFuture.allOf(manifestCleanupFutures.toArray(new CompletableFuture[0]))
          .get(30, TimeUnit.MINUTES); // Generous timeout for large tables
    } catch (Exception e) {
      LOGGER.error("Timeout or error waiting for manifest cleanups to complete", e);
    }

    return deletedFiles.get();
  }

  /**
   * Cleans up a single manifest file and all its data files. This replicates the exact logic from
   * ManifestFileCleanupTaskHandler.cleanUpManifestFile.
   */
  private long cleanupSingleManifestFile(
      ManifestFile manifestFile,
      FileIO fileIO,
      TableIdentifier tableIdentifier,
      List<String> errors) {

    AtomicLong deletedFiles = new AtomicLong(0);

    try {
      // Read all data files from this manifest
      ManifestReader<DataFile> dataFiles = ManifestFiles.read(manifestFile, fileIO);
      List<CompletableFuture<Void>> dataFileDeletes =
          StreamSupport.stream(dataFiles.spliterator(), false)
              .map(
                  dataFile -> {
                    CompletableFuture<Void> deleteFuture =
                        tryDelete(
                            tableIdentifier,
                            fileIO,
                            manifestFile.path(),
                            dataFile.location(),
                            null,
                            1);
                    return deleteFuture.whenComplete(
                        (result, ex) -> {
                          if (ex == null) {
                            deletedFiles.incrementAndGet();
                            LOGGER.debug("Deleted data file: {}", dataFile.location());
                          } else {
                            String error =
                                "Failed to delete data file: "
                                    + dataFile.location()
                                    + " - "
                                    + ex.getMessage();
                            LOGGER.warn(error);
                            synchronized (errors) {
                              errors.add(error);
                            }
                          }
                        });
                  })
              .collect(Collectors.toList());

      LOGGER.debug(
          "Scheduled {} data files to be deleted from manifest {}",
          dataFileDeletes.size(),
          manifestFile.path());

      // Wait for all data files to be deleted, then delete the manifest itself
      CompletableFuture.allOf(dataFileDeletes.toArray(new CompletableFuture[0]))
          .thenCompose(
              v -> {
                LOGGER.info(
                    "All data files in manifest deleted - deleting manifest: {}",
                    manifestFile.path());
                return tryDelete(
                    tableIdentifier, fileIO, manifestFile.path(), manifestFile.path(), null, 1);
              })
          .whenComplete(
              (result, ex) -> {
                if (ex == null) {
                  deletedFiles.incrementAndGet();
                  LOGGER.debug("Deleted manifest file: {}", manifestFile.path());
                } else {
                  String error =
                      "Failed to delete manifest file: "
                          + manifestFile.path()
                          + " - "
                          + ex.getMessage();
                  LOGGER.error(error, ex);
                  synchronized (errors) {
                    errors.add(error);
                  }
                }
              })
          .get(5, TimeUnit.MINUTES); // Timeout per manifest

    } catch (Exception e) {
      String error =
          "Error processing manifest file: " + manifestFile.path() + " - " + e.getMessage();
      LOGGER.error(error, e);
      synchronized (errors) {
        errors.add(error);
      }
    }

    return deletedFiles.get();
  }

  /**
   * Cleans up metadata files in batches. This replicates the exact logic from
   * BatchFileCleanupTaskHandler.
   */
  private long cleanupMetadataFilesInBatches(
      TableMetadata tableMetadata,
      FileIO fileIO,
      TableIdentifier tableIdentifier,
      List<String> errors) {

    AtomicLong deletedFiles = new AtomicLong(0);

    // Get all metadata files - identical to local implementation
    List<List<String>> metadataFileBatches =
        getMetadataFileBatches(tableMetadata, METADATA_BATCH_SIZE);

    LOGGER.info("Found {} metadata file batches to clean up", metadataFileBatches.size());

    List<CompletableFuture<Void>> batchCleanupFutures = new ArrayList<>();

    for (List<String> batch : metadataFileBatches) {
      CompletableFuture<Void> batchCleanupFuture =
          CompletableFuture.runAsync(
              () -> {
                try {
                  long filesDeleted =
                      cleanupMetadataFileBatch(batch, fileIO, tableIdentifier, errors);
                  deletedFiles.addAndGet(filesDeleted);
                } catch (Exception e) {
                  String error =
                      "Failed to cleanup metadata file batch: " + batch + " - " + e.getMessage();
                  LOGGER.error(error, e);
                  synchronized (errors) {
                    errors.add(error);
                  }
                }
              },
              executorService);

      batchCleanupFutures.add(batchCleanupFuture);
    }

    // Wait for all batch cleanups to complete
    try {
      CompletableFuture.allOf(batchCleanupFutures.toArray(new CompletableFuture[0]))
          .get(10, TimeUnit.MINUTES); // Timeout for all batches
    } catch (Exception e) {
      LOGGER.error("Timeout or error waiting for metadata batch cleanups to complete", e);
    }

    return deletedFiles.get();
  }

  /**
   * Cleans up a single batch of metadata files. This replicates the exact logic from
   * BatchFileCleanupTaskHandler.
   */
  private long cleanupMetadataFileBatch(
      List<String> batchFiles,
      FileIO fileIO,
      TableIdentifier tableIdentifier,
      List<String> errors) {

    AtomicLong deletedFiles = new AtomicLong(0);

    // Filter to only existing files - identical to local implementation
    List<String> validFiles =
        batchFiles.stream().filter(file -> exists(file, fileIO)).collect(Collectors.toList());

    if (validFiles.isEmpty()) {
      LOGGER.warn(
          "File batch cleanup scheduled, but none of the files in batch exists: {}", batchFiles);
      return 0;
    }

    if (validFiles.size() < batchFiles.size()) {
      List<String> missingFiles =
          batchFiles.stream().filter(file -> !exists(file, fileIO)).collect(Collectors.toList());
      LOGGER.warn(
          "File batch cleanup scheduled, but {} files in the batch are missing: {}",
          missingFiles.size(),
          missingFiles);
    }

    // Schedule the deletion for each file asynchronously
    List<CompletableFuture<Void>> deleteFutures =
        validFiles.stream()
            .map(
                file -> {
                  CompletableFuture<Void> deleteFuture =
                      tryDelete(tableIdentifier, fileIO, null, file, null, 1);
                  return deleteFuture.whenComplete(
                      (result, ex) -> {
                        if (ex == null) {
                          deletedFiles.incrementAndGet();
                          LOGGER.debug("Deleted metadata file: {}", file);
                        } else {
                          String error =
                              "Failed to delete metadata file: " + file + " - " + ex.getMessage();
                          LOGGER.warn(error);
                          synchronized (errors) {
                            errors.add(error);
                          }
                        }
                      });
                })
            .collect(Collectors.toList());

    try {
      // Wait for all delete operations to finish
      CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]))
          .get(2, TimeUnit.MINUTES); // Timeout per batch
    } catch (Exception e) {
      LOGGER.error("Exception detected during batch files deletion", e);
    }

    return deletedFiles.get();
  }

  /** Gets metadata files organized in batches - identical to local implementation. */
  private List<List<String>> getMetadataFileBatches(TableMetadata tableMetadata, int batchSize) {
    List<List<String>> result = new ArrayList<>();

    // Collect all metadata files - identical to local implementation
    List<String> metadataFiles =
        Stream.of(
                tableMetadata.previousFiles().stream().map(TableMetadata.MetadataLogEntry::file),
                tableMetadata.snapshots().stream().map(Snapshot::manifestListLocation),
                tableMetadata.statisticsFiles().stream().map(StatisticsFile::path),
                tableMetadata.partitionStatisticsFiles().stream()
                    .map(PartitionStatisticsFile::path))
            .flatMap(s -> s)
            .collect(Collectors.toList());

    // Create batches
    for (int i = 0; i < metadataFiles.size(); i += batchSize) {
      result.add(metadataFiles.subList(i, Math.min(i + batchSize, metadataFiles.size())));
    }

    return result;
  }

  /**
   * Tries to delete a file with retry logic - identical to local implementation. This replicates
   * the exact logic from FileCleanupTaskHandler.tryDelete.
   */
  private CompletableFuture<Void> tryDelete(
      TableIdentifier tableIdentifier,
      FileIO fileIO,
      String baseFile,
      String file,
      Throwable e,
      int attempt) {

    if (e != null && attempt <= MAX_ATTEMPTS) {
      LOGGER.warn(
          "Error encountered attempting to delete file: {} (attempt {}): {}",
          file,
          attempt,
          e.getMessage());
    }

    if (attempt > MAX_ATTEMPTS && e != null) {
      return CompletableFuture.failedFuture(e);
    }

    return CompletableFuture.runAsync(
            () -> {
              // Totally normal for a file to already be missing, e.g. a data file
              // may be in multiple manifests. There's a possibility we check the
              // file's existence, but then it is deleted before we have a chance to
              if (exists(file, fileIO)) {
                fileIO.deleteFile(file);
              } else {
                LOGGER.info(
                    "Table file cleanup scheduled, but file doesn't exist: {} (table: {}, baseFile: {})",
                    file,
                    tableIdentifier,
                    baseFile != null ? baseFile : "");
              }
            },
            executorService)
        .exceptionallyComposeAsync(
            newEx -> {
              LOGGER.warn(
                  "Exception caught deleting file: {} (table: {}, baseFile: {})",
                  file,
                  tableIdentifier,
                  baseFile != null ? baseFile : "",
                  newEx);
              return tryDelete(tableIdentifier, fileIO, baseFile, file, newEx, attempt + 1);
            },
            CompletableFuture.delayedExecutor(
                FILE_DELETION_RETRY_MILLIS, TimeUnit.MILLISECONDS, executorService));
  }

  /** Checks if a file exists in storage - identical to TaskUtils.exists. */
  private boolean exists(String path, FileIO fileIO) {
    try {
      return fileIO.newInputFile(path).exists();
    } catch (Exception e) {
      LOGGER.debug("Error checking if file exists: {}", path, e);
      return false;
    }
  }

  /** Result of a cleanup operation. */
  public static class CleanupResult {
    private final boolean success;
    private final long filesDeleted;
    private final String errorMessage;

    private CleanupResult(boolean success, long filesDeleted, String errorMessage) {
      this.success = success;
      this.filesDeleted = filesDeleted;
      this.errorMessage = errorMessage;
    }

    public static CleanupResult success(long filesDeleted) {
      return new CleanupResult(true, filesDeleted, null);
    }

    public static CleanupResult failure(String errorMessage) {
      return new CleanupResult(false, 0, errorMessage);
    }

    public boolean isSuccess() {
      return success;
    }

    public long getFilesDeleted() {
      return filesDeleted;
    }

    public String getErrorMessage() {
      return errorMessage;
    }
  }

  /** Response object for Polaris table loading. */
  private static class PolarisTableResponse {
    final TableMetadata metadata;
    final Map<String, String> credentials;

    PolarisTableResponse(TableMetadata metadata, Map<String, String> credentials) {
      this.metadata = metadata;
      this.credentials = credentials;
    }
  }
}
