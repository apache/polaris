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

package org.apache.polaris.storage.files.impl;

import static java.lang.String.format;

import com.google.common.collect.Streams;
import com.google.common.util.concurrent.RateLimiter;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.polaris.storage.files.api.FileFilter;
import org.apache.polaris.storage.files.api.FileOperations;
import org.apache.polaris.storage.files.api.FileSpec;
import org.apache.polaris.storage.files.api.FileType;
import org.apache.polaris.storage.files.api.ImmutablePurgeStats;
import org.apache.polaris.storage.files.api.PurgeSpec;
import org.apache.polaris.storage.files.api.PurgeStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param fileIO the {@link FileIO} instance to use. The given instance must implement both {@link
 *     org.apache.iceberg.io.SupportsBulkOperations} and {@link
 *     org.apache.iceberg.io.SupportsPrefixOperations}.
 */
record FileOperationsImpl(@Nonnull FileIO fileIO) implements FileOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileOperationsImpl.class);

  @Override
  public Stream<FileSpec> findFiles(@Nonnull String prefix, @Nonnull FileFilter filter) {
    var prefixUri = StorageUri.of(prefix).resolve("/");
    if (fileIO instanceof SupportsPrefixOperations prefixOps) {
      return Streams.stream(prefixOps.listPrefix(prefix).iterator())
          .filter(Objects::nonNull)
          .map(
              fileInfo -> {
                var location = StorageUri.of(fileInfo.location());
                if (!location.isAbsolute()) {
                  // ADLSFileIO does _not_ include the prefix, but GCSFileIO and S3FileIO do.
                  location = prefixUri.resolve(location);
                }
                return FileSpec.builder()
                    .location(location.toString())
                    .size(fileInfo.size())
                    .createdAtMillis(fileInfo.createdAtMillis())
                    .build();
              })
          .filter(filter);
    }

    throw new IllegalStateException(
        format(
            "An Iceberg FileIO supporting prefix operations is required, but the given %s does not",
            fileIO.getClass().getName()));
  }

  @Override
  public Stream<FileSpec> identifyIcebergTableFiles(@Nonnull String tableMetadataLocation) {
    var optionalTableMetadata = readTableMetadataFailsafe(tableMetadataLocation);
    if (optionalTableMetadata.isEmpty()) {
      return Stream.empty();
    }
    var metadata = optionalTableMetadata.get();

    var metadataFileSpec =
        FileSpec.fromLocation(tableMetadataLocation).fileType(FileType.ICEBERG_METADATA).build();

    var fileSources = new ArrayList<Stream<FileSpec>>();

    fileSources.add(Stream.of(metadataFileSpec));

    var statisticsFiles = metadata.statisticsFiles();
    if (statisticsFiles != null) {
      fileSources.addFirst(
          statisticsFiles.stream()
              .map(
                  statisticsFile ->
                      FileSpec.fromLocationAndSize(
                              statisticsFile.path(), statisticsFile.fileSizeInBytes())
                          .fileType(FileType.ICEBERG_STATISTICS)
                          .build()));
    }

    var previousFiles = metadata.previousFiles();
    if (previousFiles != null) {
      fileSources.add(
          previousFiles.stream()
              .filter(
                  metadataLogEntry ->
                      metadataLogEntry.file() != null && !metadataLogEntry.file().isEmpty())
              .map(
                  metadataLogEntry ->
                      FileSpec.fromLocation(metadataLogEntry.file())
                          .fileType(FileType.ICEBERG_METADATA)
                          .build()));
    }

    // Explicitly "extract" 'specsById' here to "release" the reference to 'metadata'
    // so that GC is able to collect the metadata and its referenced objects as soon
    // as possible.
    var specsById = metadata.specsById();

    var processManifestFilePredicate = bestEffortDeduplicator();

    fileSources.addFirst(
        metadata.snapshots().stream()
            // Newest snapshots first
            .sorted((s1, s2) -> Long.compare(s2.timestampMillis(), s1.timestampMillis()))
            .flatMap(
                snapshot ->
                    identifyIcebergManifests(snapshot, specsById, processManifestFilePredicate)));

    // Return "dependencies" before the "metadata" itself, so the probability of being able to
    // resume a failed/aborted purge is higher.

    return fileSources.stream().flatMap(Function.identity());
  }

  private Stream<FileSpec> identifyIcebergManifests(
      Snapshot snapshot,
      Map<Integer, PartitionSpec> specsById,
      Predicate<ManifestFile> processManifestFilePredicate) {
    var manifestListLocation = snapshot.manifestListLocation();
    var manifestListFileSpecStream = Stream.<FileSpec>empty();

    if (manifestListLocation != null && !manifestListLocation.isEmpty()) {
      var manifestListFileSpec =
          FileSpec.fromLocation(manifestListLocation)
              .fileType(FileType.ICEBERG_MANIFEST_LIST)
              .build();
      manifestListFileSpecStream = Stream.of(manifestListFileSpec);
    }

    try {
      var allManifestsFiles =
          Stream.of(snapshot)
              .flatMap(
                  s ->
                      icebergTableManifestDataFilesFromSnapshot(
                          s, specsById, processManifestFilePredicate));

      // Return "dependencies" before the "metadata" itself, so a failed/aborted purge can be
      // resumed.
      return Stream.of(allManifestsFiles, manifestListFileSpecStream).flatMap(Function.identity());
    } catch (Exception e) {
      LOGGER.warn("Failure reading manifest list file {}", manifestListLocation, e);
      return manifestListFileSpecStream;
    }
  }

  /**
   * When iterating over a snapshot's manifests, we do not have any information whether a specific
   * manifest file has been "seen" before.
   *
   * <p>This means we would be processing the same manifest files multiple times. The same is true
   * for data/delete files references from the manifest files. Note that newer snapshots reference
   * previous manifest files, as returned by {@link Snapshot#allManifests(FileIO)}. Since there is
   * no better way than that function, we have to either bite that bullet and re-read the same
   * manifest-files multiple times or have a somewhat working deduplication mechanism, as
   * implemented. The implemented deduplicator only considers manifest file paths.
   */
  private Predicate<ManifestFile> bestEffortDeduplicator() {
    var set = new LinkedHashSet<String>();
    return manifestFile -> {
      synchronized (set) {
        if (set.size() > 10_000) {
          // limit the heap pressure of the deduplication set to 10,000 manifest files
          set.removeFirst();
        }
        var path = manifestFile.path();
        var handle = set.add(path);
        System.err.println("TEST: " + path + " -> " + handle + "  " + System.identityHashCode(set));
        return handle;
      }
    };
  }

  private Stream<FileSpec> icebergTableManifestDataFilesFromSnapshot(
      Snapshot snapshot,
      Map<Integer, PartitionSpec> specsById,
      Predicate<ManifestFile> processManifestFilePredicate) {
    // Iceberg's org.apache.iceberg.ManifestLists is _not_ public and does not offer
    // an iterator-based approach as offered by ManifestFiles.read().

    // This unfortunately means that we can only rely on Snapshot.allManifests(),
    // which delegates to org.apache.iceberg.BaseSnapshot.cacheManifests().

    // That 'cacheManifests()' function reads the fill manifest list and keeps a
    // strong reference to the list of `ManifestFile` objects.
    // To optimize that, there are two options:
    // 1. Iceberg to offer an iterator-based approach for scanning manifest-lists.
    // 2. Implement or better use an alternative implementation, which likely exist.

    // This Stream.flatMap() body is the only and "last" place that needs the
    // (Base)Snapshot instance, the only place that populates the cached manifests,
    // once it finishes, the reference to the (Base)Snapshot instance becomes
    // eligible for garbage collection.

    var allManifests = snapshot.allManifests(fileIO);

    var allFilesStreams =
        allManifests.stream()
            .filter(processManifestFilePredicate)
            .map(mf -> Stream.of(mf).flatMap(m -> identifyIcebergManifestDataFiles(m, specsById)))
            .toList();

    return allFilesStreams.stream().flatMap(Function.identity());
  }

  @SuppressWarnings("UnnecessaryDefault")
  private Stream<FileSpec> identifyIcebergManifestDataFiles(
      ManifestFile manifestFile, Map<Integer, PartitionSpec> specsById) {

    var manifestFileSpec =
        FileSpec.fromLocationAndSize(manifestFile.path(), manifestFile.length())
            .fileType(FileType.ICEBERG_MANIFEST_FILE)
            .build();

    try (var contentFilesIter =
        switch (manifestFile.content()) {
          case DATA -> ManifestFiles.read(manifestFile, fileIO).iterator();
          case DELETES ->
              ManifestFiles.readDeleteManifest(manifestFile, fileIO, specsById).iterator();
          default -> {
            LOGGER.warn(
                "Unsupported content type {} in manifest {}",
                manifestFile.content(),
                manifestFile.path());
            yield CloseableIterator.<ContentFile<? extends ContentFile<?>>>empty();
          }
        }) {

      // Cannot leverage streaming here and eagerly build a list, as the manifest-file reader needs
      // to be closed.
      var files = new ArrayList<FileSpec>();
      while (contentFilesIter.hasNext()) {
        var contentFile = contentFilesIter.next();
        files.add(
            FileSpec.fromLocationAndSize(contentFile.location(), contentFile.fileSizeInBytes())
                .fileType(FileType.fromContentFile(contentFile))
                .build());
      }
      // Return "dependencies" before the "metadata" itself, so the probability of being able to
      // resume a failed/aborted purge is higher.
      files.add(manifestFileSpec);

      return files.stream();
    } catch (IOException e) {
      LOGGER.warn("Failure reading manifest file {}", manifestFile.path(), e);
      return Stream.of(manifestFileSpec);
    }
  }

  @Override
  public Stream<FileSpec> identifyIcebergViewFiles(@Nonnull String viewMetadataLocation) {
    var optionalViewMetadata = readViewMetadataFailsafe(viewMetadataLocation);
    if (optionalViewMetadata.isEmpty()) {
      return Stream.empty();
    }

    var metadataFileSpec =
        FileSpec.fromLocation(viewMetadataLocation).fileType(FileType.ICEBERG_METADATA).build();

    return Stream.of(metadataFileSpec);
  }

  @Override
  public PurgeStats purgeIcebergTable(@Nonnull String tableMetadataLocation, PurgeSpec purgeSpec) {
    var files = identifyIcebergTableFiles(tableMetadataLocation).filter(purgeSpec.fileFilter());
    return purge(files, purgeSpec);
  }

  @Override
  public PurgeStats purgeIcebergTableBaseLocation(
      @Nonnull String tableMetadataLocation, PurgeSpec purgeSpec) {
    var metadata = readTableMetadataFailsafe(tableMetadataLocation);
    if (metadata.isEmpty()) {
      return ImmutablePurgeStats.builder()
          .duration(Duration.ZERO)
          .purgeFileRequests(0L)
          .failedFilePurges(1)
          .build();
    }

    var baseLocation = metadata.get().location();
    var files = findFiles(baseLocation, purgeSpec.fileFilter());
    return purge(files, purgeSpec);
  }

  @Override
  public PurgeStats purgeIcebergView(@Nonnull String viewMetadataLocation, PurgeSpec purgeSpec) {
    var files = identifyIcebergViewFiles(viewMetadataLocation).filter(purgeSpec.fileFilter());
    return purge(files, purgeSpec);
  }

  @Override
  public PurgeStats purgeIcebergViewBaseLocation(
      @Nonnull String viewMetadataLocation, PurgeSpec purgeSpec) {
    var metadata = readViewMetadataFailsafe(viewMetadataLocation);
    if (metadata.isEmpty()) {
      return ImmutablePurgeStats.builder()
          .duration(Duration.ZERO)
          .purgeFileRequests(0L)
          .failedFilePurges(1)
          .build();
    }

    var baseLocation = metadata.get().location();
    var files = findFiles(baseLocation, purgeSpec.fileFilter());
    return purge(files, purgeSpec);
  }

  @Override
  public PurgeStats purge(@Nonnull Stream<FileSpec> locationStream, PurgeSpec purgeSpec) {
    if (fileIO instanceof SupportsBulkOperations bulkOps) {
      var startedNanos = System.nanoTime();

      var iter = locationStream.iterator();

      var batcher = new PurgeBatcher(purgeSpec, bulkOps);
      while (iter.hasNext()) {
        batcher.add(iter.next().location());
      }
      batcher.flush();

      return ImmutablePurgeStats.builder()
          .purgeFileRequests(batcher.purged)
          .failedFilePurges(batcher.failed)
          .duration(Duration.ofNanos(System.nanoTime() - startedNanos))
          .build();
    }

    throw new IllegalStateException(
        format(
            "An Iceberg FileIO supporting bulk operations is required, but the given %s does not",
            fileIO.getClass().getName()));
  }

  @SuppressWarnings("UnstableApiUsage")
  static final class PurgeBatcher {
    private final PurgeSpec purgeSpec;
    private final SupportsBulkOperations bulkOps;

    private final int deleteBatchSize;
    // Using a `Set` prevents duplicate paths in a single bulk-deletion.

    private final Set<String> batch = new HashSet<>();

    private final Runnable fileDeleteRateLimiter;
    private final Runnable batchDeleteRateLimiter;

    long purged = 0L;

    long failed = 0L;

    PurgeBatcher(PurgeSpec purgeSpec, SupportsBulkOperations bulkOps) {
      var implSpecificLimit = implSpecificDeleteBatchLimit(bulkOps);

      this.deleteBatchSize = Math.min(implSpecificLimit, Math.max(purgeSpec.deleteBatchSize(), 1));

      this.purgeSpec = purgeSpec;
      this.bulkOps = bulkOps;

      fileDeleteRateLimiter = createLimiter(purgeSpec.fileDeletesPerSecond());
      batchDeleteRateLimiter = createLimiter(purgeSpec.batchDeletesPerSecond());
    }

    private static Runnable createLimiter(OptionalDouble optionalDouble) {
      if (optionalDouble.isEmpty()) {
        // unlimited
        return () -> {};
      }
      var limiter = RateLimiter.create(optionalDouble.getAsDouble());
      return limiter::acquire;
    }

    void add(String location) {
      fileDeleteRateLimiter.run();
      batch.add(location);

      if (batch.size() >= deleteBatchSize) {
        flush();
      }
    }

    void flush() {
      int size = batch.size();
      if (size > 0) {
        batch.forEach(purgeSpec.purgeIssuedCallback());
        try {
          batchDeleteRateLimiter.run();
          bulkOps.deleteFiles(batch);
          purged += size;
        } catch (BulkDeletionFailureException e) {
          // Object stores do delete the files that exist, but a BulkDeletionFailureException is
          // still being thrown.
          // However, not all FileIO implementations behave the same way as some don't throw in the
          // non-existent-case.
          var batchFailed = e.numberFailedObjects();
          purged += size - batchFailed;
          failed += batchFailed;
        } finally {
          batch.clear();
        }
      }
    }
  }

  /** Figure out the hard-coded max batch size limit for a particular FileIO implementation. */
  static int implSpecificDeleteBatchLimit(SupportsBulkOperations bulkOps) {
    var className = bulkOps.getClass().getSimpleName();
    return switch (className) {
      // See https://aws.amazon.com/blogs/aws/amazon-s3-multi-object-deletion/
      case "S3FileIO" -> 1000;
      // See https://cloud.google.com/storage/docs/batch
      case "GCSFileIO" -> 100;
      // ADLS limited to 50, because the implementation, as of Iceberg 1.10, uses one thread per
      // file to be deleted (no specialized bulk deletion).
      case "ADLSFileIO" -> 50;
      // Use a reasonable(?) default for all other FileIO implementations.
      default -> 50;
    };
  }

  private Optional<TableMetadata> readTableMetadataFailsafe(String tableMetadataLocation) {
    try {
      var inputFile = fileIO.newInputFile(tableMetadataLocation);
      return Optional.of(TableMetadataParser.read(inputFile));
    } catch (Exception e) {
      LOGGER.warn("Failure reading table metadata file {}", tableMetadataLocation, e);
      return Optional.empty();
    }
  }

  private Optional<ViewMetadata> readViewMetadataFailsafe(String viewMetadataLocation) {
    try {
      var inputFile = fileIO.newInputFile(viewMetadataLocation);
      return Optional.of(ViewMetadataParser.read(inputFile));
    } catch (Exception e) {
      LOGGER.warn("Failure reading view metadata file {}", viewMetadataLocation, e);
      return Optional.empty();
    }
  }
}
