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

package org.apache.polaris.storage.files.api;

import jakarta.annotation.Nonnull;
import java.util.stream.Stream;

/**
 * Object storage file operations, used to find files below a given prefix, to purge files, to
 * identify referenced files, etc.
 *
 * <p>All functions of this interface rather yield incomplete results and continue over throwing
 * exceptions.
 */
public interface FileOperations {
  /**
   * Find files that match the given prefix and filter.
   *
   * <p>Whether existing but inaccessible files are included in the result depends on the object
   * store.
   *
   * <p>Call sites should consider rate-limiting the scan operations, for example, by using Guava's
   * {@code RateLimiter} via a {@code Stream.map(x -> { rateLimiter.acquire(); return x; }} step on
   * the returned stream.
   *
   * @param prefix full object storage URI prefix, including scheme and bucket.
   * @param filter file filter
   * @return a stream of file specs with the {@link FileSpec#createdAtMillis()} and {@link
   *     FileSpec#size()} attributes populated with the information provided by the object store.
   *     The {@link FileSpec#fileType() file type} attribute is not populated, it may be {@link
   *     FileSpec#guessTypeFromName() guessed}.
   */
  Stream<FileSpec> findFiles(@Nonnull String prefix, @Nonnull FileFilter filter);

  /**
   * Identifies all files referenced by the given table-metadata.
   *
   * <p>In case "container" files, like the metadata, manifest-list or manifest files, are not
   * readable, the returned stream will just not include those.
   *
   * <p>Rate-limiting the returned stream is recommended when identifying multiple tables and/or
   * views. Rate-limiting on a single invocation may not be effective as expected.
   *
   * @param tableMetadataLocation Iceberg table-metadata location
   * @param deduplicate if true, attempt to deduplicate files by their location, adding additional
   *     heap pressure to the operation. Implementations may ignore this parameter or may not
   *     deduplicate all identified files.
   * @return a stream of {@link FileSpec file specs}. The {@link FileSpec#createdAtMillis()}
   *     attribute is usually not populated, as it would have to be derived from user-provided
   *     information in metadata or snapshot. The {@link FileSpec#fileType()} attribute is populated
   *     based on where a file appears during identification.
   */
  Stream<FileSpec> identifyIcebergTableFiles(
      @Nonnull String tableMetadataLocation, boolean deduplicate);

  /**
   * Identifies all files referenced by the given view-metadata.
   *
   * <p>In case "container" files like the metadata are not readable, the returned stream will just
   * not include those.
   *
   * <p>Rate-limiting the returned stream is recommended when identifying multiple tables and/or
   * views. Rate-limiting on a single invocation may not be effective as expected.
   *
   * @param viewMetadataLocation Iceberg view-metadata location
   * @param deduplicate if true, attempt to deduplicate files by their location, adding additional
   *     heap pressure to the operation. Implementations may ignore this parameter or may not
   *     deduplicate all identified files.
   * @return a stream of {@link FileSpec file specs}. The {@link FileSpec#createdAtMillis()}
   *     attribute is usually not populated, as it would have been derived from user-provided
   *     information in metadata or snapshot. The {@link FileSpec#fileType()} attribute is populated
   *     based on where a file appears during identification.
   */
  Stream<FileSpec> identifyIcebergViewFiles(
      @Nonnull String viewMetadataLocation, boolean deduplicate);

  /**
   * Purges all files that are referenced by the given table-metadata, respecting the given filter.
   *
   * <p>In case "container" files, like the metadata, manifest-list or manifest files, are not
   * readable, those files are just ignored.
   *
   * <p>This is effectively a convenience for {@code
   * purge(identifyIcebergTableFiles(tableMetadataLocation).filter(purgeSpec.fileFilter()))}
   *
   * @see #purge(Stream, PurgeSpec)
   * @see #identifyIcebergTableFiles(String, boolean)
   * @see #findFiles(String, FileFilter)
   */
  PurgeStats purgeIcebergTable(@Nonnull String tableMetadataLocation, PurgeSpec purgeSpec);

  /**
   * Purges all files that are within the base location of the given table-metadata, purge only
   * files that match the given filter.
   *
   * <p>In case "container" files, like the metadata, manifest-list or manifest files, are not
   * readable, those files are just ignored.
   *
   * <p>This is effectively a convenience for {@code
   * purge(findFiles(tableMetadata.baseLocation()).filter(purgeSpec.fileFilter()))}
   *
   * @see #purge(Stream, PurgeSpec)
   * @see #findFiles(String, FileFilter)
   */
  PurgeStats purgeIcebergTableBaseLocation(
      @Nonnull String tableMetadataLocation, PurgeSpec purgeSpec);

  /**
   * Purges all files that are referenced by the given view-metadata, respecting the given filter. *
   *
   * <p>In case "container" files like the metadata are not readable, those files are just ignored.
   *
   * <p>This is effectively a convenience for {@code
   * purge(identifyIcebergViewFiles(tableMetadataLocation).filter(fileFilter))}
   *
   * @see #purge(Stream, PurgeSpec)
   * @see #identifyIcebergViewFiles(String, boolean)
   * @see #findFiles(String, FileFilter)
   */
  PurgeStats purgeIcebergView(@Nonnull String viewMetadataLocation, PurgeSpec purgeSpec);

  /**
   * Purges all files that are within the base location of the given view-metadata, purge only files
   * that match the given filter. *
   *
   * <p>In case "container" files like the metadata are not readable, those files are just ignored.
   *
   * <p>This is effectively a convenience for {@code
   * purge(findFiles(viewMetadata.baseLocation()).filter(fileFilter))}
   *
   * @see #purge(Stream, PurgeSpec)
   * @see #findFiles(String, FileFilter)
   */
  PurgeStats purgeIcebergViewBaseLocation(
      @Nonnull String viewMetadataLocation, PurgeSpec purgeSpec);

  /**
   * Purges all files that match the given stream of locations. The {@link Stream} will be fully
   * consumed.
   *
   * <p>This is a convenience for {@link #purgeFiles(Stream, PurgeSpec)
   * purgeFiles(locationStream.map(FileSpec::location))}
   */
  PurgeStats purge(@Nonnull Stream<FileSpec> locationStream, PurgeSpec purgeSpec);

  /**
   * Purges all files from the given stream of locations. The {@link Stream} will be fully consumed.
   *
   * <p>Non-existing files and other deletion errors will not let the call fail, which makes it
   * resilient against transient or irrelevant errors.
   */
  PurgeStats purgeFiles(@Nonnull Stream<String> locationStream, PurgeSpec purgeSpec);
}
