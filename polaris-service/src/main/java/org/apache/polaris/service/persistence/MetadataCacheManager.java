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
package org.apache.polaris.service.persistence;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.util.JsonUtil;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.TableLikeEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataCacheManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataCacheManager.class);

  /**
   * Load the cached {@link Table} or fall back to `fallback` if one doesn't exist. If the metadata
   * is not currently cached, it may be added to the cache.
   */
  public static TableMetadata loadTableMetadata(
      TableIdentifier tableIdentifier,
      int maxBytesToCache,
      PolarisCallContext callContext,
      PolarisMetaStoreManager metastoreManager,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      Supplier<TableMetadata> fallback) {
    LOGGER.debug(String.format("Loading cached metadata for %s", tableIdentifier));
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(tableIdentifier, PolarisEntitySubType.TABLE);
    TableLikeEntity tableLikeEntity = TableLikeEntity.of(resolvedEntities.getRawLeafEntity());
    String cacheContent = tableLikeEntity.getMetadataCacheContent();
    if (cacheContent != null) {
      LOGGER.debug(String.format("Using cached metadata for %s", tableIdentifier));
      return TableMetadataParser.fromJson(tableLikeEntity.getMetadataCacheContent());
    } else {
      TableMetadata metadata = fallback.get();
      PolarisMetaStoreManager.EntityResult cacheResult =
          cacheTableMetadata(
              tableLikeEntity,
              metadata,
              maxBytesToCache,
              callContext,
              metastoreManager,
              resolvedEntityView);
      if (!cacheResult.isSuccess()) {
        LOGGER.debug(String.format("Failed to cache metadata for %s", tableIdentifier));
      }
      return metadata;
    }
  }

  /** Convert a {@link TableMetadata} to JSON, with the size bounded */
  public static Optional<String> toBoundedJson(TableMetadata metadata, int maxBytes) {
    try (StringWriter unboundedWriter = new StringWriter()) {
      BoundedStringWriter boundedWriter = new BoundedStringWriter(unboundedWriter, maxBytes);
      JsonGenerator generator = JsonUtil.factory().createGenerator(boundedWriter);
      TableMetadataParser.toJson(metadata, generator);
      generator.flush();
      String result = boundedWriter.toString();
      if (boundedWriter.isLimitExceeded()) {
        return Optional.empty();
      } else {
        return Optional.ofNullable(result);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json for: %s", metadata);
    }
  }

  /**
   * Attempt to add table metadata to the cache
   *
   * @return The result of trying to cache the metadata
   */
  private static PolarisMetaStoreManager.EntityResult cacheTableMetadata(
      TableLikeEntity tableLikeEntity,
      TableMetadata metadata,
      int maxBytesToCache,
      PolarisCallContext callContext,
      PolarisMetaStoreManager metaStoreManager,
      PolarisResolutionManifestCatalogView resolvedEntityView) {
    Optional<String> jsonOpt = toBoundedJson(metadata, maxBytesToCache);
    // We should not reach this method in this case, but check just in case...
    if (maxBytesToCache != PolarisConfiguration.METADATA_CACHE_MAX_BYTES_NO_CACHING) {
      if (jsonOpt.isEmpty()) {
        LOGGER.debug(
            String.format(
                "Will not cache metadata for %s; metadata above the limit of %d bytes",
                tableLikeEntity.getTableIdentifier(), maxBytesToCache));
        return new PolarisMetaStoreManager.EntityResult(
            PolarisMetaStoreManager.EntityResult.ReturnStatus.SUCCESS, null);
      } else {
        LOGGER.debug(
            String.format("Caching metadata for %s", tableLikeEntity.getTableIdentifier()));
        TableLikeEntity newTableLikeEntity =
            new TableLikeEntity.Builder(tableLikeEntity)
                .setMetadataContent(tableLikeEntity.getMetadataLocation(), jsonOpt.get())
                .build();
        PolarisResolvedPathWrapper resolvedPath =
            resolvedEntityView.getResolvedPath(
                tableLikeEntity.getTableIdentifier(), PolarisEntitySubType.TABLE);
        try {
          return metaStoreManager.updateEntityPropertiesIfNotChanged(
              callContext,
              PolarisEntity.toCoreList(resolvedPath.getRawParentPath()),
              newTableLikeEntity);
        } catch (RuntimeException e) {
          // PersistenceException (& other extension-specific exceptions) may not be in scope,
          // but we can make a best-effort attempt to swallow it and just forego caching
          if (e.toString().contains("PersistenceException")) {
            LOGGER.debug(
                String.format(
                    "Encountered an error while caching %s: %s",
                    tableLikeEntity.getTableIdentifier(), e));
            return new PolarisMetaStoreManager.EntityResult(
                PolarisMetaStoreManager.EntityResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED,
                e.getMessage());
          } else {
            throw e;
          }
        }
      }
    } else {
      LOGGER.debug(
          String.format(
              "Will not cache metadata for %s; metadata caching is disabled",
              tableLikeEntity.getTableIdentifier()));
      return new PolarisMetaStoreManager.EntityResult(
          PolarisMetaStoreManager.EntityResult.ReturnStatus.SUCCESS, null);
    }
  }

  private static class BoundedStringWriter extends Writer {
    private final Writer delegate;
    private final int maxBytes;
    private long writtenBytes = 0;
    private boolean limitExceeded = false;

    /** Create a new BoundedWriter with a given limit `maxBytes`. -1 means no limit. */
    public BoundedStringWriter(StringWriter delegate, int maxBytes) {
      this.delegate = delegate;
      if (maxBytes == -1) {
        this.maxBytes = Integer.MAX_VALUE;
      } else {
        this.maxBytes = maxBytes;
      }
    }

    private boolean canWriteBytes(long bytesToWrite) throws IOException {
      if (writtenBytes + bytesToWrite > maxBytes) {
        limitExceeded = true;
      }
      return !limitExceeded;
    }

    /** `true` when the writer was asked to write more than `maxBytes` bytes */
    public final boolean isLimitExceeded() {
      return limitExceeded;
    }

    @Override
    public final void write(char[] cbuf, int off, int len) throws IOException {
      if (canWriteBytes(len)) {
        delegate.write(cbuf, off, len);
        writtenBytes += len;
      }
    }

    @Override
    public final void write(int c) throws IOException {
      if (canWriteBytes(1)) {
        delegate.write(c);
        writtenBytes++;
      }
    }

    @Override
    public final void write(String str, int off, int len) throws IOException {
      if (canWriteBytes(len)) {
        delegate.write(str, off, len);
        writtenBytes += len;
      }
    }

    @Override
    public final Writer append(CharSequence csq) throws IOException {
      String str = (csq == null) ? "null" : csq.toString();
      write(str, 0, str.length());
      return this;
    }

    @Override
    public final Writer append(CharSequence csq, int start, int end) throws IOException {
      String str = (csq == null) ? "null" : csq.subSequence(start, end).toString();
      write(str, 0, str.length());
      return this;
    }

    @Override
    public final Writer append(char c) throws IOException {
      write(c);
      return this;
    }

    @Override
    public final void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public final void close() throws IOException {
      delegate.close();
    }

    @Override
    public final String toString() {
      return delegate.toString();
    }
  }
}
