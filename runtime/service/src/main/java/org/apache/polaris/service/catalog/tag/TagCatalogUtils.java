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
package org.apache.polaris.service.catalog.tag;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.tag.exceptions.NoSuchTargetException;
import org.apache.polaris.service.catalog.common.CatalogUtils;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.catalog.validation.IcebergPropertiesValidation;
import org.apache.polaris.service.types.TagAttachmentTarget;

/** Utilities for resolving tag assignment targets and top-level Iceberg column field ids. */
public class TagCatalogUtils {

  private static final String FALLBACK_IO_IMPL = "org.apache.iceberg.io.ResolvingFileIO";

  private TagCatalogUtils() {}

  /**
   * Resolves the entity a tag assignment target addresses from the already-resolved manifest. A
   * column target resolves to its containing table.
   */
  public static PolarisResolvedPathWrapper getResolvedTargetWrapper(
      PolarisResolutionManifestCatalogView resolutionManifest, TagAttachmentTarget target) {
    return switch (target.getType()) {
      case CATALOG -> resolutionManifest.getResolvedReferenceCatalogEntity();
      case NAMESPACE -> {
        Namespace namespace = Namespace.of(target.getPath().toArray(new String[0]));
        var resolved = resolutionManifest.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
        if (resolved == null) {
          throw new NoSuchTargetException(
              "Namespace does not exist: %s", namespace.isEmpty() ? "''" : namespace.toString());
        }
        yield resolved;
      }
      case TABLE_LIKE, COLUMN -> {
        TableIdentifier tableIdentifier =
            TableIdentifier.of(target.getPath().toArray(new String[0]));
        var resolved =
            resolutionManifest.getResolvedPath(
                ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.ANY_SUBTYPE);
        if (resolved == null) {
          throw new NoSuchTargetException("Table does not exist: %s", tableIdentifier);
        }
        yield resolved;
      }
      default -> throw new IllegalArgumentException("Unsupported target type: " + target.getType());
    };
  }

  /**
   * Loads the current Iceberg schema of the resolved table by reading its metadata file directly:
   * the resolved entity carries only the metadata location, and one metadata read is required on
   * any path, so this avoids a full catalog table load. PR4-style existence checks can reuse the
   * same schema object for every column of one target.
   */
  public static Schema loadCurrentSchema(
      StorageAccessConfigProvider storageAccessConfigProvider,
      FileIOFactory fileIOFactory,
      RealmConfig realmConfig,
      CatalogEntity catalogEntity,
      PolarisResolutionManifestCatalogView resolutionManifest,
      TableIdentifier tableIdentifier,
      PolarisResolvedPathWrapper resolvedTablePath) {
    IcebergTableLikeEntity tableEntity =
        IcebergTableLikeEntity.of(resolvedTablePath.getRawLeafEntity());
    String metadataLocation = tableEntity.getMetadataLocation();
    if (metadataLocation == null) {
      throw new NoSuchTargetException("Table metadata not found for %s", tableIdentifier);
    }
    PolarisResolvedPathWrapper storageEntityPath =
        CatalogUtils.findResolvedStorageEntity(resolutionManifest, tableIdentifier);
    Map<String, String> catalogProperties = catalogEntity.getPropertiesAsMap();
    String ioImplClassName =
        IcebergPropertiesValidation.determineFileIOClassName(
            realmConfig, catalogProperties, catalogEntity.getStorageConfigurationInfo());
    if (ioImplClassName == null) {
      ioImplClassName = FALLBACK_IO_IMPL;
    }
    StorageAccessConfig storageAccessConfig =
        storageAccessConfigProvider.getStorageAccessConfig(
            tableIdentifier,
            Set.of(metadataLocation),
            Set.of(PolarisStorageActions.READ),
            Optional.empty(),
            storageEntityPath);
    try (FileIO fileIO =
        fileIOFactory.loadFileIO(
            storageAccessConfig, ioImplClassName, tableDefaultProperties(catalogProperties))) {
      TableMetadata metadata = TableMetadataParser.read(fileIO, metadataLocation);
      return metadata.schema();
    }
  }

  /**
   * The property map handed to a table-scoped FileIO: the catalog's {@code table-default.}
   * properties with the prefix stripped, matching how the table load and refresh paths initialize
   * their FileIO. The raw catalog map stays reserved for io-impl class selection; passing it here
   * would hand the FileIO prefixed keys it cannot recognize.
   */
  static Map<String, String> tableDefaultProperties(Map<String, String> catalogProperties) {
    return PropertyUtil.propertiesWithPrefix(
        catalogProperties, CatalogProperties.TABLE_DEFAULT_PREFIX);
  }

  /**
   * Resolves a top-level column name in the given schema to its Iceberg field id. Matching is exact
   * and case-sensitive, and only top-level columns are supported. Field id 0 is reserved by the tag
   * assignment record for whole-object assignments, so a resolved id that is not positive is
   * rejected rather than silently accepted as a column target.
   */
  public static int resolveTopLevelFieldId(Schema schema, String columnName) {
    List<Types.NestedField> columns = schema.columns();
    for (Types.NestedField column : columns) {
      if (column.name().equals(columnName)) {
        int fieldId = column.fieldId();
        if (fieldId <= 0) {
          throw new BadRequestException(
              "Column %s has field id %d, which is not supported as a tag target",
              columnName, fieldId);
        }
        return fieldId;
      }
    }
    throw new NoSuchTargetException("Column does not exist: %s", columnName);
  }
}
