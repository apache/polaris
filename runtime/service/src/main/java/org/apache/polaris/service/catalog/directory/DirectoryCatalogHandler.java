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
package org.apache.polaris.service.catalog.directory;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Instance;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.catalog.DirectoryCatalog;
import org.apache.polaris.core.catalog.FederatedCatalogFactory;
import org.apache.polaris.core.catalog.LocalCatalogFactory;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.DirectoryEntity;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.types.Directory;
import org.apache.polaris.service.types.DirectoryFilter;
import org.apache.polaris.service.types.ListDirectoriesResponse;
import org.apache.polaris.service.types.LoadDirectoryResponse;
import org.apache.polaris.service.types.ScanSchedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PolarisImmutable
@SuppressWarnings("immutables:incompat")
public abstract class DirectoryCatalogHandler extends CatalogHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryCatalogHandler.class);

  /** Predefined Iceberg table schema for directory file metadata. */
  public static final Schema DIRECTORY_TABLE_SCHEMA =
      new Schema(
          required(1, "file_uri", Types.StringType.get(), "The object URI"),
          optional(2, "content_type", Types.StringType.get(), "The MIME content type of the object"),
          optional(3, "size", Types.LongType.get(), "The size of the object in bytes"),
          optional(
              4,
              "checksum_algorithm",
              Types.StringType.get(),
              "The checksum algorithm (e.g. MD5, SHA-256)"),
          optional(5, "checksum", Types.StringType.get(), "The file checksum"),
          optional(
              6,
              "last_modified",
              Types.TimestampType.withZone(),
              "The last modification timestamp"),
          optional(
              7,
              "metadata",
              Types.MapType.ofRequired(8, 9, Types.StringType.get(), Types.StringType.get()),
              "Additional key-value metadata from the object store"));

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<List<String>> LIST_OF_STRING = new TypeReference<>() {};

  static String toJsonArray(List<String> list) {
    if (list == null) {
      return null;
    }
    try {
      return MAPPER.writeValueAsString(list);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize filter list", e);
    }
  }

  private static List<String> parseJsonArray(String json) {
    if (json == null) {
      return null;
    }
    try {
      return MAPPER.readValue(json, LIST_OF_STRING);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse filter list", e);
    }
  }

  private static DirectoryFilter toDirectoryFilter(DirectoryEntity entity) {
    List<String> include = parseJsonArray(entity.getFilterInclude());
    List<String> exclude = parseJsonArray(entity.getFilterExclude());
    if (include == null && exclude == null) {
      return null;
    }
    return DirectoryFilter.builder().setInclude(include).setExclude(exclude).build();
  }

  static String scanScheduleToJson(ScanSchedule scanSchedule) {
    if (scanSchedule == null) {
      return null;
    }
    try {
      return MAPPER.writeValueAsString(scanSchedule);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize scan schedule", e);
    }
  }

  private static ScanSchedule scanScheduleFromJson(String json) {
    if (json == null) {
      return null;
    }
    try {
      return MAPPER.readValue(json, ScanSchedule.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse scan schedule", e);
    }
  }

  protected abstract PolarisCredentialManager credentialManager();

  protected abstract Instance<FederatedCatalogFactory> federatedCatalogFactories();

  protected abstract LocalCatalogFactory localCatalogFactory();

  private DirectoryCatalog directoryCatalog;
  private Catalog icebergCatalog;

  @Override
  protected void initializeCatalog() {
    CatalogEntity resolvedCatalogEntity = resolutionManifest.getResolvedCatalogEntity();
    ConnectionConfigInfoDpo connectionConfigInfoDpo =
        resolvedCatalogEntity.getConnectionConfigInfoDpo();
    if (connectionConfigInfoDpo != null) {
      LOGGER
          .atInfo()
          .addKeyValue("remoteUrl", connectionConfigInfoDpo.getUri())
          .log("Initializing federated catalog");
      FeatureConfiguration.enforceFeatureEnabledOrThrow(
          realmConfig(), FeatureConfiguration.ENABLE_CATALOG_FEDERATION);

      DirectoryCatalog federatedCatalog;
      ConnectionType connectionType =
          ConnectionType.fromCode(connectionConfigInfoDpo.getConnectionTypeCode());

      Instance<FederatedCatalogFactory> federatedCatalogFactory =
          federatedCatalogFactories()
              .select(Identifier.Literal.of(connectionType.getFactoryIdentifier()));
      if (federatedCatalogFactory.isResolvable()) {
        Map<String, String> catalogProperties = resolvedCatalogEntity.getPropertiesAsMap();
        federatedCatalog =
            federatedCatalogFactory
                .get()
                .createDirectoryCatalog(
                    connectionConfigInfoDpo, credentialManager(), catalogProperties);
      } else {
        throw new UnsupportedOperationException(
            "External catalog factory for type '" + connectionType + "' is unavailable.");
      }
      this.directoryCatalog = federatedCatalog;
    } else {
      LOGGER.atInfo().log("Initializing non-federated catalog");
      this.directoryCatalog =
          new PolarisDirectoryCatalog(metaStoreManager(), callContext(), this.resolutionManifest);
      this.directoryCatalog.initialize(catalogName(), Map.of());
    }

    // Initialize an IcebergCatalog for creating/dropping the associated Iceberg tables
    this.icebergCatalog = localCatalogFactory().createCatalog(this.resolutionManifest);
  }

  public ListDirectoriesResponse listDirectories(Namespace parent) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    authorizeBasicNamespaceOperationOrThrow(op, parent);

    return ListDirectoriesResponse.builder()
        .setIdentifiers(new LinkedHashSet<>(directoryCatalog.listDirectories(parent)))
        .build();
  }

  public LoadDirectoryResponse createDirectory(
      TableIdentifier identifier,
      String baseLocation,
      String filterInclude,
      String filterExclude,
      String scanSchedule,
      Map<String, String> properties) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_TABLE_DIRECT;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);

    // Check that the Iceberg table name does not collide with an existing entity
    TableIdentifier tableId = TableIdentifier.of(identifier.namespace(), identifier.name());
    if (this.icebergCatalog.tableExists(tableId)) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException(
          "Cannot create directory %s: associated table %s already exists", identifier, tableId);
    }

    DirectoryEntity createdEntity =
        this.directoryCatalog.createDirectory(
            identifier, baseLocation, filterInclude, filterExclude, scanSchedule, properties);

    // Create the associated Iceberg table with the predefined schema
    try {
      this.icebergCatalog.createTable(tableId, DIRECTORY_TABLE_SCHEMA);
      LOGGER.debug(
          "Created associated Iceberg table {} for directory {}", tableId, identifier);
    } catch (Exception e) {
      // Roll back the directory entity if table creation fails
      LOGGER.warn(
          "Failed to create Iceberg table {} for directory {}, rolling back",
          tableId,
          identifier,
          e);
      this.directoryCatalog.dropDirectory(identifier);
      throw e;
    }

    Directory createdDirectory =
        Directory.builder()
            .setName(createdEntity.getName())
            .setBaseLocation(createdEntity.getBaseLocation())
            .setFilter(toDirectoryFilter(createdEntity))
            .setScanSchedule(scanScheduleFromJson(createdEntity.getScanSchedule()))
            .setProperties(createdEntity.getPropertiesAsMap())
            .build();

    return LoadDirectoryResponse.builder().setDirectory(createdDirectory).build();
  }

  public boolean dropDirectory(TableIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);

    // Verify the directory exists before attempting to drop the associated table
    this.directoryCatalog.loadDirectory(identifier);

    // The associated Iceberg table shares the same name as the directory.
    // Drop the table first — if this fails the directory entity is still intact
    // and the operation can be retried.
    try {
      this.icebergCatalog.dropTable(identifier, false);
      LOGGER.debug("Dropped associated Iceberg table for directory {}", identifier);
    } catch (Exception e) {
      LOGGER.error("Failed to drop associated Iceberg table for directory {}", identifier, e);
      throw e;
    }

    return this.directoryCatalog.dropDirectory(identifier);
  }

  public LoadDirectoryResponse loadDirectory(TableIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_TABLE;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.DIRECTORY, identifier);

    DirectoryEntity loadedEntity = this.directoryCatalog.loadDirectory(identifier);
    Directory loadedDirectory =
        Directory.builder()
            .setName(loadedEntity.getName())
            .setBaseLocation(loadedEntity.getBaseLocation())
            .setFilter(toDirectoryFilter(loadedEntity))
            .setScanSchedule(scanScheduleFromJson(loadedEntity.getScanSchedule()))
            .setProperties(loadedEntity.getPropertiesAsMap())
            .build();

    return LoadDirectoryResponse.builder().setDirectory(loadedDirectory).build();
  }
}
