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
package org.apache.polaris.spark.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * Utility class for Hudi-specific catalog operations, particularly namespace synchronization
 * between Polaris catalog and Spark session catalog for Hudi compatibility.
 *
 * <p>Hudi table loading requires namespace validation through the session catalog, but only the
 * Polaris catalog contains the actual namespace metadata. This class provides methods to
 * synchronize namespace operations to maintain consistency between catalogs.
 */
public class HudiCatalogUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HudiCatalogUtils.class);

  /**
   * Synchronizes namespace creation to session catalog when Hudi extension is enabled. This ensures
   * session catalog metadata stays consistent with Polaris catalog for Hudi compatibility.
   *
   * @param namespace The namespace to create
   * @param metadata The namespace metadata properties
   */
  public static void createNamespace(String[] namespace, Map<String, String> metadata) {
    // Sync namespace with filtered metadata to session catalog only when Hudi is enabled
    // This is needed because Hudi table loading uses the spark session catalog
    // to validate namespace existence and access metadata properties.
    // Reserved properties (owner, location, comment) are automatically filtered out.
    try {
      SparkSession spark = SparkSession.active();
      SessionCatalog sessionCatalog = spark.sessionState().catalog();
      String dbName = namespaceToDatabaseName(namespace);

      // Create CatalogDatabase object from namespace and metadata
      CatalogDatabase catalogDb = createCatalogDatabase(namespace, metadata);

      LOG.debug("Syncing namespace to session catalog: {}", dbName);
      sessionCatalog.createDatabase(catalogDb, true); // ignoreIfExists = true
      LOG.debug("Successfully synced namespace {} to session catalog", dbName);

    } catch (UnsupportedOperationException e) {
      String msg =
          String.format(
              "Session catalog does not support database operations, but Hudi extension requires "
                  + "namespace synchronization. Cannot create namespace: %s",
              String.join(".", namespace));
      LOG.error(msg);
      throw new RuntimeException(msg, e);
    } catch (Exception e) {
      // Handle any database creation errors
      String dbName = namespaceToDatabaseName(namespace);
      if (e.getMessage() != null && e.getMessage().contains("already exists")) {
        LOG.debug("Database already exists in session catalog, continuing: {}", dbName);
      } else {
        handleNamespaceSyncError(namespace, e);
      }
    }
  }

  /**
   * Synchronizes namespace alterations to session catalog when Hudi extension is enabled. Applies
   * both namespace existence and property changes for Hudi compatibility.
   *
   * @param namespace The namespace to alter
   * @param changes The namespace changes to apply
   */
  public static void alterNamespace(String[] namespace, NamespaceChange... changes) {
    // For Hudi compatibility, sync namespace changes to session catalog
    // Apply both namespace existence and property changes via SessionCatalog API
    try {
      SparkSession spark = SparkSession.active();
      SessionCatalog sessionCatalog = spark.sessionState().catalog();
      String dbName = namespaceToDatabaseName(namespace);

      // Ensure database exists first - get existing database or create with empty properties
      CatalogDatabase existingDb;
      try {
        existingDb = sessionCatalog.getDatabaseMetadata(dbName);
      } catch (Exception e) {
        // Check if this is a "database/schema not found" exception
        // The exception could be wrapped or be a different type, so we check both the type and
        // message
        if (e instanceof NoSuchDatabaseException) {
          // Database doesn't exist, create it with empty properties first
          CatalogDatabase newDb =
              createCatalogDatabase(namespace, java.util.Collections.emptyMap());
          sessionCatalog.createDatabase(newDb, false);
          existingDb = newDb;
        } else {
          throw e;
        }
      }

      // Apply namespace changes to the existing database
      CatalogDatabase updatedDb = applyNamespaceChanges(existingDb, changes);
      sessionCatalog.alterDatabase(updatedDb);
    } catch (UnsupportedOperationException e) {
      String msg =
          String.format(
              "Session catalog does not support database operations, but Hudi extension requires "
                  + "namespace synchronization. Cannot alter namespace: %s",
              String.join(".", namespace));
      LOG.error(msg);
      throw new RuntimeException(msg, e);
    } catch (Exception e) {
      handleNamespaceSyncError(namespace, "alter", e, false);
    }
  }

  /**
   * Ensures namespace exists in session catalog for Hudi compatibility. If namespace exists in
   * Polaris but not in session catalog, it will be synced automatically. This method is called
   * during namespace metadata loading to provide lazy synchronization.
   *
   * @param namespace The namespace to ensure exists
   * @param metadata The namespace metadata from Polaris catalog
   */
  public static void loadNamespaceMetadata(String[] namespace, Map<String, String> metadata) {
    // Check if namespace already exists in session catalog
    try {
      SparkSession spark = SparkSession.active();
      SessionCatalog sessionCatalog = spark.sessionState().catalog();
      String dbName = namespaceToDatabaseName(namespace);

      // Try to get existing database metadata
      try {
        sessionCatalog.getDatabaseMetadata(dbName);
        // Database exists, no sync needed
        LOG.debug("Namespace {} already exists in session catalog", dbName);
        return;
      } catch (Exception e) {
        // Check if this is a "database/schema not found" exception
        // The exception could be wrapped or be a different type, so we check both the type and
        // message
        if (e instanceof NoSuchDatabaseException) {
          LOG.debug("Namespace {} not found in session catalog, syncing from Polaris", dbName);
        } else {
          // Some other error occurred during lookup
          LOG.error("Error checking namespace existence in session catalog: {}", e.getMessage());
          return;
        }
      }

      // Sync namespace from Polaris to session catalog
      CatalogDatabase catalogDb = createCatalogDatabase(namespace, metadata);
      sessionCatalog.createDatabase(catalogDb, true); // ignoreIfExists = true
      LOG.debug("Successfully synced namespace {} from Polaris to session catalog", dbName);

    } catch (UnsupportedOperationException e) {
      LOG.warn(
          "Session catalog does not support database operations, cannot sync namespace: {}",
          String.join(".", namespace));
    } catch (Exception e) {
      // Log warning for sync errors, but don't fail the operation
      // This allows the main loadNamespaceMetadata operation to continue
      LOG.warn(
          "Failed to sync namespace {} to session catalog: {}",
          String.join(".", namespace),
          e.getMessage());
    }
  }

  /**
   * Synchronizes namespace drop to session catalog when Hudi extension is enabled. This maintains
   * consistency between catalogs for Hudi table operations.
   *
   * @param namespace The namespace to drop
   * @param cascade Whether to cascade the drop operation
   */
  public static void dropNamespace(String[] namespace, boolean cascade) {
    // Sync namespace drop to session catalog only when Hudi is enabled
    // This maintains consistency between catalogs for Hudi table operations
    try {
      SparkSession spark = SparkSession.active();
      SessionCatalog sessionCatalog = spark.sessionState().catalog();
      String dbName = namespaceToDatabaseName(namespace);

      LOG.debug("Dropping database from session catalog using SessionCatalog API: {}", dbName);
      sessionCatalog.dropDatabase(dbName, true, cascade); // ignoreIfNotExists = true
      LOG.debug("Successfully dropped database {} from session catalog", dbName);
    } catch (UnsupportedOperationException e) {
      String msg =
          String.format(
              "Session catalog does not support database operations, but Hudi extension requires "
                  + "namespace synchronization. Cannot drop namespace: %s",
              String.join(".", namespace));
      LOG.error(msg);
      throw new RuntimeException(msg, e);
    } catch (Exception e) {
      handleNamespaceSyncError(namespace, "drop", e, false);
    }
  }

  /**
   * Handles namespace synchronization errors with clear error messages and appropriate actions.
   *
   * @param namespace The namespace that failed to sync
   * @param operation The operation being performed (for logging)
   * @param e The exception that occurred
   * @param throwOnError Whether to throw RuntimeException for critical errors (vs log warning)
   * @throws RuntimeException For critical errors when throwOnError is true
   */
  private static void handleNamespaceSyncError(
      String[] namespace, String operation, Exception e, boolean throwOnError) {
    String errorMsg = e.getMessage();
    String ns = String.join(".", namespace);

    String msg =
        String.format(
            "Error during namespace %s. Namespace: %s, Error: %s", operation, ns, errorMsg);

    if (throwOnError) {
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } else {
      LOG.warn(msg);
    }
  }

  /** Handles namespace synchronization errors for createNamespace (throws on critical errors). */
  private static void handleNamespaceSyncError(String[] namespace, Exception e) {
    handleNamespaceSyncError(namespace, "sync", e, true);
  }

  /**
   * Converts namespace array to database name for SessionCatalog operations.
   *
   * @param namespace The namespace array
   * @return Database name as a string
   */
  private static String namespaceToDatabaseName(String[] namespace) {
    return String.join(".", namespace);
  }

  /**
   * Creates a fallback database URI using the Spark warehouse directory when Polaris location is
   * not available or invalid.
   *
   * @param dbName The database name
   * @return URI for the database location
   */
  private static URI createFallbackDatabaseUri(String dbName) {
    try {
      SparkSession spark = SparkSession.active();
      String warehousePath = spark.conf().get("spark.sql.warehouse.dir", "spark-warehouse");
      // Ensure warehousePath doesn't end with "/" to avoid double slashes
      String cleanWarehousePath =
          warehousePath.endsWith("/")
              ? warehousePath.substring(0, warehousePath.length() - 1)
              : warehousePath;
      return new URI(cleanWarehousePath + "/" + dbName + ".db");
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          "Failed to create fallback database URI for database: " + dbName, e);
    }
  }

  /**
   * Creates a CatalogDatabase object from namespace information and metadata.
   *
   * @param namespace The namespace array
   * @param metadata The namespace metadata properties
   * @return CatalogDatabase object for SessionCatalog operations
   */
  private static CatalogDatabase createCatalogDatabase(
      String[] namespace, Map<String, String> metadata) {
    String dbName = namespaceToDatabaseName(namespace);

    // Get database URI from Polaris metadata if available, otherwise use warehouse path
    URI databaseUri;
    String polarisLocation = metadata.get("location");
    if (polarisLocation != null && !polarisLocation.isEmpty()) {
      try {
        databaseUri = new URI(polarisLocation);
        LOG.debug("Using Polaris location for database {}: {}", dbName, polarisLocation);
      } catch (URISyntaxException e) {
        LOG.warn(
            "Invalid URI in Polaris location '{}', falling back to warehouse path",
            polarisLocation);
        databaseUri = createFallbackDatabaseUri(dbName);
      }
    } else {
      LOG.debug("No location in Polaris metadata for database {}, using warehouse path", dbName);
      databaseUri = createFallbackDatabaseUri(dbName);
    }

    // Filter out reserved properties that Spark doesn't allow
    Map<String, String> filteredMetadata = PolarisCatalogUtils.filterReservedProperties(metadata);

    // Convert Java Map to Scala Map for CatalogDatabase
    scala.collection.immutable.Map<String, String> scalaProperties =
        scala.collection.immutable.Map$.MODULE$.empty();
    for (Map.Entry<String, String> entry : filteredMetadata.entrySet()) {
      scalaProperties = scalaProperties.$plus(scala.Tuple2.apply(entry.getKey(), entry.getValue()));
    }

    // Create CatalogDatabase with proper database URI
    return new CatalogDatabase(dbName, "", databaseUri, scalaProperties);
  }

  /**
   * Applies namespace changes to an existing CatalogDatabase and returns the updated version.
   *
   * @param existingDb The existing CatalogDatabase
   * @param changes The namespace changes to apply
   * @return Updated CatalogDatabase with changes applied
   */
  private static CatalogDatabase applyNamespaceChanges(
      CatalogDatabase existingDb, NamespaceChange... changes) {
    // Convert existing Scala properties to Java Map for manipulation - create mutable copy
    Map<String, String> properties =
        new HashMap<>(JavaConverters.mapAsJavaMapConverter(existingDb.properties()).asJava());

    // Apply each change
    for (NamespaceChange change : changes) {
      if (change instanceof NamespaceChange.SetProperty) {
        NamespaceChange.SetProperty setProp = (NamespaceChange.SetProperty) change;
        properties.put(setProp.property(), setProp.value());
      } else if (change instanceof NamespaceChange.RemoveProperty) {
        NamespaceChange.RemoveProperty removeProp = (NamespaceChange.RemoveProperty) change;
        properties.remove(removeProp.property());
      }
      // Other change types are not supported for database operations
    }

    // Filter out reserved properties
    Map<String, String> filteredProperties =
        PolarisCatalogUtils.filterReservedProperties(properties);

    // Convert back to Scala Map
    scala.collection.immutable.Map<String, String> scalaProperties =
        scala.collection.immutable.Map$.MODULE$.empty();
    for (Map.Entry<String, String> entry : filteredProperties.entrySet()) {
      scalaProperties = scalaProperties.$plus(scala.Tuple2.apply(entry.getKey(), entry.getValue()));
    }

    // Return updated CatalogDatabase
    return new CatalogDatabase(
        existingDb.name(), existingDb.description(), existingDb.locationUri(), scalaProperties);
  }
}
