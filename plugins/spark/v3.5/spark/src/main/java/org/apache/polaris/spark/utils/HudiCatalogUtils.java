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

import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * session catalog metadata stays consistent with Polaris catalog for comprehensive Hudi
   * compatibility.
   *
   * @param namespace The namespace to create
   * @param metadata The namespace metadata properties
   */
  public static void createNamespace(String[] namespace, Map<String, String> metadata) {
    if (!PolarisCatalogUtils.isHudiExtensionEnabled()) {
      return;
    }

    // Sync namespace with filtered metadata to session catalog only when Hudi is enabled
    // This is needed because Hudi table loading uses the spark session catalog
    // to validate namespace existence and access metadata properties.
    // Reserved properties (owner, location, comment) are automatically filtered out.
    try {
      SparkSession spark = SparkSession.active();
      String ns = String.join(".", namespace);

      // Build CREATE NAMESPACE SQL with metadata properties (filtered for reserved properties)
      String createSql = String.format("CREATE NAMESPACE IF NOT EXISTS spark_catalog.%s", ns);
      String propertiesClause = PolarisCatalogUtils.formatPropertiesForSQL(metadata);
      createSql += propertiesClause;

      LOG.debug("Syncing namespace to session catalog with SQL: {}", createSql);
      spark.sql(createSql);
      LOG.debug("Successfully synced namespace {} to session catalog", ns);

    } catch (UnsupportedOperationException e) {
      String msg =
          String.format(
              "Session catalog does not support namespace operations, but Hudi extension requires "
                  + "namespace synchronization. Cannot create namespace: %s",
              String.join(".", namespace));
      LOG.error(msg);
      throw new RuntimeException(msg, e);
    } catch (Exception e) {
      handleNamespaceSyncError(namespace, e);
    }
  }

  /**
   * Synchronizes namespace alterations to session catalog when Hudi extension is enabled. Applies
   * both namespace existence and property changes via SQL commands for Hudi compatibility.
   *
   * @param namespace The namespace to alter
   * @param changes The namespace changes to apply
   */
  public static void alterNamespace(String[] namespace, NamespaceChange... changes) {
    if (!PolarisCatalogUtils.isHudiExtensionEnabled()) {
      return;
    }

    // For Hudi compatibility, sync namespace changes to session catalog
    // Apply both namespace existence and property changes via SQL commands
    try {
      SparkSession spark = SparkSession.active();
      String ns = String.join(".", namespace);

      // Ensure namespace exists first
      spark.sql(String.format("CREATE NAMESPACE IF NOT EXISTS spark_catalog.%s", ns));

      // Apply namespace changes via SQL
      for (NamespaceChange change : changes) {
        String sql = PolarisCatalogUtils.convertNamespaceChangeToSQL(ns, change);
        if (sql != null) {
          spark.sql(sql);
        }
      }
    } catch (UnsupportedOperationException e) {
      String msg =
          String.format(
              "Session catalog does not support namespace operations, but Hudi extension requires "
                  + "namespace synchronization. Cannot alter namespace: %s",
              String.join(".", namespace));
      LOG.error(msg);
      throw new RuntimeException(msg, e);
    } catch (Exception e) {
      handleNamespaceSyncError(namespace, "alter", e, false);
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
    if (!PolarisCatalogUtils.isHudiExtensionEnabled()) {
      return;
    }

    // Sync namespace drop to session catalog only when Hudi is enabled
    // This maintains consistency between catalogs for Hudi table operations
    try {
      SparkSession spark = SparkSession.active();
      String ns = String.join(".", namespace);

      // Build DROP NAMESPACE SQL with CASCADE/RESTRICT flag
      String cascadeClause = cascade ? " CASCADE" : " RESTRICT";
      String dropSql =
          String.format("DROP NAMESPACE IF EXISTS spark_catalog.%s%s", ns, cascadeClause);

      spark.sql(dropSql);
    } catch (UnsupportedOperationException e) {
      String msg =
          String.format(
              "Session catalog does not support namespace operations, but Hudi extension requires "
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

    if (errorMsg != null && errorMsg.contains("reserved namespace property")) {
      String msg =
          String.format(
              "Namespace %s failed due to reserved property - this should not happen after filtering. "
                  + "Namespace: %s, Error: %s",
              operation, ns, errorMsg);
      if (throwOnError) {
        LOG.error(msg);
        throw new RuntimeException(msg, e);
      } else {
        LOG.warn(msg);
      }

    } else if (errorMsg != null && errorMsg.contains("already exists")) {
      LOG.debug("Namespace {} already exists in session catalog, continuing", ns);

    } else if (errorMsg != null && errorMsg.contains("UNSUPPORTED_FEATURE")) {
      String msg =
          String.format(
              "Session catalog does not support required features for namespace %s. "
                  + "Namespace: %s, Error: %s",
              operation, ns, errorMsg);
      if (throwOnError) {
        LOG.error(msg);
        throw new RuntimeException(msg, e);
      } else {
        LOG.warn(msg);
      }

    } else {
      String msg =
          String.format(
              "Unexpected error during namespace %s. Namespace: %s, Error: %s",
              operation, ns, errorMsg);
      if (throwOnError) {
        LOG.error(msg, e);
        throw new RuntimeException(msg, e);
      } else {
        LOG.warn(msg);
      }
    }
  }

  /** Handles namespace synchronization errors for createNamespace (throws on critical errors). */
  private static void handleNamespaceSyncError(String[] namespace, Exception e) {
    handleNamespaceSyncError(namespace, "sync", e, true);
  }
}
