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

import com.google.common.collect.Maps;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.polaris.spark.rest.GenericTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils;
import org.apache.spark.sql.hudi.catalog.HoodieInternalV2Table;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class PolarisCatalogUtils {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisCatalogUtils.class);

  public static final String TABLE_PROVIDER_KEY = "provider";
  public static final String TABLE_PATH_KEY = "path";

  /** Check whether the table provider is iceberg. */
  public static boolean useIceberg(String provider) {
    return provider == null || "iceberg".equalsIgnoreCase(provider);
  }

  /** Check whether the table provider is delta. */
  public static boolean useDelta(String provider) {
    return "delta".equalsIgnoreCase(provider);
  }

  public static boolean useHudi(String provider) {
    return "hudi".equalsIgnoreCase(provider);
  }

  public static boolean isHudiExtensionEnabled() {
    SparkSession spark = SparkSession.active();
    String extensions = spark.conf().get("spark.sql.extensions", null);
    return extensions != null
        && extensions.contains("org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
  }

  /**
   * For tables whose location is manged by Spark Session Catalog, there will be no location or path
   * in the properties.
   */
  public static boolean isTableWithSparkManagedLocation(Map<String, String> properties) {
    boolean hasLocationClause = properties.containsKey(TableCatalog.PROP_LOCATION);
    boolean hasPathClause = properties.containsKey(TABLE_PATH_KEY);
    return !hasLocationClause && !hasPathClause;
  }

  /**
   * Load spark table using DataSourceV2.
   *
   * @return V2Table if DataSourceV2 is available for the table format. For delta table, it returns
   *     DeltaTableV2. For hudi it should return HoodieInternalV2Table.
   */
  public static Table loadSparkTable(GenericTable genericTable, Identifier identifier) {
    if (genericTable.getFormat().equalsIgnoreCase("hudi")) {
      // hudi does not implement table provider interface, so will need to catch it
      return loadHudiSparkTable(genericTable, identifier);
    }
    SparkSession sparkSession = SparkSession.active();
    TableProvider provider =
        DataSource.lookupDataSourceV2(genericTable.getFormat(), sparkSession.sessionState().conf())
            .get();
    Map<String, String> properties = genericTable.getProperties();
    boolean hasLocationClause = properties.get(TableCatalog.PROP_LOCATION) != null;
    boolean hasPathClause = properties.get(TABLE_PATH_KEY) != null;
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.putAll(properties);
    if (!hasPathClause && hasLocationClause) {
      // DataSourceV2 requires the path property on table loading. However, spark today
      // doesn't create the corresponding path property if the path keyword is not
      // provided by user when location is provided. Here, we duplicate the location
      // property as path to make sure the table can be loaded.
      tableProperties.put(TABLE_PATH_KEY, properties.get(TableCatalog.PROP_LOCATION));
    }
    return DataSourceV2Utils.getTableFromProvider(
        provider, new CaseInsensitiveStringMap(tableProperties), scala.Option.empty());
  }

  public static Table loadHudiSparkTable(GenericTable genericTable, Identifier identifier) {
    SparkSession sparkSession = SparkSession.active();
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.putAll(genericTable.getProperties());
    tableProperties.put(
        TABLE_PATH_KEY, genericTable.getProperties().get(TableCatalog.PROP_LOCATION));
    String namespacePath = String.join(".", identifier.namespace());
    TableIdentifier tableIdentifier =
        new TableIdentifier(identifier.name(), Option.apply(namespacePath));
    CatalogTable catalogTable = null;
    try {
      catalogTable = sparkSession.sessionState().catalog().getTableMetadata(tableIdentifier);
    } catch (NoSuchDatabaseException e) {
      throw new RuntimeException(
          "No database found for the given tableIdentifier:" + tableIdentifier, e);
    } catch (NoSuchTableException e) {
      LOG.debug("No table currently exists, as an initial create table");
    }
    return new HoodieInternalV2Table(
        sparkSession,
        genericTable.getProperties().get(TableCatalog.PROP_LOCATION),
        Option.apply(catalogTable),
        Option.apply(identifier.toString()),
        new CaseInsensitiveStringMap(tableProperties));
  }

  /**
   * Get the catalogAuth field inside the RESTSessionCatalog used by Iceberg Spark Catalog use
   * reflection. TODO: Deprecate this function once the iceberg client is updated to 1.9.0 to use
   * AuthManager and the capability of injecting an AuthManger is available. Related iceberg PR:
   * https://github.com/apache/iceberg/pull/12655
   */
  public static OAuth2Util.AuthSession getAuthSession(SparkCatalog sparkCatalog) {
    try {
      Field icebergCatalogField = sparkCatalog.getClass().getDeclaredField("icebergCatalog");
      icebergCatalogField.setAccessible(true);
      Catalog icebergCatalog = (Catalog) icebergCatalogField.get(sparkCatalog);
      RESTCatalog icebergRestCatalog;
      if (icebergCatalog instanceof CachingCatalog) {
        Field catalogField = icebergCatalog.getClass().getDeclaredField("catalog");
        catalogField.setAccessible(true);
        icebergRestCatalog = (RESTCatalog) catalogField.get(icebergCatalog);
      } else {
        icebergRestCatalog = (RESTCatalog) icebergCatalog;
      }

      Field sessionCatalogField = icebergRestCatalog.getClass().getDeclaredField("sessionCatalog");
      sessionCatalogField.setAccessible(true);
      RESTSessionCatalog sessionCatalog =
          (RESTSessionCatalog) sessionCatalogField.get(icebergRestCatalog);

      Field authField = sessionCatalog.getClass().getDeclaredField("catalogAuth");
      authField.setAccessible(true);
      return (OAuth2Util.AuthSession) authField.get(sessionCatalog);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get the catalogAuth from the Iceberg SparkCatalog", e);
    }
  }

  /**
   * Converts a NamespaceChange object to equivalent SQL command for session catalog sync.
   *
   * @param namespace The namespace name (already joined with dots)
   * @param change The NamespaceChange to convert
   * @return SQL command string or null if change type is not supported
   */
  public static String convertNamespaceChangeToSQL(String namespace, NamespaceChange change) {
    if (change instanceof NamespaceChange.SetProperty) {
      NamespaceChange.SetProperty setProp = (NamespaceChange.SetProperty) change;
      return String.format(
          "ALTER NAMESPACE spark_catalog.%s SET PROPERTIES ('%s' = '%s')",
          namespace, setProp.property(), setProp.value());
    } else if (change instanceof NamespaceChange.RemoveProperty) {
      NamespaceChange.RemoveProperty removeProp = (NamespaceChange.RemoveProperty) change;
      return String.format(
          "ALTER NAMESPACE spark_catalog.%s UNSET PROPERTIES ('%s')",
          namespace, removeProp.property());
    }
    // Other change types not supported or don't need session catalog sync
    return null;
  }

  /**
   * Set of namespace properties that are reserved by Spark and cannot be set via DBPROPERTIES.
   * These properties are managed by Spark internally and will cause errors if included in CREATE
   * NAMESPACE ... WITH DBPROPERTIES statements.
   */
  private static final Set<String> SPARK_RESERVED_NAMESPACE_PROPERTIES =
      Set.of(
          "owner", // Automatically set to current user
          "location", // Managed by Spark for database location
          "comment" // May have special handling in some contexts
          );

  /**
   * Filters out Spark reserved properties from metadata map to prevent SQL errors.
   *
   * @param metadata The original metadata map
   * @return Filtered metadata map without reserved properties
   */
  public static Map<String, String> filterReservedProperties(Map<String, String> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return metadata;
    }

    Map<String, String> filtered = new HashMap<>();
    List<String> filteredOut = new ArrayList<>();

    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      String key = entry.getKey();
      if (SPARK_RESERVED_NAMESPACE_PROPERTIES.contains(key.toLowerCase(Locale.ROOT))) {
        filteredOut.add(key);
      } else {
        filtered.put(key, entry.getValue());
      }
    }

    if (!filteredOut.isEmpty()) {
      LOG.info(
          "Filtered out reserved namespace properties for Hudi compatibility: {}", filteredOut);
    }

    return filtered;
  }

  /**
   * Formats metadata properties for SQL DBPROPERTIES clause with proper escaping. Automatically
   * filters out reserved properties that Spark doesn't allow.
   *
   * @param metadata The metadata map to format
   * @return Formatted DBPROPERTIES clause or empty string if no properties
   */
  public static String formatPropertiesForSQL(Map<String, String> metadata) {
    Map<String, String> filteredMetadata = filterReservedProperties(metadata);

    if (filteredMetadata == null || filteredMetadata.isEmpty()) {
      return "";
    }

    StringBuilder props = new StringBuilder();
    props.append(" WITH DBPROPERTIES (");

    List<String> propertyList = new ArrayList<>();
    for (Map.Entry<String, String> entry : filteredMetadata.entrySet()) {
      // Escape single quotes in keys and values
      String key = entry.getKey().replace("'", "''");
      String value = entry.getValue().replace("'", "''");
      propertyList.add(String.format("'%s' = '%s'", key, value));
    }

    props.append(String.join(", ", propertyList));
    props.append(")");

    return props.toString();
  }
}
