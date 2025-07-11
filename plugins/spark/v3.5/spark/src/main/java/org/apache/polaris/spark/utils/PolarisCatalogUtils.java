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
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils;
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
      // hudi does not implement Spark V2 table provider interface
      // therefore will need to return a V1Table
      return loadV1SparkHudiTable(genericTable, identifier);
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

  /**
   * Extract catalog name from Spark session configuration.
   * Looks for configuration like: spark.sql.catalog.<CATALOG_NAME>=org.apache.polaris.spark.SparkCatalog
   */
  private static String getCatalogName() {
    SparkSession spark = SparkSession.active();
    String catalogPrefix = "spark.sql.catalog.";
    String polarisSparkCatalog = "org.apache.polaris.spark.SparkCatalog";
    
    scala.collection.Iterator<scala.Tuple2<String, String>> configIterator = spark.conf().getAll().iterator();
    while (configIterator.hasNext()) {
      scala.Tuple2<String, String> config = configIterator.next();
      String key = config._1();
      String value = config._2();
      
      if (key.startsWith(catalogPrefix) && polarisSparkCatalog.equals(value)) {
        return key.substring(catalogPrefix.length());
      }
    }
    
    return null;
  }

  public static Table loadV1SparkHudiTable(GenericTable genericTable, Identifier identifier) {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.putAll(genericTable.getProperties());
    tableProperties.put(
        TABLE_PATH_KEY, genericTable.getProperties().get(TableCatalog.PROP_LOCATION));

    // Need full identifier in order to construct CatalogTable correctly
    String namespacePath = String.join(".", identifier.namespace());
    TableIdentifier tableIdentifier =
        new TableIdentifier(identifier.name(), Option.apply(namespacePath), Option.apply(getCatalogName()));

    scala.collection.immutable.Map<String, String> scalaOptions =
        (scala.collection.immutable.Map<String, String>)
            scala.collection.immutable.Map$.MODULE$.apply(
                scala.collection.JavaConverters.mapAsScalaMap(tableProperties).toSeq());

    org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat storage =
        DataSource.buildStorageFormatFromOptions(scalaOptions);

    // Currently Polaris generic table does not contain any schema information, partition columns, stats, etc
    // for now we will just use fill the parameters we have from catalog, and let underlying client resolve the rest within its catalog implementation
    org.apache.spark.sql.types.StructType emptySchema = new org.apache.spark.sql.types.StructType();
    scala.collection.immutable.Seq<String> emptyStringSeq =
        scala.collection.JavaConverters.asScalaBuffer(new java.util.ArrayList<String>()).toList();
    CatalogTable catalogTable =
        new CatalogTable(
            tableIdentifier,
            CatalogTableType.EXTERNAL(),
            storage,
            emptySchema,
            Option.apply(genericTable.getProperties().get("provider")),
            emptyStringSeq,
            scala.Option.empty(),
            genericTable.getProperties().get("owner"),
            System.currentTimeMillis(),
            -1L,
            "",
            scalaOptions,
            scala.Option.empty(),
            scala.Option.empty(),
            scala.Option.empty(),
            emptyStringSeq,
            false,
            true,
            scala.collection.immutable.Map$.MODULE$.empty(),
            scala.Option.empty());

    return new org.apache.spark.sql.connector.catalog.V1Table(catalogTable);
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
   * Set of namespace properties that are reserved by Spark and cannot be set via database
   * properties. These properties are managed by Spark internally and will cause errors if included
   * in database metadata operations.
   */
  private static final Set<String> SPARK_RESERVED_NAMESPACE_PROPERTIES =
      Set.of("owner", "location", "comment");

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
}
