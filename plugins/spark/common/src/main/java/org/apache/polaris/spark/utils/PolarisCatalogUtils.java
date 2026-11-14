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
import java.util.Map;
import org.apache.polaris.spark.rest.GenericTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Base class with helpers used by the Polaris Spark client.
 */
public abstract class PolarisCatalogUtils {

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

  /** Check whether the table provider is paimon. */
  public static boolean usePaimon(String provider) {
    return "paimon".equalsIgnoreCase(provider);
  }

  /**
   * For tables whose location is managed by Spark Session Catalog, there will be no location or
   * path in the properties.
   */
  public static boolean isTableWithSparkManagedLocation(Map<String, String> properties) {
    boolean hasLocationClause = properties.containsKey(TableCatalog.PROP_LOCATION);
    boolean hasPathClause = properties.containsKey(TABLE_PATH_KEY);
    return !hasLocationClause && !hasPathClause;
  }

  /**
   * Normalize table properties for loading Spark tables by ensuring the TABLE_PATH_KEY is properly
   * set. DataSourceV2 requires the path property on table loading.
   */
  protected static Map<String, String> normalizeTablePropertiesForLoadSparkTable(
      GenericTable genericTable) {
    Map<String, String> properties = genericTable.properties();
    boolean hasLocationClause = properties.get(TableCatalog.PROP_LOCATION) != null;
    boolean hasPathClause = properties.get(TABLE_PATH_KEY) != null;
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.putAll(properties);
    if (!hasPathClause) {
      // DataSourceV2 requires the path property on table loading. However, spark today
      // doesn't create the corresponding path property if the path keyword is not
      // provided by user when location is provided. Here, we duplicate the location
      // property as path to make sure the table can be loaded.
      if (genericTable.baseLocation() != null && !genericTable.baseLocation().isEmpty()) {
        tableProperties.put(TABLE_PATH_KEY, genericTable.baseLocation());
      } else if (hasLocationClause) {
        tableProperties.put(TABLE_PATH_KEY, properties.get(TableCatalog.PROP_LOCATION));
      }
    }
    return tableProperties;
  }

  /**
   * Load spark table using DataSourceV2.
   *
   * @return V2Table if DataSourceV2 is available for the table format. For delta table, it returns
   *     DeltaTableV2.
   */
  public static Table loadV2SparkTable(GenericTable genericTable) {
    SparkSession sparkSession = SparkSession.active();
    TableProvider provider =
        DataSource.lookupDataSourceV2(genericTable.format(), sparkSession.sessionState().conf())
            .get();
    Map<String, String> tableProperties = normalizeTablePropertiesForLoadSparkTable(genericTable);
    return DataSourceV2Utils.getTableFromProvider(
        provider, new CaseInsensitiveStringMap(tableProperties), scala.Option.empty());
  }

  /**
   * Return a Spark V1Table for formats that do not use DataSourceV2. Currently, this is being used
   * for Hudi tables
   */
  public abstract Table loadV1SparkTable(
      GenericTable genericTable, Identifier identifier, String catalogName);
}
