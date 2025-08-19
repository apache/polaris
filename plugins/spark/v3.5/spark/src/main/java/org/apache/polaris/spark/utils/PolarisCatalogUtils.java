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
import java.util.Map;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.polaris.spark.rest.GenericTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class PolarisCatalogUtils {
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
   *     DeltaTableV2.
   */
  public static Table loadSparkTable(GenericTable genericTable) {
    SparkSession sparkSession = SparkSession.active();
    TableProvider provider =
        DataSource.lookupDataSourceV2(genericTable.getFormat(), sparkSession.sessionState().conf())
            .get();
    Map<String, String> properties = genericTable.getProperties();
    boolean hasLocationClause = properties.get(TableCatalog.PROP_LOCATION) != null;
    boolean hasPathClause = properties.get(TABLE_PATH_KEY) != null;
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.putAll(properties);
    if (!hasPathClause) {
      // DataSourceV2 requires the path property on table loading. However, spark today
      // doesn't create the corresponding path property if the path keyword is not
      // provided by user when location is provided. Here, we duplicate the location
      // property as path to make sure the table can be loaded.
      if (genericTable.getBaseLocation() != null && !genericTable.getBaseLocation().isEmpty()) {
        tableProperties.put(TABLE_PATH_KEY, genericTable.getBaseLocation());
      } else if (hasLocationClause) {
        tableProperties.put(TABLE_PATH_KEY, properties.get(TableCatalog.PROP_LOCATION));
      }
    }
    return DataSourceV2Utils.getTableFromProvider(
        provider, new CaseInsensitiveStringMap(tableProperties), scala.Option.empty());
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
}
