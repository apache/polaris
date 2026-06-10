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
import org.apache.polaris.spark.rest.GenericTable;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.DataSource;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map$;
import scala.collection.mutable.Builder;

/** Spark 4.0 implementation of {@link PolarisCatalogUtils}. */
public class PolarisCatalogSpark4Utils extends PolarisCatalogUtils {

  @Override
  public Table loadV1SparkTable(
      GenericTable genericTable, Identifier identifier, String catalogName) {
    Map<String, String> tableProperties = normalizeTablePropertiesForLoadSparkTable(genericTable);

    // Need full identifier in order to construct CatalogTable
    String namespacePath = String.join(".", identifier.namespace());
    TableIdentifier tableIdentifier =
        new TableIdentifier(
            identifier.name(), Option.apply(namespacePath), Option.apply(catalogName));

    Builder<Tuple2<String, String>, scala.collection.immutable.Map<String, String>> mb =
        Map$.MODULE$.newBuilder();
    tableProperties.forEach((k, v) -> mb.$plus$eq(Tuple2.apply(k, v)));
    scala.collection.immutable.Map<String, String> scalaOptions = mb.result();

    org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat storage =
        DataSource.buildStorageFormatFromOptions(scalaOptions);

    // Currently Polaris generic table does not contain any schema information, partition columns,
    // stats, etc
    // for now we will fill the parameters we have from polaris catalog, and let underlying client
    // resolve the rest within its catalog implementation
    org.apache.spark.sql.types.StructType emptySchema = new org.apache.spark.sql.types.StructType();
    @SuppressWarnings("deprecation")
    scala.collection.immutable.Seq<String> emptyStringSeq =
        scala.collection.JavaConverters.asScalaBuffer(new java.util.ArrayList<String>()).toList();
    CatalogTable catalogTable =
        new CatalogTable(
            tableIdentifier,
            CatalogTableType.EXTERNAL(),
            storage,
            emptySchema,
            Option.apply(genericTable.format()),
            emptyStringSeq,
            scala.Option.empty(),
            genericTable.properties().getOrDefault(TableCatalog.PROP_OWNER, ""),
            System.currentTimeMillis(),
            -1L,
            "",
            scalaOptions,
            scala.Option.empty(),
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
}
