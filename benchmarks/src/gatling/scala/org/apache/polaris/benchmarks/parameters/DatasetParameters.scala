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

package org.apache.polaris.benchmarks.parameters

import org.apache.polaris.benchmarks.NAryTreeBuilder

/**
 * Case class to hold the dataset parameters for the benchmark.
 *
 * @param numCatalogs The number of catalogs to create.
 * @param defaultBaseLocation The default base location for the datasets.
 * @param nsWidth The width of the namespace n-ary tree.
 * @param nsDepth The depth of the namespace n-ary tree.
 * @param numNamespaceProperties The number of namespace properties to create.
 * @param numNamespacePropertyUpdates The number of namespace property updates to perform.
 * @param numTablesPerNs The number of tables per namespace to create.
 * @param numTablesMax The maximum number of tables to create. If set to -1, all tables are created.
 * @param numColumns The number of columns per table to create.
 * @param numTableProperties The number of table properties to create.
 * @param numTablePropertyUpdates The number of table property updates to perform.
 * @param numViewsPerNs The number of views per namespace to create.
 * @param numViewsMax The maximum number of views to create. If set to -1, all views are created.
 * @param numViewPropertyUpdates The number of view property updates to perform.
 */
case class DatasetParameters(
    numCatalogs: Int,
    defaultBaseLocation: String,
    nsWidth: Int,
    nsDepth: Int,
    numNamespaceProperties: Int,
    numNamespacePropertyUpdates: Int,
    numTablesPerNs: Int,
    numTablesMax: Int,
    numColumns: Int,
    numTableProperties: Int,
    numTablePropertyUpdates: Int,
    numViewsPerNs: Int,
    numViewsMax: Int,
    numViewPropertyUpdates: Int
) extends Explainable {
  val nAryTree: NAryTreeBuilder = NAryTreeBuilder(nsWidth, nsDepth)
  val numTables: Int = if (numTablesMax <= 0) {
    nAryTree.numberOfLastLevelElements * numTablesPerNs
  } else {
    numTablesMax
  }

  val numViews: Int = if (numViewsMax <= 0) {
    nAryTree.numberOfLastLevelElements * numViewsPerNs
  } else {
    numViewsMax
  }

  override def explanations: List[String] = List(
    s"The dataset parameters describe $numCatalogs catalogs with a default base location of $defaultBaseLocation.",
    s"In the first catalog, there is a complete n-ary tree of namespaces with width $nsWidth and depth $nsDepth, for a total of ${nAryTree.numberOfNodes} namespaces.",
    s"Each namespace has $numNamespaceProperties properties and $numNamespacePropertyUpdates property updates.",
    s"Each of the ${nAryTree.numberOfLastLevelElements} leaf namespaces has $numTablesPerNs tables, for a total of $numTables tables.",
    s"Each of the ${nAryTree.numberOfLastLevelElements} leaf namespaces has $numViewsPerNs views, for a total of $numViews views.",
    s"Each table has $numColumns columns and $numTableProperties properties.",
    s"There are $numTablePropertyUpdates table property updates for each table, for a total of ${numTablePropertyUpdates * numTables} updates.",
    s"There are $numViewPropertyUpdates view property updates for each view, for a total of ${numViewPropertyUpdates * numViews} updates."
  )
}

/**
 * Object holding the `datasetParameters` instance, populated from environment variables. Multiple
 * catalogs can be created, but namespaces and other entities will only be created in the first
 * catalog. Namespaces will be created in a complete n-ary tree structure. By default, we create one
 * catalog and a complete n-ary tree of namespaces with width 2 and depth 4. Each namespace has 10
 * properties. Leaf namespaces have 5 tables with 10 columns and 10 properties each.
 */
object DatasetParameters {
  val datasetParameters: DatasetParameters = DatasetParameters(
    sys.env.getOrElse("NUM_CATALOGS", "1").toInt,
    sys.env.getOrElse("DEFAULT_BASE_LOCATION", "file:///tmp/polaris"),
    sys.env.getOrElse("NAMESPACE_WIDTH", "2").toInt,
    sys.env.getOrElse("NAMESPACE_DEPTH", "4").toInt,
    sys.env.getOrElse("NUM_NAMESPACE_PROPERTIES", "10").toInt,
    sys.env.getOrElse("NUM_NAMESPACE_PROPERTY_UPDATES", "5").toInt,
    sys.env.getOrElse("NUM_TABLES_PER_NAMESPACE", "5").toInt,
    sys.env.getOrElse("NUM_TABLES_MAX", "-1").toInt,
    sys.env.getOrElse("NUM_COLUMNS", "10").toInt,
    sys.env.getOrElse("NUM_TABLE_PROPERTIES", "10").toInt,
    sys.env.getOrElse("NUM_TABLE_PROPERTY_UPDATES", "10").toInt,
    sys.env.getOrElse("NUM_VIEWS_PER_NAMESPACE", "3").toInt,
    sys.env.getOrElse("NUM_VIEWS_MAX", "-1").toInt,
    sys.env.getOrElse("NUM_VIEW_PROPERTY_UPDATES", "1").toInt
  )
}
