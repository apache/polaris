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
package org.apache.polaris.spark;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SupportsReplaceView;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.*;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkCatalog
    implements TableCatalog, SupportsNamespaces, ViewCatalog, SupportsReplaceView {
  private static final Set<String> DEFAULT_NS_KEYS = ImmutableSet.of(TableCatalog.PROP_OWNER);
  private String catalogName = null;
  private Catalog icebergCatalog = null;
  // TODO: Add Polaris Specific REST Catalog

  private org.apache.iceberg.catalog.SupportsNamespaces asNamespaceCatalog = null;
  private org.apache.iceberg.catalog.ViewCatalog asViewCatalog = null;

  private Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
    Configuration conf = SparkUtil.hadoopConfCatalogOverrides(SparkSession.active(), name);
    Map<String, String> optionsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    optionsMap.putAll(options.asCaseSensitiveMap());
    optionsMap.put(CatalogProperties.APP_ID, SparkSession.active().sparkContext().applicationId());
    optionsMap.put(CatalogProperties.USER, SparkSession.active().sparkContext().sparkUser());
    return CatalogUtil.buildIcebergCatalog(name, optionsMap, conf);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.icebergCatalog = buildIcebergCatalog(name, options);

    this.asNamespaceCatalog = (org.apache.iceberg.catalog.SupportsNamespaces) this.icebergCatalog;
    this.asViewCatalog = (org.apache.iceberg.catalog.ViewCatalog) this.icebergCatalog;
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void renameTable(Identifier from, Identifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public String[] defaultNamespace() {
    return new String[0];
  }

  @Override
  public String[][] listNamespaces() {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      return asNamespaceCatalog.listNamespaces().stream()
          .map(Namespace::levels)
          .toArray(String[][]::new);
    }

    return new String[0][];
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public Identifier[] listViews(String... namespace) {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public View loadView(Identifier ident) throws NoSuchViewException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public View createView(
      Identifier ident,
      String sql,
      String currentCatalog,
      String[] currentNamespace,
      StructType schema,
      String[] queryColumnNames,
      String[] columnAliases,
      String[] columnComments,
      Map<String, String> properties)
      throws ViewAlreadyExistsException, NoSuchNamespaceException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public View alterView(Identifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public boolean dropView(Identifier ident) {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void renameView(Identifier fromIdentifier, Identifier toIdentifier)
      throws NoSuchViewException, ViewAlreadyExistsException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public View replaceView(
      Identifier ident,
      String sql,
      String currentCatalog,
      String[] currentNamespace,
      StructType schema,
      String[] queryColumnNames,
      String[] columnAliases,
      String[] columnComments,
      Map<String, String> properties)
      throws NoSuchNamespaceException, NoSuchViewException {
    throw new RuntimeException("Not Implemented");
  }
}
