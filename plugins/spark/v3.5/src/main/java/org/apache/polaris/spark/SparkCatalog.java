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
import org.apache.iceberg.spark.SupportsReplaceView;
import org.apache.spark.sql.catalyst.analysis.*;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkCatalog
    implements StagingTableCatalog,
        TableCatalog,
        SupportsNamespaces,
        ViewCatalog,
        SupportsReplaceView {

  private static final Set<String> DEFAULT_NS_KEYS = ImmutableSet.of(TableCatalog.PROP_OWNER);
  private String catalogName = null;
  private org.apache.iceberg.spark.SparkCatalog icebergsSparkCatalog = null;

  // TODO: Add Polaris Specific REST Catalog

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.icebergsSparkCatalog = new org.apache.iceberg.spark.SparkCatalog();
    this.icebergsSparkCatalog.initialize(name, options);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    throw new UnsupportedOperationException("loadTable");
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    throw new UnsupportedOperationException("createTable");
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new UnsupportedOperationException("alterTable");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    throw new UnsupportedOperationException("dropTable");
  }

  @Override
  public void renameTable(Identifier from, Identifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("renameTable");
  }

  @Override
  public void invalidateTable(Identifier ident) {
    throw new UnsupportedOperationException("invalidateTable");
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    throw new UnsupportedOperationException("purgeTable");
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    throw new UnsupportedOperationException("listTables");
  }

  @Override
  public StagedTable stageCreate(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    return this.icebergsSparkCatalog.stageCreate(ident, schema, transforms, properties);
  }

  @Override
  public StagedTable stageReplace(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws NoSuchTableException {
    return this.icebergsSparkCatalog.stageReplace(ident, schema, transforms, properties);
  }

  @Override
  public StagedTable stageCreateOrReplace(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties) {
    return this.icebergsSparkCatalog.stageCreateOrReplace(ident, schema, transforms, properties);
  }

  @Override
  public String[] defaultNamespace() {
    return this.icebergsSparkCatalog.defaultNamespace();
  }

  @Override
  public String[][] listNamespaces() {
    return this.icebergsSparkCatalog.listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return this.icebergsSparkCatalog.listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    return this.icebergsSparkCatalog.loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    this.icebergsSparkCatalog.createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    this.icebergsSparkCatalog.alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException {
    return this.icebergsSparkCatalog.dropNamespace(namespace, cascade);
  }

  @Override
  public Identifier[] listViews(String... namespace) {
    return this.icebergsSparkCatalog.listViews(namespace);
  }

  @Override
  public View loadView(Identifier ident) throws NoSuchViewException {
    return this.icebergsSparkCatalog.loadView(ident);
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
    return this.icebergsSparkCatalog.createView(
        ident,
        sql,
        currentCatalog,
        currentNamespace,
        schema,
        queryColumnNames,
        columnAliases,
        columnComments,
        properties);
  }

  @Override
  public View alterView(Identifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    return this.icebergsSparkCatalog.alterView(ident, changes);
  }

  @Override
  public boolean dropView(Identifier ident) {
    return this.icebergsSparkCatalog.dropView(ident);
  }

  @Override
  public void renameView(Identifier fromIdentifier, Identifier toIdentifier)
      throws NoSuchViewException, ViewAlreadyExistsException {
    this.icebergsSparkCatalog.renameView(fromIdentifier, toIdentifier);
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
    return this.icebergsSparkCatalog.replaceView(
        ident,
        sql,
        currentCatalog,
        currentNamespace,
        schema,
        queryColumnNames,
        columnAliases,
        columnComments,
        properties);
  }
}
