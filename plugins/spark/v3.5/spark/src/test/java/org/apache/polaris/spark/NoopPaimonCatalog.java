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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * This is a fake paimon catalog class that is used for testing. This class mimics the real Paimon
 * SparkCatalog which is a standalone catalog (not using DelegatingCatalogExtension). Paimon manages
 * its own namespaces and table metadata independently.
 */
public class NoopPaimonCatalog implements TableCatalog, SupportsNamespaces, CatalogPlugin {

  private String catalogName;
  private CaseInsensitiveStringMap options;
  private Map<String, Map<String, String>> namespaces = new HashMap<>();
  private Map<String, NoopPaimonTable> tables = new HashMap<>();

  @Override
  public void initialize(String name, CaseInsensitiveStringMap opts) {
    this.catalogName = name;
    this.options = opts;
  }

  @Override
  public String name() {
    return catalogName;
  }

  // TableCatalog methods

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    String nsKey = String.join(".", namespace);
    return tables.keySet().stream()
        .filter(key -> key.startsWith(nsKey + "."))
        .map(
            key -> {
              String tableName = key.substring(nsKey.length() + 1);
              return Identifier.of(namespace, tableName);
            })
        .toArray(Identifier[]::new);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    String key = getTableKey(ident);
    NoopPaimonTable table = tables.get(key);
    if (table == null) {
      throw new NoSuchTableException(ident);
    }
    return table;
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String key = getTableKey(ident);
    if (tables.containsKey(key)) {
      throw new TableAlreadyExistsException(ident);
    }
    NoopPaimonTable table = new NoopPaimonTable(ident.name(), schema, properties);
    tables.put(key, table);
    return table;
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    String key = getTableKey(ident);
    NoopPaimonTable table = tables.get(key);
    if (table == null) {
      throw new NoSuchTableException(ident);
    }
    return table;
  }

  @Override
  public boolean dropTable(Identifier ident) {
    String key = getTableKey(ident);
    return tables.remove(key) != null;
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) throws NoSuchTableException {
    String oldKey = getTableKey(oldIdent);
    NoopPaimonTable table = tables.remove(oldKey);
    if (table == null) {
      throw new NoSuchTableException(oldIdent);
    }
    String newKey = getTableKey(newIdent);
    tables.put(newKey, table);
  }

  private String getTableKey(Identifier ident) {
    return String.join(".", ident.namespace()) + "." + ident.name();
  }

  // SupportsNamespaces methods

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return namespaces.keySet().stream().map(ns -> new String[] {ns}).toArray(String[][]::new);
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return new String[0][];
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    String key = String.join(".", namespace);
    if (!namespaces.containsKey(key)) {
      throw new NoSuchNamespaceException(namespace);
    }
    return namespaces.get(key);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    String key = String.join(".", namespace);
    if (namespaces.containsKey(key)) {
      throw new NamespaceAlreadyExistsException(namespace);
    }
    namespaces.put(key, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    String key = String.join(".", namespace);
    if (!namespaces.containsKey(key)) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException {
    String key = String.join(".", namespace);
    return namespaces.remove(key) != null;
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    String key = String.join(".", namespace);
    return namespaces.containsKey(key);
  }

  /** A simple table implementation for testing. */
  private static class NoopPaimonTable implements Table {
    private final String name;
    private final StructType schema;
    private final Map<String, String> properties;

    NoopPaimonTable(String name, StructType schema, Map<String, String> properties) {
      this.name = name;
      this.schema = schema;
      this.properties = properties != null ? new HashMap<>(properties) : new HashMap<>();
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public StructType schema() {
      return schema;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public Set<TableCapability> capabilities() {
      return new HashSet<>();
    }
  }
}
