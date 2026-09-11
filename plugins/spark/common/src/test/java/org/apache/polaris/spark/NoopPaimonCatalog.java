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
 * In-memory standalone catalog that mimics Paimon's ownership of physical tables independently of
 * the Polaris Generic Table registration.
 */
public class NoopPaimonCatalog implements TableCatalog, SupportsNamespaces, CatalogPlugin {
  private String catalogName;
  private final Map<String, Map<String, String>> namespaces = new HashMap<>();
  private final Map<String, NoopPaimonTable> tables = new HashMap<>();

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    String prefix = String.join(".", namespace) + ".";
    return tables.keySet().stream()
        .filter(key -> key.startsWith(prefix))
        .map(key -> Identifier.of(namespace, key.substring(prefix.length())))
        .toArray(Identifier[]::new);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    NoopPaimonTable table = tables.get(tableKey(ident));
    if (table == null) {
      throw new NoSuchTableException(ident);
    }
    return table;
  }

  @Override
  @SuppressWarnings({"deprecation", "RedundantSuppression"})
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException {
    String key = tableKey(ident);
    if (tables.containsKey(key)) {
      throw new TableAlreadyExistsException(ident);
    }
    Map<String, String> tableProperties = new HashMap<>(properties);
    NoopPaimonTable table = new NoopPaimonTable(ident.name(), schema, tableProperties);
    tables.put(key, table);
    return table;
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    return loadTable(ident);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return tables.remove(tableKey(ident)) != null;
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) throws NoSuchTableException {
    NoopPaimonTable table = tables.remove(tableKey(oldIdent));
    if (table == null) {
      throw new NoSuchTableException(oldIdent);
    }
    tables.put(tableKey(newIdent), table);
  }

  @Override
  public String[][] listNamespaces() {
    return namespaces.keySet().stream().map(ns -> new String[] {ns}).toArray(String[][]::new);
  }

  @Override
  public String[][] listNamespaces(String[] namespace) {
    return new String[0][];
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    Map<String, String> metadata = namespaces.get(String.join(".", namespace));
    if (metadata == null) {
      throw new NoSuchNamespaceException(namespace);
    }
    return metadata;
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    String key = String.join(".", namespace);
    if (namespaces.containsKey(key)) {
      throw new NamespaceAlreadyExistsException(namespace);
    }
    namespaces.put(key, new HashMap<>(metadata));
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) {
    return namespaces.remove(String.join(".", namespace)) != null;
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    return namespaces.containsKey(String.join(".", namespace));
  }

  private static String tableKey(Identifier ident) {
    return String.join(".", ident.namespace()) + "." + ident.name();
  }

  private static class NoopPaimonTable implements Table {
    private final String name;
    private final StructType schema;
    private final Map<String, String> properties;

    private NoopPaimonTable(String name, StructType schema, Map<String, String> properties) {
      this.name = name;
      this.schema = schema;
      this.properties = properties;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    @SuppressWarnings({"deprecation", "RedundantSuppression"})
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
