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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.spark.SupportsReplaceView;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.ViewChange;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.polaris.spark.utils.DeltaHelper;
import org.apache.polaris.spark.utils.PolarisCatalogUtils;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * SparkCatalog Implementation that is able to interact with both Iceberg SparkCatalog and Polaris
 * SparkCatalog. All namespaces and view related operations continue goes through the Iceberg
 * SparkCatalog. For table operations, depends on the table format, the operation can be achieved
 * with interaction with both Iceberg and Polaris SparkCatalog.
 */
public class SparkCatalog
    implements StagingTableCatalog,
        TableCatalog,
        SupportsNamespaces,
        ViewCatalog,
        SupportsReplaceView {
  private static final Logger LOG = LoggerFactory.getLogger(SparkCatalog.class);

  private String catalogName = null;
  private org.apache.iceberg.spark.SparkCatalog icebergsSparkCatalog = null;
  private PolarisSparkCatalog polarisSparkCatalog = null;

  protected DeltaHelper deltaHelper = null;

  @Override
  public String name() {
    return catalogName;
  }

  /**
   * Initialize REST Catalog for Iceberg and Polaris, this is the only catalog type supported by
   * Polaris at this moment.
   */
  private void initRESTCatalog(String name, CaseInsensitiveStringMap options) {
    // TODO: relax this in the future
    String catalogType =
        PropertyUtil.propertyAsString(
            options, CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    if (!catalogType.equals(CatalogUtil.ICEBERG_CATALOG_TYPE_REST)) {
      throw new UnsupportedOperationException(
          "Only rest catalog type is supported, but got catalog type: " + catalogType);
    }

    String catalogImpl = options.get(CatalogProperties.CATALOG_IMPL);
    if (catalogImpl != null) {
      throw new UnsupportedOperationException(
          "Customized catalog implementation is currently not supported!");
    }

    // initialize the icebergSparkCatalog
    this.icebergsSparkCatalog = new org.apache.iceberg.spark.SparkCatalog();
    this.icebergsSparkCatalog.initialize(name, options);

    // initialize the polaris spark catalog
    OAuth2Util.AuthSession catalogAuth =
        PolarisCatalogUtils.getAuthSession(this.icebergsSparkCatalog);
    PolarisRESTCatalog restCatalog = new PolarisRESTCatalog();
    restCatalog.initialize(options, catalogAuth);
    this.polarisSparkCatalog = new PolarisSparkCatalog(restCatalog);
    this.polarisSparkCatalog.initialize(name, options);
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    initRESTCatalog(name, options);
    this.deltaHelper = new DeltaHelper(options);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      return this.icebergsSparkCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      return this.polarisSparkCatalog.loadTable(ident);
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String provider = properties.get(PolarisCatalogUtils.TABLE_PROVIDER_KEY);
    if (PolarisCatalogUtils.useIceberg(provider)) {
      return this.icebergsSparkCatalog.createTable(ident, schema, transforms, properties);
    } else if (PolarisCatalogUtils.useDelta(provider)) {
      // For delta table, we load the delta catalog to help dealing with the
      // delta log creation.
      TableCatalog deltaCatalog = deltaHelper.loadDeltaCatalog(this.polarisSparkCatalog);
      return deltaCatalog.createTable(ident, schema, transforms, properties);
    } else {
      return this.polarisSparkCatalog.createTable(ident, schema, transforms, properties);
    }
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
