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
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.spark.utils.PolarisCatalogUtils;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A spark TableCatalog Implementation interacts with Polaris specific APIs only. The APIs it
 * interacts with is generic table APIs, and all table operations performed in this class are
 * expected to be for non-iceberg tables.
 */
public class PolarisSparkCatalog implements TableCatalog {

  private PolarisCatalog polarisCatalog = null;
  private String catalogName = null;

  public PolarisSparkCatalog(PolarisCatalog polarisCatalog) {
    this.polarisCatalog = polarisCatalog;
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Table loadTable(Identifier identifier) throws NoSuchTableException {
    try {
      GenericTable genericTable =
          this.polarisCatalog.loadGenericTable(Spark3Util.identifierToTableIdentifier(identifier));
      return PolarisCatalogUtils.loadSparkTable(genericTable);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(identifier);
    }
  }

  @Override
  public Table createTable(
      Identifier identifier,
      StructType schema,
      Transform[] transforms,
      Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    try {
      String format = properties.get(PolarisCatalogUtils.TABLE_PROVIDER_KEY);
      GenericTable genericTable =
          this.polarisCatalog.createGenericTable(
              Spark3Util.identifierToTableIdentifier(identifier), format, null, properties);
      return PolarisCatalogUtils.loadSparkTable(genericTable);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(identifier);
    }
  }

  @Override
  public Table alterTable(Identifier identifier, TableChange... changes)
      throws NoSuchTableException {
    throw new NoSuchTableException(identifier);
  }

  @Override
  public boolean dropTable(Identifier identifier) {
    return false;
  }

  @Override
  public void renameTable(Identifier from, Identifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("renameTable operation is not supported");
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    throw new UnsupportedOperationException("listTables operation is not supported");
  }
}
