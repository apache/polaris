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

package org.apache.polaris.core.catalog;

import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;

/** Base interface for local Polaris Catalogs dealing with Iceberg tables. */
public interface LocalIcebergCatalog extends Catalog, ViewCatalog, SupportsNamespaces {
  @Override
  default String name() {
    return Catalog.super.name();
  }

  @Override
  default void initialize(String name, Map<String, String> properties) {
    Catalog.super.initialize(name, properties);
  }

  Page<Namespace> listNamespaces(Namespace namespace, PageToken pageToken);

  Page<TableIdentifier> listTables(Namespace namespace, PageToken pageToken);

  Page<TableIdentifier> listViews(Namespace namespace, PageToken pageToken);

  String transformTableLikeLocation(TableIdentifier tableIdentifier, String location);

  void validateStagedTableCreate(TableIdentifier tableIdentifier, TableMetadata tableMetadata);

  void setMetaStoreManager(PolarisMetaStoreManager newMetaStoreManager);
}
