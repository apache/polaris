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
package org.apache.polaris.service.catalog.generic;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.table.GenericTableEntity;

/** A catalog for managing `GenericTableEntity` instances */
public interface GenericTableCatalog {

  /** Should be called before other methods */
  void initialize(String name, Map<String, String> properties);

  /** Create a generic table with the specified identifier */
  GenericTableEntity createGenericTable(
      TableIdentifier tableIdentifier,
      String format,
      String baseLocation,
      String doc,
      Map<String, String> properties);

  /** Retrieve a generic table entity with a given identifier */
  GenericTableEntity loadGenericTable(TableIdentifier tableIdentifier);

  /** Drop a generic table entity with a given identifier */
  boolean dropGenericTable(TableIdentifier tableIdentifier);

  /** List all generic tables under a specific namespace */
  List<TableIdentifier> listGenericTables(Namespace namespace);
}
