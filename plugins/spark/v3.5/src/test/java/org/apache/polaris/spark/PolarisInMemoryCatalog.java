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

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.polaris.service.types.GenericTable;

/** InMemory implementation for the Polaris Catalog. This class is mainly used by testing. */
public class PolarisInMemoryCatalog extends InMemoryCatalog implements PolarisCatalog {
  private final ConcurrentMap<TableIdentifier, GenericTable> genericTables;

  public PolarisInMemoryCatalog() {
    this.genericTables = Maps.newConcurrentMap();
  }

  @Override
  public List<TableIdentifier> listGenericTables(Namespace ns) {
    return this.genericTables.keySet().stream()
        .filter(t -> t.namespace().equals(ns))
        .sorted(Comparator.comparing(TableIdentifier::toString))
        .collect(Collectors.toList());
  }

  @Override
  public GenericTable loadGenericTable(TableIdentifier identifier) {
    GenericTable table = this.genericTables.get(identifier);
    if (table == null) {
      throw new NoSuchTableException("Generic table does not exist: %s", identifier);
    }

    return table;
  }

  @Override
  public boolean dropGenericTable(TableIdentifier identifier) {
    synchronized (this) {
      if (null == this.genericTables.remove(identifier)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public GenericTable createGenericTable(
      TableIdentifier identifier, String format, Map<String, String> props) {
    synchronized (this) {
      if (!namespaceExists(identifier.namespace())) {
        throw new NoSuchNamespaceException(
            "Cannot create generic table %s. Namespace does not exist: %s",
            identifier, identifier.namespace());
      }
      if (listViews(identifier.namespace()).contains(identifier)) {
        throw new AlreadyExistsException("View with same name already exists: %s", identifier);
      }
      if (listTables(identifier.namespace()).contains(identifier)) {
        throw new AlreadyExistsException(
            "Iceberg table  with same name already exists: %s", identifier);
      }
      if (this.genericTables.containsKey(identifier)) {
        throw new AlreadyExistsException("Generic table %s already exists", identifier);
      }
      this.genericTables.compute(
          identifier,
          (k, table) -> {
            return GenericTable.builder()
                .setName(k.name())
                .setFormat(format)
                .setProperties(props)
                .build();
          });
    }

    return this.genericTables.get(identifier);
  }
}
