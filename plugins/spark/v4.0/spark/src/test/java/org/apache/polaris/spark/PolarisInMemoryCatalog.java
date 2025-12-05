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

import com.google.common.collect.Maps;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.polaris.spark.rest.GenericTable;

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
    return null != this.genericTables.remove(identifier);
  }

  @Override
  public GenericTable createGenericTable(
      TableIdentifier identifier,
      String format,
      String baseLocation,
      String doc,
      Map<String, String> props) {
    if (!namespaceExists(identifier.namespace())) {
      throw new NoSuchNamespaceException(
          "Cannot create generic table %s. Namespace does not exist: %s",
          identifier, identifier.namespace());
    }

    GenericTable previous =
        this.genericTables.putIfAbsent(
            identifier,
            GenericTable.builder()
                .setName(identifier.name())
                .setFormat(format)
                .setBaseLocation(baseLocation)
                .setProperties(props)
                .build());

    if (previous != null) {
      throw new AlreadyExistsException("Generic table already exists: %s", identifier);
    }

    return this.genericTables.get(identifier);
  }
}
