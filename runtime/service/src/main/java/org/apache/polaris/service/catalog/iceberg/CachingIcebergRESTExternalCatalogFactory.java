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

package org.apache.polaris.service.catalog.iceberg;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.catalog.GenericTableCatalog;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;

public class CachingIcebergRESTExternalCatalogFactory implements ExternalCatalogFactory {

  @Override
  public Catalog createCatalog(
      ConnectionConfigInfoDpo connectionConfig,
      PolarisCredentialManager polarisCredentialManager,
      Map<String, String> catalogProperties) {
    if (!(connectionConfig instanceof IcebergRestConnectionConfigInfoDpo icebergConfig)) {
      throw new IllegalArgumentException(
          "Expected IcebergRestConnectionConfigInfoDpo but got: "
              + connectionConfig.getClass().getSimpleName());
    }

    SessionCatalog.SessionContext context = SessionCatalog.SessionContext.createEmpty();
    RESTCatalog federatedCatalog =
        new RESTCatalog(
            context,
            (config) ->
                HTTPClient.builder(config)
                    .uri(config.get(org.apache.iceberg.CatalogProperties.URI))
                    .build());

    // Merge properties with precedence: connection config properties override catalog properties
    // to ensure required settings like URI and authentication cannot be accidentally overwritten.
    Map<String, String> mergedProperties =
        RESTUtil.merge(
            catalogProperties != null ? catalogProperties : Map.of(),
            connectionConfig.asIcebergCatalogProperties(polarisCredentialManager));

    federatedCatalog.initialize(icebergConfig.getRemoteCatalogName(), mergedProperties);

    return new CachingIcebergCatalog(federatedCatalog, null);
  }

  @Override
  public GenericTableCatalog createGenericCatalog(
      ConnectionConfigInfoDpo connectionConfig,
      PolarisCredentialManager polarisCredentialManager,
      Map<String, String> catalogProperties) {
    // TODO implement
    throw new UnsupportedOperationException(
        "Generic table federation to this catalog is not supported.");
  }

  private static class CachingIcebergCatalog implements Catalog {
    private final RESTCatalog catalog;
    private final IcebergCatalog icebergCatalog;

    private CachingIcebergCatalog(RESTCatalog catalog, IcebergCatalog icebergCatalog) {
      this.catalog = catalog;
      this.icebergCatalog = icebergCatalog;
    }

    @Override
    public String name() {
      return icebergCatalog.name();
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return icebergCatalog.listTables(namespace);
    }

    @Override
    public Table createTable(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec spec,
        String location,
        Map<String, String> properties) {
      return icebergCatalog.createTable(identifier, schema, spec, location, properties);
    }

    @Override
    public Table createTable(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec spec,
        Map<String, String> properties) {
      return icebergCatalog.createTable(identifier, schema, spec, properties);
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
      return Catalog.super.createTable(identifier, schema, spec);
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema) {
      return Catalog.super.createTable(identifier, schema);
    }

    @Override
    public Transaction newCreateTableTransaction(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec spec,
        String location,
        Map<String, String> properties) {
      return Catalog.super.newCreateTableTransaction(
          identifier, schema, spec, location, properties);
    }

    @Override
    public Transaction newCreateTableTransaction(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec spec,
        Map<String, String> properties) {
      return Catalog.super.newCreateTableTransaction(identifier, schema, spec, properties);
    }

    @Override
    public Transaction newCreateTableTransaction(
        TableIdentifier identifier, Schema schema, PartitionSpec spec) {
      return Catalog.super.newCreateTableTransaction(identifier, schema, spec);
    }

    @Override
    public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema) {
      return Catalog.super.newCreateTableTransaction(identifier, schema);
    }

    @Override
    public Transaction newReplaceTableTransaction(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec spec,
        String location,
        Map<String, String> properties,
        boolean orCreate) {
      return Catalog.super.newReplaceTableTransaction(
          identifier, schema, spec, location, properties, orCreate);
    }

    @Override
    public Transaction newReplaceTableTransaction(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec spec,
        Map<String, String> properties,
        boolean orCreate) {
      return Catalog.super.newReplaceTableTransaction(
          identifier, schema, spec, properties, orCreate);
    }

    @Override
    public Transaction newReplaceTableTransaction(
        TableIdentifier identifier, Schema schema, PartitionSpec spec, boolean orCreate) {
      return Catalog.super.newReplaceTableTransaction(identifier, schema, spec, orCreate);
    }

    @Override
    public Transaction newReplaceTableTransaction(
        TableIdentifier identifier, Schema schema, boolean orCreate) {
      return Catalog.super.newReplaceTableTransaction(identifier, schema, orCreate);
    }

    @Override
    public boolean tableExists(TableIdentifier identifier) {
      return Catalog.super.tableExists(identifier);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier) {
      return Catalog.super.dropTable(identifier);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {}

    @Override
    public Table loadTable(TableIdentifier identifier) {
      return null;
    }

    @Override
    public void invalidateTable(TableIdentifier identifier) {
      Catalog.super.invalidateTable(identifier);
    }

    @Override
    public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
      return Catalog.super.registerTable(identifier, metadataFileLocation);
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
      return Catalog.super.buildTable(identifier, schema);
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
      Catalog.super.initialize(name, properties);
    }
  }
}
