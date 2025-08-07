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
package org.apache.polaris.federator;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.connection.hadoop.HadoopConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for creating federated catalogs based on connection configuration. Currently
 * supports Iceberg REST and Hadoop catalogs.
 */
public class FederatedCatalogFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(FederatedCatalogFactory.class);

  /**
   * Creates a federated catalog based on the provided connection configuration.
   *
   * @param connectionConfigInfoDpo The connection configuration
   * @param userSecretsManager The user secrets manager for handling credentials
   * @return The initialized federated catalog
   * @throws UnsupportedOperationException if the connection type is not supported
   */
  public static Catalog createFederatedCatalog(
      ConnectionConfigInfoDpo connectionConfigInfoDpo, UserSecretsManager userSecretsManager) {

    ConnectionType connectionType =
        ConnectionType.fromCode(connectionConfigInfoDpo.getConnectionTypeCode());

    Catalog federatedCatalog;

    switch (connectionType) {
      case ICEBERG_REST:
        federatedCatalog = createIcebergRestCatalog(connectionConfigInfoDpo, userSecretsManager);
        break;
      case HADOOP:
        federatedCatalog = createHadoopCatalog(connectionConfigInfoDpo, userSecretsManager);
        break;
      default:
        throw new UnsupportedOperationException(
            "Connection type not supported for federation: " + connectionType);
    }

    return federatedCatalog;
  }

  private static Catalog createIcebergRestCatalog(
      ConnectionConfigInfoDpo connectionConfigInfoDpo, UserSecretsManager userSecretsManager) {
    SessionCatalog.SessionContext context = SessionCatalog.SessionContext.createEmpty();
    RESTCatalog restCatalog =
        new RESTCatalog(
            context,
            (config) ->
                HTTPClient.builder(config)
                    .uri(config.get(org.apache.iceberg.CatalogProperties.URI))
                    .build());

    IcebergRestConnectionConfigInfoDpo icebergRestConfig =
        (IcebergRestConnectionConfigInfoDpo) connectionConfigInfoDpo;

    restCatalog.initialize(
        icebergRestConfig.getRemoteCatalogName(),
        connectionConfigInfoDpo.asIcebergCatalogProperties(userSecretsManager));

    return restCatalog;
  }

  private static Catalog createHadoopCatalog(
      ConnectionConfigInfoDpo connectionConfigInfoDpo, UserSecretsManager userSecretsManager) {
    HadoopCatalog hadoopCatalog = new HadoopCatalog();

    HadoopConnectionConfigInfoDpo hadoopConfig =
        (HadoopConnectionConfigInfoDpo) connectionConfigInfoDpo;

    hadoopCatalog.initialize(
        hadoopConfig.getWarehouse(),
        connectionConfigInfoDpo.asIcebergCatalogProperties(userSecretsManager));

    return hadoopCatalog;
  }
}
