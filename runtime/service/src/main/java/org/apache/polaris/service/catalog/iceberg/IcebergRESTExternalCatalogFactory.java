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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.apache.polaris.core.secrets.UserSecretsManager;

/** Factory class for creating an Iceberg REST catalog handle based on connection configuration. */
@ApplicationScoped
@Identifier(ConnectionType.ICEBERG_REST_FACTORY_IDENTIFIER)
public class IcebergRESTExternalCatalogFactory implements ExternalCatalogFactory {

  @Override
  public Catalog createCatalog(
      ConnectionConfigInfoDpo connectionConfig, UserSecretsManager userSecretsManager) {
    if (!(connectionConfig instanceof IcebergRestConnectionConfigInfoDpo)) {
      throw new IllegalArgumentException(
          "Expected IcebergRestConnectionConfigInfoDpo but got: "
              + connectionConfig.getClass().getSimpleName());
    }

    IcebergRestConnectionConfigInfoDpo icebergConfig =
        (IcebergRestConnectionConfigInfoDpo) connectionConfig;

    SessionCatalog.SessionContext context = SessionCatalog.SessionContext.createEmpty();
    RESTCatalog federatedCatalog =
        new RESTCatalog(
            context,
            (config) ->
                HTTPClient.builder(config)
                    .uri(config.get(org.apache.iceberg.CatalogProperties.URI))
                    .build());

    federatedCatalog.initialize(
        icebergConfig.getRemoteCatalogName(),
        connectionConfig.asIcebergCatalogProperties(userSecretsManager));

    return federatedCatalog;
  }
}
