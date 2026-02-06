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
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.catalog.GenericTableCatalog;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;

/** Factory class for creating an Iceberg REST catalog handle based on connection configuration. */
@ApplicationScoped
@Identifier(ConnectionType.ICEBERG_REST_FACTORY_IDENTIFIER)
public class IcebergRESTExternalCatalogFactory implements ExternalCatalogFactory {

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
            (config) -> {
              return HTTPClient.builder(config)
                  .withHeaders(RESTUtil.configHeaders(config))
                  .uri(config.get(org.apache.iceberg.CatalogProperties.URI))
                  .build();
            });

    // Merge properties with precedence: connection config properties override catalog properties
    // to ensure required settings like URI and authentication cannot be accidentally overwritten.
    Map<String, String> mergedProperties =
        RESTUtil.merge(
            catalogProperties != null ? catalogProperties : Map.of(),
            connectionConfig.asIcebergCatalogProperties(polarisCredentialManager));

    federatedCatalog.initialize(icebergConfig.getRemoteCatalogName(), mergedProperties);

    return federatedCatalog;
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
}
