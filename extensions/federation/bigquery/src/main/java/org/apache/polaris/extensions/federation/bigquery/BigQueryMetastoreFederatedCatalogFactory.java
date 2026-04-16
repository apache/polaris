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
package org.apache.polaris.extensions.federation.bigquery;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.catalog.FederatedCatalogFactory;
import org.apache.polaris.core.catalog.GenericTableCatalog;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.credentials.PolarisCredentialManager;

/**
 * Factory class for creating a BigQuery Metastore catalog handle based on connection configuration.
 */
@ApplicationScoped
@Identifier(ConnectionType.BIGQUERY_FACTORY_IDENTIFIER)
public class BigQueryMetastoreFederatedCatalogFactory implements FederatedCatalogFactory {

  @Override
  public Catalog createCatalog(
      ConnectionConfigInfoDpo connectionConfigInfoDpo,
      PolarisCredentialManager polarisCredentialManager,
      Map<String, String> catalogProperties) {

    // Currently, Polaris supports BigQuery Metastore federation only via IMPLICIT authentication.
    // Hence, prior to initializing the configuration, ensure that the catalog uses
    // IMPLICIT authentication.
    AuthenticationParametersDpo authenticationParametersDpo =
        connectionConfigInfoDpo.getAuthenticationParameters();
    if (authenticationParametersDpo.getAuthenticationTypeCode()
        != AuthenticationType.IMPLICIT.getCode()) {
      throw new IllegalStateException(
          "BigQuery Metastore federation only supports IMPLICIT authentication.");
    }

    Map<String, String> properties = connectionConfigInfoDpo.getProperties();
    String warehouse = properties.get("warehouse");
    if (warehouse == null || warehouse.isEmpty()) {
      throw new IllegalArgumentException("warehouse is required for BigQuery Metastore federation");
    }
    if (properties.get("gcp.bigquery.project-id") == null) {
      throw new IllegalArgumentException(
          "gcp.bigquery.project-id is required for BigQuery Metastore federation");
    }
    Map<String, String> mergedProperties =
        RESTUtil.merge(
            catalogProperties != null ? catalogProperties : Map.of(),
            connectionConfigInfoDpo.asIcebergCatalogProperties(polarisCredentialManager));

    // Credentials are resolved via Google Application Default Credentials (ADC).
    // GCS storage operations use the same ADC credentials by default.
    BigQueryMetastoreCatalog bigQueryMetastoreCatalog = new BigQueryMetastoreCatalog();
    bigQueryMetastoreCatalog.initialize(warehouse, mergedProperties);

    return bigQueryMetastoreCatalog;
  }

  @Override
  public GenericTableCatalog createGenericCatalog(
      ConnectionConfigInfoDpo connectionConfig,
      PolarisCredentialManager polarisCredentialManager,
      Map<String, String> catalogProperties) {
    throw new UnsupportedOperationException(
        "Generic table federation to BigQuery Metastore is not supported.");
  }
}
