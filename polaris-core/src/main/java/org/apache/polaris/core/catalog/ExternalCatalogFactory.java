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
import org.apache.iceberg.catalog.Catalog;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.slf4j.LoggerFactory;

/**
 * Factory interface for creating external catalog handles based on connection configuration.
 *
 * <p>Implementations should be annotated with CDI annotations and use the @Identifier annotation to
 * specify which connection type they support.
 */
public interface ExternalCatalogFactory {

  /**
   * Creates a catalog handle for the given connection configuration.
   *
   * @deprecated Implement {@link #createCatalog(ConnectionConfigInfoDpo, PolarisCredentialManager,
   *     Map)} instead to support catalog properties pass-through (e.g., proxy settings).
   * @param connectionConfig the connection configuration
   * @param polarisCredentialManager the credential manager for generating connection credentials
   *     that Polaris uses to access external systems
   * @return the initialized catalog
   * @throws IllegalStateException if the connection configuration is invalid
   */
  @Deprecated
  Catalog createCatalog(
      ConnectionConfigInfoDpo connectionConfig, PolarisCredentialManager polarisCredentialManager);

  /**
   * Creates a catalog handle for the given connection configuration.
   *
   * <p>Implementations should override this method to receive catalog properties (e.g.,
   * rest.client.proxy.*, timeout settings). The default implementation delegates to the deprecated
   * 2-parameter version and logs a warning if properties are being ignored.
   *
   * @param connectionConfig the connection configuration
   * @param polarisCredentialManager the credential manager for generating connection credentials
   *     that Polaris uses to access external systems
   * @param catalogProperties additional properties from the ExternalCatalog entity that should be
   *     passed through to the underlying catalog (e.g., rest.client.proxy.*, timeout settings).
   *     These are merged with lower precedence than connection config properties.
   * @return the initialized catalog
   * @throws IllegalStateException if the connection configuration is invalid
   */
  default Catalog createCatalog(
      ConnectionConfigInfoDpo connectionConfig,
      PolarisCredentialManager polarisCredentialManager,
      Map<String, String> catalogProperties) {
    if (catalogProperties != null && !catalogProperties.isEmpty()) {
      LoggerFactory.getLogger(getClass())
          .warn(
              "catalogProperties were provided but {} does not override createCatalog with "
                  + "catalogProperties parameter. Properties will be ignored: {}. "
                  + "Consider upgrading the factory implementation.",
              getClass().getName(),
              catalogProperties.keySet());
    }
    return createCatalog(connectionConfig, polarisCredentialManager);
  }

  /**
   * Creates a generic table catalog for the given connection configuration.
   *
   * @deprecated Implement {@link #createGenericCatalog(ConnectionConfigInfoDpo,
   *     PolarisCredentialManager, Map)} instead to support catalog properties pass-through.
   * @param connectionConfig the connection configuration
   * @param polarisCredentialManager the credential manager for generating connection credentials
   *     that Polaris uses to access external systems
   * @return the initialized catalog
   * @throws IllegalStateException if the connection configuration is invalid
   */
  @Deprecated
  GenericTableCatalog createGenericCatalog(
      ConnectionConfigInfoDpo connectionConfig, PolarisCredentialManager polarisCredentialManager);

  /**
   * Creates a generic table catalog for the given connection configuration.
   *
   * <p>Implementations should override this method to receive catalog properties. The default
   * implementation delegates to the deprecated 2-parameter version and logs a warning if properties
   * are being ignored.
   *
   * @param connectionConfig the connection configuration
   * @param polarisCredentialManager the credential manager for generating connection credentials
   *     that Polaris uses to access external systems
   * @param catalogProperties additional properties from the ExternalCatalog entity that should be
   *     passed through to the underlying catalog
   * @return the initialized catalog
   * @throws IllegalStateException if the connection configuration is invalid
   */
  default GenericTableCatalog createGenericCatalog(
      ConnectionConfigInfoDpo connectionConfig,
      PolarisCredentialManager polarisCredentialManager,
      Map<String, String> catalogProperties) {
    if (catalogProperties != null && !catalogProperties.isEmpty()) {
      LoggerFactory.getLogger(getClass())
          .warn(
              "catalogProperties were provided but {} does not override createGenericCatalog with "
                  + "catalogProperties parameter. Properties will be ignored: {}. "
                  + "Consider upgrading the factory implementation.",
              getClass().getName(),
              catalogProperties.keySet());
    }
    return createGenericCatalog(connectionConfig, polarisCredentialManager);
  }
}
