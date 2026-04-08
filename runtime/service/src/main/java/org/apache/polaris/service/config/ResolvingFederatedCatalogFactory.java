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

package org.apache.polaris.service.config;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.polaris.core.catalog.FederatedCatalogFactory;
import org.apache.polaris.core.catalog.GenericTableCatalog;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.credentials.PolarisCredentialManager;

@ApplicationScoped
public class ResolvingFederatedCatalogFactory implements FederatedCatalogFactory {

  @Inject @Any Instance<FederatedCatalogFactory> factories;

  private FederatedCatalogFactory delegate(ConnectionConfigInfoDpo connectionConfig) {
    ConnectionType connectionType = connectionConfig.getConnectionType();
    Instance<FederatedCatalogFactory> federatedCatalogFactory =
        factories.select(Identifier.Literal.of(connectionType.getFactoryIdentifier()));
    if (federatedCatalogFactory.isResolvable()) {
      return federatedCatalogFactory.get();
    } else {
      throw new UnsupportedOperationException(
          "External catalog factory for type '" + connectionType + "' is unavailable.");
    }
  }

  @Override
  public Catalog createCatalog(
      ConnectionConfigInfoDpo connectionConfig,
      PolarisCredentialManager polarisCredentialManager,
      Map<String, String> catalogProperties) {
    return delegate(connectionConfig)
        .createCatalog(connectionConfig, polarisCredentialManager, catalogProperties);
  }

  @Override
  public GenericTableCatalog createGenericCatalog(
      ConnectionConfigInfoDpo connectionConfig,
      PolarisCredentialManager polarisCredentialManager,
      Map<String, String> catalogProperties) {
    return delegate(connectionConfig)
        .createGenericCatalog(connectionConfig, polarisCredentialManager, catalogProperties);
  }
}
