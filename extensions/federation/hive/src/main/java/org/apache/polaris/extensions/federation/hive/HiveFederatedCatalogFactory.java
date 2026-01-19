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
package org.apache.polaris.extensions.federation.hive;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.catalog.GenericTableCatalog;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.connection.hive.HiveConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;

/** Factory class for creating a Hive catalog handle based on connection configuration. */
@ApplicationScoped
@Identifier(ConnectionType.HIVE_FACTORY_IDENTIFIER)
public class HiveFederatedCatalogFactory implements ExternalCatalogFactory {

  @Override
  public Catalog createCatalog(
      ConnectionConfigInfoDpo connectionConfigInfoDpo,
      PolarisCredentialManager polarisCredentialManager,
      Map<String, String> catalogProperties) {
    // Currently, Polaris supports Hive federation only via IMPLICIT authentication.
    // Hence, prior to initializing the configuration, ensure that the catalog uses
    // IMPLICIT authentication.
    AuthenticationParametersDpo authenticationParametersDpo =
        connectionConfigInfoDpo.getAuthenticationParameters();
    if (authenticationParametersDpo.getAuthenticationTypeCode()
        != AuthenticationType.IMPLICIT.getCode()) {
      throw new IllegalStateException("Hive federation only supports IMPLICIT authentication.");
    }
    String warehouse = ((HiveConnectionConfigInfoDpo) connectionConfigInfoDpo).getWarehouse();
    // Unlike Hadoop, HiveCatalog does not require us to create a Configuration object, the iceberg
    // rest library find the default configuration by reading hive-site.xml in the classpath
    // (including HADOOP_CONF_DIR classpath).

    // TODO: In the future, we could support multiple HiveCatalog instances based on polaris/catalog
    // properties.
    // A brief set of setps involved (and the options):
    // 1. Create a configuration without default properties.
    //  `Configuration conf = new Configuration(boolean loadDefaults=false);`
    // 2a. Specify the hive-site.xml file path in the configuration.
    //  `conf.addResource(new Path(hiveSiteXmlPath));`
    // 2b. Specify individual properties in the configuration.
    //  `conf.set(property, value);`
    // Polaris could support federating to multiple LDAP based Hive metastores. Multiple
    // Kerberos instances are not suitable because Kerberos ties a single identity to the server.
    HiveCatalog hiveCatalog = new HiveCatalog();
    Map<String, String> mergedProperties =
        RESTUtil.merge(
            catalogProperties != null ? catalogProperties : Map.of(),
            connectionConfigInfoDpo.asIcebergCatalogProperties(polarisCredentialManager));
    hiveCatalog.initialize(warehouse, mergedProperties);
    return hiveCatalog;
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
