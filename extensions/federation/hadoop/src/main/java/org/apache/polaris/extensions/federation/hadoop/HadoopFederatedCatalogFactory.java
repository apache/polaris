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
package org.apache.polaris.extensions.federation.hadoop;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.catalog.GenericTableCatalog;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.connection.hadoop.HadoopConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;

/** Factory class for creating a Hadoop catalog handle based on connection configuration. */
@ApplicationScoped
@Identifier(ConnectionType.HADOOP_FACTORY_IDENTIFIER)
public class HadoopFederatedCatalogFactory implements ExternalCatalogFactory {

  @Override
  public Catalog createCatalog(
      ConnectionConfigInfoDpo connectionConfigInfoDpo,
      PolarisCredentialManager polarisCredentialManager,
      Map<String, String> catalogProperties) {
    // Currently, Polaris supports Hadoop federation only via IMPLICIT authentication.
    // Hence, prior to initializing the configuration, ensure that the catalog uses
    // IMPLICIT authentication.
    AuthenticationParametersDpo authenticationParametersDpo =
        connectionConfigInfoDpo.getAuthenticationParameters();
    if (authenticationParametersDpo.getAuthenticationTypeCode()
        != AuthenticationType.IMPLICIT.getCode()) {
      throw new IllegalStateException("Hadoop federation only supports IMPLICIT authentication.");
    }
    String warehouse = ((HadoopConnectionConfigInfoDpo) connectionConfigInfoDpo).getWarehouse();
    Map<String, String> mergedProperties =
        RESTUtil.merge(
            catalogProperties != null ? catalogProperties : Map.of(),
            connectionConfigInfoDpo.asIcebergCatalogProperties(polarisCredentialManager));

    // Use no-arg constructor + setConf + initialize pattern to avoid double initialization.
    // The HadoopCatalog(conf, warehouse) constructor internally calls initialize(), so using
    // it followed by another initialize() call would be redundant.
    HadoopCatalog hadoopCatalog = new HadoopCatalog();
    hadoopCatalog.setConf(new Configuration());
    hadoopCatalog.initialize(warehouse, mergedProperties);
    return hadoopCatalog;
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
