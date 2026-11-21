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
package org.apache.polaris.service.admin;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Set;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;

/**
 * Rejects federated catalogs whose connection type only works with {@code IMPLICIT} authentication
 * but that request another authentication type. The Hadoop, Hive and BigQuery Metastore federation
 * factories refuse any other type when the catalog is first used, so such a catalog could be
 * created but never used.
 */
@ApplicationScoped
public class ImplicitOnlyConnectionValidator implements CatalogConfigValidator {

  private static final Set<ConnectionConfigInfo.ConnectionTypeEnum> IMPLICIT_ONLY =
      Set.of(
          ConnectionConfigInfo.ConnectionTypeEnum.HADOOP,
          ConnectionConfigInfo.ConnectionTypeEnum.HIVE,
          ConnectionConfigInfo.ConnectionTypeEnum.BIGQUERY);

  @Override
  public void validate(Catalog catalog) {
    if (!(catalog instanceof ExternalCatalog externalCatalog)) {
      return;
    }
    ConnectionConfigInfo connection = externalCatalog.getConnectionConfigInfo();
    if (connection == null || !IMPLICIT_ONLY.contains(connection.getConnectionType())) {
      return;
    }
    // A missing value is reported by the request validation that requires it.
    AuthenticationParameters authentication = connection.getAuthenticationParameters();
    if (authentication == null
        || authentication.getAuthenticationType()
            == AuthenticationParameters.AuthenticationTypeEnum.IMPLICIT) {
      return;
    }
    throw new IllegalArgumentException(
        connection.getConnectionType().name()
            + " connections only support IMPLICIT authentication, but "
            + authentication.getAuthenticationType().name()
            + " was requested");
  }
}
