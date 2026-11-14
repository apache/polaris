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

import com.google.common.base.Strings;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.SigV4AuthenticationParameters;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;

/**
 * Rejects SigV4 {@code storageName} catalogs unless named server-side AWS credentials are enabled.
 */
@ApplicationScoped
public class SigV4CatalogValidator implements CatalogConfigValidator {
  private final RealmConfig realmConfig;

  @Inject
  public SigV4CatalogValidator(RealmConfig realmConfig) {
    this.realmConfig = realmConfig;
  }

  @Override
  public void validate(Catalog catalog) {
    if (!(catalog instanceof ExternalCatalog externalCatalog)) {
      return;
    }

    ConnectionConfigInfo connectionConfig = externalCatalog.getConnectionConfigInfo();
    if (connectionConfig == null
        || !(connectionConfig.getAuthenticationParameters()
            instanceof SigV4AuthenticationParameters sigV4)
        || Strings.isNullOrEmpty(sigV4.getStorageName())) {
      return;
    }

    FeatureConfiguration.enforceFeatureEnabledOrThrow(
        realmConfig, FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME);
  }
}
