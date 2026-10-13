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
import java.net.URI;
import java.util.Map;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;

@ApplicationScoped
public class BigLakeCatalogValidator implements CatalogConfigValidator {
  private static final String BIGLAKE_HOST = "biglake.googleapis.com";
  private static final String BIGLAKE_PATH = "/iceberg/v1/restcatalog";
  private static final String DEFAULT_BASE_LOCATION_KEY = "default-base-location";
  private static final String QUOTA_PROJECT_HEADER = "header.x-goog-user-project";

  @Override
  public void validate(RealmConfig realmConfig, Catalog catalog) {
    if (!(catalog instanceof ExternalCatalog externalCatalog)) {
      return;
    }

    if (!(externalCatalog.getConnectionConfigInfo()
        instanceof IcebergRestConnectionConfigInfo connectionConfig)) {
      return;
    }

    // BigLake uses ICEBERG_REST with GCP authentication. This is distinct from the BIGQUERY
    // connection type used by the optional BigQuery Metastore federation extension.
    if (connectionConfig.getAuthenticationParameters() == null
        || connectionConfig.getAuthenticationParameters().getAuthenticationType()
            != AuthenticationParameters.AuthenticationTypeEnum.GCP) {
      return;
    }

    if (!isBigLakeEndpoint(connectionConfig.getUri())) {
      return;
    }

    validateBigLakeRemoteCatalogName(connectionConfig.getRemoteCatalogName());
    validateBigLakeHeaders(
        connectionConfig.getProperties(), externalCatalog.getProperties().toMap());
    validateBigLakeStorageConfiguration(realmConfig, externalCatalog);
  }

  private static void validateBigLakeRemoteCatalogName(String remoteCatalogName) {
    if (Strings.isNullOrEmpty(remoteCatalogName) || remoteCatalogName.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid BigLake connectionConfigInfo.remoteCatalogName: a remote catalog or warehouse identifier is required.");
    }
  }

  private static void validateBigLakeHeaders(
      Map<String, String> connectionProperties, Map<String, String> catalogProperties) {
    String quotaProject = findIgnoreCase(connectionProperties, QUOTA_PROJECT_HEADER);
    if (Strings.isNullOrEmpty(quotaProject)) {
      quotaProject = findIgnoreCase(catalogProperties, QUOTA_PROJECT_HEADER);
    }
    if (Strings.isNullOrEmpty(quotaProject) || quotaProject.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid BigLake catalog properties entry '"
              + QUOTA_PROJECT_HEADER
              + "': a quota project is required.");
    }
  }

  private static void validateBigLakeStorageConfiguration(
      RealmConfig realmConfig, ExternalCatalog externalCatalog) {
    boolean credentialVendingEnabled =
        realmConfig.getConfig(
                FeatureConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING,
                externalCatalog.getProperties().toMap())
            && realmConfig.getConfig(
                FeatureConfiguration.ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING,
                externalCatalog.getProperties().toMap());

    if (!credentialVendingEnabled) {
      return;
    }

    StorageConfigInfo storageConfigInfo = externalCatalog.getStorageConfigInfo();
    if (storageConfigInfo == null
        || storageConfigInfo.getStorageType() != StorageConfigInfo.StorageTypeEnum.GCS
        || !(storageConfigInfo instanceof GcpStorageConfigInfo gcpStorageConfigInfo)) {
      throw new IllegalArgumentException(
          "Invalid BigLake storageConfigInfo: GCS storage configuration is required when credential vending is enabled.");
    }

    requireValue(
        "catalog.properties." + DEFAULT_BASE_LOCATION_KEY,
        externalCatalog.getProperties().toMap().get(DEFAULT_BASE_LOCATION_KEY));
    String serviceAccount = gcpStorageConfigInfo.getGcsServiceAccount();
    if (Strings.isNullOrEmpty(serviceAccount) || serviceAccount.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid BigLake storageConfigInfo.gcsServiceAccount: a value is required when credential vending is enabled.");
    }
  }

  private static void requireValue(String fieldName, String value) {
    if (Strings.isNullOrEmpty(value) || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Invalid BigLake " + fieldName + ": a value is required.");
    }
  }

  private static boolean isBigLakeEndpoint(String uriString) {
    if (Strings.isNullOrEmpty(uriString)) {
      return false;
    }

    String normalizedUri =
        uriString.endsWith("/") ? uriString.substring(0, uriString.length() - 1) : uriString;
    try {
      URI uri = URI.create(normalizedUri);
      return "https".equalsIgnoreCase(uri.getScheme())
          && BIGLAKE_HOST.equalsIgnoreCase(uri.getHost())
          && BIGLAKE_PATH.equals(uri.getPath())
          && uri.getPort() == -1
          && uri.getRawQuery() == null
          && uri.getRawFragment() == null;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static String findIgnoreCase(Map<String, String> properties, String name) {
    if (properties == null) {
      return null;
    }
    return properties.entrySet().stream()
        .filter(entry -> name.equalsIgnoreCase(entry.getKey()))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(null);
  }
}
