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
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.StorageUri;

final class BigLakeCatalogValidator {
  private static final String BIGLAKE_HOST = "biglake.googleapis.com";
  private static final String BIGLAKE_PATH = "/iceberg/v1/restcatalog";
  private static final String DEFAULT_BASE_LOCATION_KEY = "default-base-location";
  private static final String QUOTA_PROJECT_HEADER = "header.x-goog-user-project";

  private static final Pattern GCP_PROJECT_ID_PATTERN =
      Pattern.compile("^[a-z][a-z0-9-]{4,28}[a-z0-9]$");
  private static final Pattern GCP_PROJECT_NUMBER_PATTERN = Pattern.compile("^[1-9][0-9]{5,}$");
  private static final Pattern BIGLAKE_URI_CATALOG_PATTERN =
      Pattern.compile("^/[1-9][0-9]{5,}/catalogs/[^/\\s]+$");
  private static final Pattern BIGLAKE_RESOURCE_NAME_PATTERN =
      Pattern.compile("^projects/[^/\\s]+/locations/[^/\\s]+/catalogs/[^/\\s]+$");
  private static final Pattern BIGLAKE_SIMPLE_CATALOG_PATTERN =
      Pattern.compile("^[A-Za-z0-9._-]+$");
  private static final Pattern GCS_SERVICE_ACCOUNT_PATTERN =
      Pattern.compile(
          "^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\\.iam\\.gserviceaccount\\.com$");

  private static final Set<String> BLOCKED_HEADER_PROPERTIES =
      Set.of("header.authorization", "header.proxy-authorization");

  private BigLakeCatalogValidator() {}

  static void validate(RealmConfig realmConfig, Catalog catalog) {
    if (!(catalog instanceof ExternalCatalog externalCatalog)) {
      return;
    }

    if (!(externalCatalog.getConnectionConfigInfo()
        instanceof IcebergRestConnectionConfigInfo connectionConfig)) {
      return;
    }

    if (connectionConfig.getAuthenticationParameters() == null
        || connectionConfig.getAuthenticationParameters().getAuthenticationType()
            != AuthenticationParameters.AuthenticationTypeEnum.GCP) {
      return;
    }

    URI uri = parseUri(connectionConfig.getUri());
    if (!targetsBigLakeHost(uri)) {
      return;
    }

    validateBigLakeEndpoint(connectionConfig.getUri(), uri);
    validateBigLakeRemoteCatalogName(connectionConfig.getRemoteCatalogName());
    validateBigLakeHeaders(
        connectionConfig.getProperties(), externalCatalog.getProperties().toMap());
    validateBigLakeStorageConfiguration(realmConfig, externalCatalog);
  }

  private static void validateBigLakeEndpoint(String uriString, URI uri) {
    if (Strings.isNullOrEmpty(uriString)) {
      throw new IllegalArgumentException(
          "Invalid BigLake connectionConfigInfo.uri: an https:// BigLake endpoint is required.");
    }

    if (!"https".equalsIgnoreCase(uri.getScheme())) {
      throw new IllegalArgumentException(
          "Invalid BigLake connectionConfigInfo.uri '"
              + uriString
              + "': BigLake requires an https:// URI.");
    }

    if (!BIGLAKE_HOST.equalsIgnoreCase(uri.getHost())) {
      throw new IllegalArgumentException(
          "Invalid BigLake connectionConfigInfo.uri '"
              + uriString
              + "': unsupported host '"
              + uri.getHost()
              + "'. Expected '"
              + BIGLAKE_HOST
              + "'.");
    }

    String normalizedPath = normalizePath(uri.getPath());
    if (!BIGLAKE_PATH.equals(normalizedPath)) {
      throw new IllegalArgumentException(
          "Invalid BigLake connectionConfigInfo.uri '"
              + uriString
              + "': unsupported path '"
              + uri.getPath()
              + "'. Expected '"
              + BIGLAKE_PATH
              + "'.");
    }

    if (uri.getRawQuery() != null || uri.getRawFragment() != null || uri.getPort() != -1) {
      throw new IllegalArgumentException(
          "Invalid BigLake connectionConfigInfo.uri '"
              + uriString
              + "': query, fragment, and custom port components are not supported.");
    }
  }

  private static void validateBigLakeRemoteCatalogName(String remoteCatalogName) {
    if (Strings.isNullOrEmpty(remoteCatalogName) || remoteCatalogName.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid BigLake connectionConfigInfo.remoteCatalogName: a remote catalog or warehouse identifier is required.");
    }

    String trimmedRemoteCatalogName = remoteCatalogName.trim();
    if (trimmedRemoteCatalogName.startsWith("gs://")) {
      validateGsLocation("connectionConfigInfo.remoteCatalogName", trimmedRemoteCatalogName);
      return;
    }

    if (isBigLakeCatalogUri(trimmedRemoteCatalogName)
        || BIGLAKE_RESOURCE_NAME_PATTERN.matcher(trimmedRemoteCatalogName).matches()
        || BIGLAKE_SIMPLE_CATALOG_PATTERN.matcher(trimmedRemoteCatalogName).matches()) {
      return;
    }

    throw new IllegalArgumentException(
        "Invalid BigLake connectionConfigInfo.remoteCatalogName '"
            + remoteCatalogName
            + "': expected a BigLake catalog identifier or gs:// warehouse location.");
  }

  private static void validateBigLakeHeaders(
      Map<String, String> connectionProperties, Map<String, String> catalogProperties) {
    Map<String, String> headerProperties =
        connectionProperties != null ? connectionProperties : Map.of();

    for (String propertyName : headerProperties.keySet()) {
      if (propertyName == null) {
        continue;
      }

      String normalizedPropertyName = propertyName.toLowerCase(Locale.ROOT);
      if (!normalizedPropertyName.startsWith("header.")
          || QUOTA_PROJECT_HEADER.equals(normalizedPropertyName)) {
        continue;
      }

      if (BLOCKED_HEADER_PROPERTIES.contains(normalizedPropertyName)) {
        throw new IllegalArgumentException(
            "Invalid BigLake connectionConfigInfo.properties entry '"
                + propertyName
                + "': overriding security-sensitive headers is not allowed.");
      }

      throw new IllegalArgumentException(
          "Invalid BigLake connectionConfigInfo.properties entry '"
              + propertyName
              + "': only '"
              + QUOTA_PROJECT_HEADER
              + "' is supported.");
    }

    String quotaProject = headerProperties.get(QUOTA_PROJECT_HEADER);
    if (Strings.isNullOrEmpty(quotaProject) && catalogProperties != null) {
      // Preserve existing CLI-created catalogs while new CLI requests store this header on the
      // connection configuration, where it is used for outbound BigLake requests.
      quotaProject = catalogProperties.get(QUOTA_PROJECT_HEADER);
    }
    if (Strings.isNullOrEmpty(quotaProject) || quotaProject.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid BigLake connectionConfigInfo.properties entry or catalog.properties entry '"
              + QUOTA_PROJECT_HEADER
              + "': a quota project is required.");
    }

    String trimmedQuotaProject = quotaProject.trim();
    if (!GCP_PROJECT_ID_PATTERN.matcher(trimmedQuotaProject).matches()
        && !GCP_PROJECT_NUMBER_PATTERN.matcher(trimmedQuotaProject).matches()) {
      throw new IllegalArgumentException(
          "Invalid BigLake connectionConfigInfo.properties entry or catalog.properties entry '"
              + QUOTA_PROJECT_HEADER
              + "': '"
              + quotaProject
              + "' is not a valid GCP quota project.");
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

    StorageConfigInfo storageConfigInfo = externalCatalog.getStorageConfigInfo();
    if (storageConfigInfo == null) {
      if (credentialVendingEnabled) {
        throw new IllegalArgumentException(
            "Invalid BigLake storageConfigInfo: GCS storage configuration is required when credential vending is enabled.");
      }
      return;
    }

    if (storageConfigInfo.getStorageType() != StorageConfigInfo.StorageTypeEnum.GCS
        || !(storageConfigInfo instanceof GcpStorageConfigInfo gcpStorageConfigInfo)) {
      throw new IllegalArgumentException(
          "Invalid BigLake storageConfigInfo.storageType: expected GCS but found "
              + storageConfigInfo.getStorageType()
              + ".");
    }

    String defaultBaseLocation =
        externalCatalog.getProperties().toMap().get(DEFAULT_BASE_LOCATION_KEY);
    validateGsLocation("catalog.properties." + DEFAULT_BASE_LOCATION_KEY, defaultBaseLocation);

    List<String> allowedLocations = gcpStorageConfigInfo.getAllowedLocations();
    if (allowedLocations != null) {
      for (int index = 0; index < allowedLocations.size(); index++) {
        validateGsLocation(
            "storageConfigInfo.allowedLocations[" + index + "]", allowedLocations.get(index));
      }
    }

    String serviceAccount = gcpStorageConfigInfo.getGcsServiceAccount();
    if (!Strings.isNullOrEmpty(serviceAccount)
        && !GCS_SERVICE_ACCOUNT_PATTERN.matcher(serviceAccount).matches()) {
      throw new IllegalArgumentException(
          "Invalid BigLake storageConfigInfo.gcsServiceAccount '"
              + serviceAccount
              + "': expected a Google service account email.");
    }

    if (credentialVendingEnabled && Strings.isNullOrEmpty(serviceAccount)) {
      throw new IllegalArgumentException(
          "Invalid BigLake storageConfigInfo.gcsServiceAccount: a Google service account is required when credential vending is enabled.");
    }
  }

  private static void validateGsLocation(String fieldName, String location) {
    if (Strings.isNullOrEmpty(location) || location.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid BigLake " + fieldName + ": a non-empty gs:// location is required.");
    }

    StorageUri storageUri;
    try {
      storageUri = StorageUri.parse(location);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid BigLake " + fieldName + " '" + location + "': malformed gs:// location.", e);
    }

    if (!"gs".equalsIgnoreCase(storageUri.scheme())
        || Strings.isNullOrEmpty(storageUri.authority())) {
      throw new IllegalArgumentException(
          "Invalid BigLake " + fieldName + " '" + location + "': expected a gs:// location.");
    }
  }

  private static URI parseUri(String uriString) {
    if (Strings.isNullOrEmpty(uriString)) {
      return null;
    }

    try {
      return URI.create(uriString);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static boolean targetsBigLakeHost(URI uri) {
    return uri != null && BIGLAKE_HOST.equalsIgnoreCase(uri.getHost());
  }

  private static boolean isBigLakeCatalogUri(String remoteCatalogName) {
    URI uri = parseUri(remoteCatalogName);
    return uri != null
        && "bl".equalsIgnoreCase(uri.getScheme())
        && "projects".equalsIgnoreCase(uri.getHost())
        && uri.getPort() == -1
        && uri.getRawQuery() == null
        && uri.getRawFragment() == null
        && BIGLAKE_URI_CATALOG_PATTERN.matcher(normalizePath(uri.getPath())).matches();
  }

  private static String normalizePath(String path) {
    if (Strings.isNullOrEmpty(path)) {
      return "";
    }
    return path.endsWith("/") && path.length() > 1 ? path.substring(0, path.length() - 1) : path;
  }
}
