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
package org.apache.polaris.service.catalog;

import com.google.common.base.Strings;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;

/** Shared validation and redaction helpers for GCP-authenticated external catalogs. */
public final class GcpExternalCatalogSecurity {
  private static final String DEFAULT_BASE_LOCATION = "default-base-location";

  private static final List<String> PROHIBITED_PROPERTY_KEY_FRAGMENTS =
      List.of(
          "google.application.credentials",
          "google_application_credentials",
          "google.credentials",
          "google.credentials.file",
          "google.credentials.path",
          "google.oauth2.",
          "google.service-account",
          "google.service.account",
          "google.private-key",
          "google.private.key",
          "google.private_key",
          "google.refresh-token",
          "google.refresh.token",
          "google.access-token",
          "google.access.token",
          "google.client-secret",
          "google.client.secret",
          "google.client_secret",
          "gcp.credentials",
          "gcp.credentials.file",
          "gcp.credentials.path",
          "gcp.oauth2.",
          "gcp.service-account",
          "gcp.service.account",
          "gcp.private-key",
          "gcp.private.key",
          "gcp.private_key",
          "gcp.refresh-token",
          "gcp.refresh.token",
          "gcp.access-token",
          "gcp.access.token",
          "gcp.client-secret",
          "gcp.client.secret",
          "gcp.client_secret",
          "gcs.oauth2.token",
          "gcs.oauth2.refresh");

  private static final List<String> SENSITIVE_JSON_MARKERS =
      List.of(
          "\"type\":\"service_account\"",
          "\"type\": \"service_account\"",
          "\"private_key\"",
          "\"privateKey\"",
          "\"client_email\"",
          "\"refresh_token\"");

  private GcpExternalCatalogSecurity() {}

  public static boolean isGcpExternalCatalog(Catalog catalog) {
    if (!(catalog instanceof ExternalCatalog externalCatalog)) {
      return false;
    }

    if (!(externalCatalog.getConnectionConfigInfo() instanceof IcebergRestConnectionConfigInfo
        connectionConfigInfo)) {
      return false;
    }

    return connectionConfigInfo.getAuthenticationParameters() != null
        && connectionConfigInfo.getAuthenticationParameters().getAuthenticationType()
            == AuthenticationParameters.AuthenticationTypeEnum.GCP;
  }

  public static boolean isGcpExternalCatalog(ConnectionConfigInfoDpo connectionConfigInfoDpo) {
    return connectionConfigInfoDpo instanceof IcebergRestConnectionConfigInfoDpo
        && ConnectionType.fromCode(connectionConfigInfoDpo.getConnectionTypeCode())
            == ConnectionType.ICEBERG_REST
        && connectionConfigInfoDpo.getAuthenticationParameters() != null
        && connectionConfigInfoDpo.getAuthenticationParameters().getAuthenticationType()
            == AuthenticationType.GCP;
  }

  public static void validateNoSensitiveProperties(Catalog catalog) {
    if (!isGcpExternalCatalog(catalog)) {
      return;
    }

    ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
    validatePropertyMap("catalog.properties", catalogProperties(externalCatalog));
    validatePropertyMap(
        "connectionConfigInfo.properties", connectionProperties(externalCatalog));
  }

  public static Catalog sanitizeCatalog(Catalog catalog) {
    if (!isGcpExternalCatalog(catalog)) {
      return catalog;
    }

    ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
    IcebergRestConnectionConfigInfo connectionConfigInfo =
        (IcebergRestConnectionConfigInfo) externalCatalog.getConnectionConfigInfo();

    CatalogProperties sanitizedCatalogProperties =
        rebuildCatalogProperties(sanitizePropertyMap(catalogProperties(externalCatalog)));
    IcebergRestConnectionConfigInfo sanitizedConnectionConfigInfo =
        IcebergRestConnectionConfigInfo.builder()
            .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setUri(connectionConfigInfo.getUri())
            .setRemoteCatalogName(connectionConfigInfo.getRemoteCatalogName())
            .setAuthenticationParameters(connectionConfigInfo.getAuthenticationParameters())
            .setServiceIdentity(connectionConfigInfo.getServiceIdentity())
            .setProperties(sanitizePropertyMap(connectionProperties(externalCatalog)))
            .build();

    return ExternalCatalog.builder()
        .setType(Catalog.TypeEnum.EXTERNAL)
        .setName(externalCatalog.getName())
        .setProperties(sanitizedCatalogProperties)
        .setStorageConfigInfo(externalCatalog.getStorageConfigInfo())
        .setConnectionConfigInfo(sanitizedConnectionConfigInfo)
        .setCreateTimestamp(externalCatalog.getCreateTimestamp())
        .setLastUpdateTimestamp(externalCatalog.getLastUpdateTimestamp())
        .setEntityVersion(externalCatalog.getEntityVersion())
        .build();
  }

  public static Map<String, String> sanitizePropertyMap(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Map.of();
    }

    Map<String, String> sanitized = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (!isSensitiveGoogleProperty(entry.getKey(), entry.getValue())) {
        sanitized.put(entry.getKey(), entry.getValue());
      }
    }
    return Map.copyOf(sanitized);
  }

  private static void validatePropertyMap(String fieldPrefix, Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return;
    }

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (isSensitiveGoogleProperty(entry.getKey(), entry.getValue())) {
        throw new IllegalArgumentException(
            "Google credential material must not be stored in "
                + fieldPrefix
                + ". Configure outbound GCP authentication on the Polaris server via Application"
                + " Default Credentials, using an attached service account, workload identity, or"
                + " GOOGLE_APPLICATION_CREDENTIALS. Rejected property: "
                + propertyPath(fieldPrefix, entry.getKey()));
      }
    }
  }

  private static boolean isSensitiveGoogleProperty(String propertyName, String value) {
    String normalizedPropertyName =
        propertyName == null ? "" : propertyName.toLowerCase(Locale.ROOT).trim();

    for (String fragment : PROHIBITED_PROPERTY_KEY_FRAGMENTS) {
      if (normalizedPropertyName.contains(fragment)) {
        return true;
      }
    }

    if (Strings.isNullOrEmpty(value)) {
      return false;
    }

    String trimmedValue = value.trim();
    if (trimmedValue.startsWith("ya29.") || trimmedValue.contains("-----BEGIN PRIVATE KEY-----")) {
      return true;
    }

    String normalizedValue = trimmedValue.toLowerCase(Locale.ROOT);
    for (String marker : SENSITIVE_JSON_MARKERS) {
      if (normalizedValue.contains(marker)) {
        return true;
      }
    }

    return false;
  }

  private static CatalogProperties rebuildCatalogProperties(Map<String, String> properties) {
    String defaultBaseLocation = properties.get(DEFAULT_BASE_LOCATION);
    CatalogProperties.Builder builder =
        defaultBaseLocation == null
            ? CatalogProperties.builder()
            : CatalogProperties.builder(defaultBaseLocation);
    return builder.putAll(properties).build();
  }

  private static Map<String, String> catalogProperties(ExternalCatalog catalog) {
    return catalog.getProperties() == null ? Map.of() : catalog.getProperties().toMap();
  }

  private static Map<String, String> connectionProperties(ExternalCatalog catalog) {
    return catalog.getConnectionConfigInfo() == null || catalog.getConnectionConfigInfo().getProperties() == null
        ? Map.of()
        : catalog.getConnectionConfigInfo().getProperties();
  }

  private static String propertyPath(String fieldPrefix, String propertyName) {
    return propertyName == null ? fieldPrefix : fieldPrefix + "." + propertyName;
  }
}







