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

package org.apache.polaris.core.connection.bigquery;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.CatalogProperties;
import org.apache.polaris.core.admin.model.BigQueryMetastoreConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;

/**
 * The internal persistence-object counterpart to {@link
 * org.apache.polaris.core.admin.model.BigQueryMetastoreConnectionConfigInfo} defined in the API
 * model.
 */
public class BigQueryMetastoreConnectionConfigInfoDpo extends ConnectionConfigInfoDpo {

  public static final String DEFAULT_URI = "https://bigquery.googleapis.com";

  // BigQuery Metastore catalog property keys
  private static final String GCP_BIGQUERY_PROJECT_ID = "gcp.bigquery.project-id";

  private final String warehouse;
  private final String gcpProjectId;
  private final Map<String, String> properties;

  public BigQueryMetastoreConnectionConfigInfoDpo(
      @JsonProperty(value = "uri", required = true) @Nonnull String uri,
      @JsonProperty(value = "authenticationParameters", required = false) @Nullable
          AuthenticationParametersDpo authenticationParameters,
      @JsonProperty(value = "warehouse", required = true) @Nonnull String warehouse,
      @JsonProperty(value = "gcpProjectId", required = true) @Nonnull String gcpProjectId,
      @JsonProperty(value = "properties", required = false) @Nullable
          Map<String, String> properties,
      @JsonProperty(value = "serviceIdentity", required = false) @Nullable
          ServiceIdentityInfoDpo serviceIdentity) {
    super(ConnectionType.BIGQUERY.getCode(), uri, authenticationParameters, serviceIdentity);
    this.warehouse = warehouse;
    this.gcpProjectId = gcpProjectId;
    this.properties = properties != null ? properties : Map.of();
  }

  public String getWarehouse() {
    return warehouse;
  }

  public String getGcpProjectId() {
    return gcpProjectId;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("connectionTypeCode", getConnectionTypeCode())
        .add("uri", getUri())
        .add("warehouse", warehouse)
        .add("gcpProjectId", gcpProjectId)
        .add(
            "authenticationParameters",
            getAuthenticationParameters() != null
                ? getAuthenticationParameters().toString()
                : "null")
        .toString();
  }

  @Override
  public @Nonnull Map<String, String> asIcebergCatalogProperties(
      PolarisCredentialManager polarisCredentialManager) {
    HashMap<String, String> properties = new HashMap<>();

    // Required properties for BigQueryMetastoreCatalog
    properties.put(GCP_BIGQUERY_PROJECT_ID, gcpProjectId);
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

    // Add all user provided properties
    properties.putAll(this.properties);

    if (getAuthenticationParameters() != null) {
      // Add authentication-specific metadata (non-credential properties)
      properties.putAll(
          getAuthenticationParameters().asIcebergCatalogProperties(polarisCredentialManager));
      // Add connection credentials from Polaris credential manager
      ConnectionCredentials connectionCredentials =
          polarisCredentialManager.getConnectionCredentials(this);
      properties.putAll(connectionCredentials.credentials());
    }
    return properties;
  }

  @Override
  public ConnectionConfigInfoDpo withServiceIdentity(
      @Nonnull ServiceIdentityInfoDpo serviceIdentityInfo) {
    return new BigQueryMetastoreConnectionConfigInfoDpo(
        getUri(),
        getAuthenticationParameters(),
        warehouse,
        gcpProjectId,
        properties,
        serviceIdentityInfo);
  }

  @Override
  public ConnectionConfigInfo asConnectionConfigInfoModel(
      ServiceIdentityProvider serviceIdentityProvider) {
    return BigQueryMetastoreConnectionConfigInfo.builder()
        .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.BIGQUERY)
        .setUri(getUri())
        .setWarehouse(warehouse)
        .setGcpProjectId(gcpProjectId)
        .setProperties(properties)
        .setAuthenticationParameters(
            getAuthenticationParameters() != null
                ? getAuthenticationParameters().asAuthenticationParametersModel()
                : null)
        .setServiceIdentity(
            Optional.ofNullable(getServiceIdentity())
                .map(
                    serviceIdentityInfoDpo ->
                        serviceIdentityInfoDpo.asServiceIdentityInfoModel(serviceIdentityProvider))
                .orElse(null))
        .build();
  }
}
