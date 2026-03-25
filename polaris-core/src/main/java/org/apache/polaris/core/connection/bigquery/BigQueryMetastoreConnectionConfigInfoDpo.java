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
import java.util.List;
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

  // BigQuery Metastore catalog property keys
  private static final String GCP_BIGQUERY_PROJECT_ID = "gcp.bigquery.project-id";
  private static final String GCP_BIGQUERY_LOCATION = "gcp.bigquery.location";
  private static final String GCP_BIGQUERY_LIST_ALL_TABLES = "gcp.bigquery.list-all-tables";
  private static final String IMPERSONATE_SERVICE_ACCOUNT =
      "gcp.bigquery.impersonate.service-account";
  private static final String IMPERSONATE_LIFETIME_SECONDS =
      "gcp.bigquery.impersonate.lifetime-seconds";
  private static final String IMPERSONATE_SCOPES = "gcp.bigquery.impersonate.scopes";
  private static final String IMPERSONATE_DELEGATES = "gcp.bigquery.impersonate.delegates";

  private final String warehouse;
  private final String gcpProjectId;
  private final String gcpLocation;
  private final Boolean listAllTables;
  private final String impersonateServiceAccount;
  private final Integer impersonateLifetimeSeconds;
  private final List<String> impersonateScopes;
  private final List<String> impersonateDelegates;

  public BigQueryMetastoreConnectionConfigInfoDpo(
      @JsonProperty(value = "uri", required = true) @Nonnull String uri,
      @JsonProperty(value = "authenticationParameters", required = false) @Nullable
          AuthenticationParametersDpo authenticationParameters,
      @JsonProperty(value = "warehouse", required = true) @Nonnull String warehouse,
      @JsonProperty(value = "gcpProjectId", required = true) @Nonnull String gcpProjectId,
      @JsonProperty(value = "gcpLocation", required = false) @Nullable String gcpLocation,
      @JsonProperty(value = "listAllTables", required = false) @Nullable Boolean listAllTables,
      @JsonProperty(value = "impersonateServiceAccount", required = false) @Nullable
          String impersonateServiceAccount,
      @JsonProperty(value = "impersonateLifetimeSeconds", required = false) @Nullable
          Integer impersonateLifetimeSeconds,
      @JsonProperty(value = "impersonateScopes", required = false) @Nullable
          List<String> impersonateScopes,
      @JsonProperty(value = "impersonateDelegates", required = false) @Nullable
          List<String> impersonateDelegates,
      @JsonProperty(value = "serviceIdentity", required = false) @Nullable
          ServiceIdentityInfoDpo serviceIdentity) {
    super(ConnectionType.BIGQUERY.getCode(), uri, authenticationParameters, serviceIdentity);
    this.warehouse = warehouse;
    this.gcpProjectId = gcpProjectId;
    this.gcpLocation = gcpLocation;
    this.listAllTables = listAllTables;
    this.impersonateServiceAccount = impersonateServiceAccount;
    this.impersonateLifetimeSeconds = impersonateLifetimeSeconds;
    this.impersonateScopes = impersonateScopes;
    this.impersonateDelegates = impersonateDelegates;
  }

  public String getWarehouse() {
    return warehouse;
  }

  public String getGcpProjectId() {
    return gcpProjectId;
  }

  public String getGcpLocation() {
    return gcpLocation;
  }

  public Boolean getListAllTables() {
    return listAllTables;
  }

  public String getImpersonateServiceAccount() {
    return impersonateServiceAccount;
  }

  public Integer getImpersonateLifetimeSeconds() {
    return impersonateLifetimeSeconds;
  }

  public List<String> getImpersonateScopes() {
    return impersonateScopes;
  }

  public List<String> getImpersonateDelegates() {
    return impersonateDelegates;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("connectionTypeCode", getConnectionTypeCode())
        .add("uri", getUri())
        .add("warehouse", warehouse)
        .add("gcpProjectId", gcpProjectId)
        .add("gcpLocation", gcpLocation)
        .add("listAllTables", listAllTables)
        .add("impersonateServiceAccount", impersonateServiceAccount)
        .add("impersonateLifetimeSeconds", impersonateLifetimeSeconds)
        .add("impersonateScopes", impersonateScopes)
        .add("impersonateDelegates", impersonateDelegates)
        .add("authenticationParameters", getAuthenticationParameters().toString())
        .toString();
  }

  @Override
  public @Nonnull Map<String, String> asIcebergCatalogProperties(
      PolarisCredentialManager polarisCredentialManager) {
    HashMap<String, String> properties = new HashMap<>();

    // Required properties for BigQueryMetastoreCatalog
    properties.put(GCP_BIGQUERY_PROJECT_ID, gcpProjectId);
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

    // Optional properties
    if (gcpLocation != null) {
      properties.put(GCP_BIGQUERY_LOCATION, gcpLocation);
    }
    if (listAllTables != null) {
      properties.put(GCP_BIGQUERY_LIST_ALL_TABLES, String.valueOf(listAllTables));
    }

    // Optional impersonation properties
    if (impersonateServiceAccount != null) {
      properties.put(IMPERSONATE_SERVICE_ACCOUNT, impersonateServiceAccount);
    }
    if (impersonateLifetimeSeconds != null) {
      properties.put(IMPERSONATE_LIFETIME_SECONDS, String.valueOf(impersonateLifetimeSeconds));
    }
    if (impersonateScopes != null && !impersonateScopes.isEmpty()) {
      properties.put(IMPERSONATE_SCOPES, String.join(",", impersonateScopes));
    }
    if (impersonateDelegates != null && !impersonateDelegates.isEmpty()) {
      properties.put(IMPERSONATE_DELEGATES, String.join(",", impersonateDelegates));
    }

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
        gcpLocation,
        listAllTables,
        impersonateServiceAccount,
        impersonateLifetimeSeconds,
        impersonateScopes,
        impersonateDelegates,
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
        .setGcpLocation(gcpLocation)
        .setListAllTables(listAllTables)
        .setImpersonateServiceAccount(impersonateServiceAccount)
        .setImpersonateLifetimeSeconds(impersonateLifetimeSeconds)
        .setImpersonateScopes(impersonateScopes)
        .setImpersonateDelegates(impersonateDelegates)
        .setAuthenticationParameters(
            getAuthenticationParameters().asAuthenticationParametersModel())
        .setServiceIdentity(
            Optional.ofNullable(getServiceIdentity())
                .map(
                    serviceIdentityInfoDpo ->
                        serviceIdentityInfoDpo.asServiceIdentityInfoModel(serviceIdentityProvider))
                .orElse(null))
        .build();
  }
}
