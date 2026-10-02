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
package org.apache.polaris.core.connection.hive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.HiveConnectionConfigInfo;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The internal persistence-object counterpart to {@link
 * org.apache.polaris.core.admin.model.HiveConnectionConfigInfo} defined in the API model.
 */
public class HiveConnectionConfigInfoDpo extends ConnectionConfigInfoDpo {

  /**
   * Connection properties forwarded to the Iceberg {@code HiveCatalog}: catalog client settings and
   * non-secret FileIO client settings. Keys that select implementations or carry credentials are
   * deliberately not forwarded. The FileIO keys only take effect when the catalog is configured to
   * use {@code S3FileIO} via the catalog-level {@code io-impl} property.
   */
  static final List<String> ALLOWED_PROPERTIES =
      List.of(
          CatalogProperties.CLIENT_POOL_SIZE,
          "list-all-tables",
          S3FileIOProperties.ENDPOINT,
          S3FileIOProperties.PATH_STYLE_ACCESS,
          AwsClientProperties.CLIENT_REGION);

  private final String warehouse;

  public HiveConnectionConfigInfoDpo(
      @JsonProperty(value = "uri", required = true) @NonNull String uri,
      @JsonProperty(value = "authenticationParameters", required = false)
          @Nullable AuthenticationParametersDpo authenticationParameters,
      @JsonProperty(value = "warehouse", required = false) @Nullable String warehouse,
      @JsonProperty(value = "serviceIdentity", required = false)
          @Nullable ServiceIdentityInfoDpo serviceIdentity,
      @JsonProperty(value = "properties", required = false)
          @Nullable Map<String, String> properties) {
    super(
        ConnectionType.HIVE.getCode(), uri, authenticationParameters, serviceIdentity, properties);
    this.warehouse = warehouse;
  }

  public String getWarehouse() {
    return warehouse;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("connectionTypeCode", getConnectionTypeCode())
        .add("uri", getUri())
        .add("warehouse", getWarehouse())
        .add("authenticationParameters", getAuthenticationParameters().toString())
        .add("properties", getProperties())
        .toString();
  }

  @Override
  public @NonNull Map<String, String> asIcebergCatalogProperties(
      PolarisCredentialManager polarisCredentialManager) {
    HashMap<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, getUri());
    if (getWarehouse() != null) {
      properties.put(CatalogProperties.WAREHOUSE_LOCATION, getWarehouse());
    }
    copyAllowedProperties(properties, ALLOWED_PROPERTIES);
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
      @NonNull ServiceIdentityInfoDpo serviceIdentityInfo) {
    return new HiveConnectionConfigInfoDpo(
        getUri(), getAuthenticationParameters(), warehouse, serviceIdentityInfo, getProperties());
  }

  @Override
  public ConnectionConfigInfo asConnectionConfigInfoModel(
      ServiceIdentityProvider serviceIdentityProvider) {
    return HiveConnectionConfigInfo.builder()
        .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.HIVE)
        .setUri(getUri())
        .setWarehouse(getWarehouse())
        .setAuthenticationParameters(
            getAuthenticationParameters().asAuthenticationParametersModel())
        .setServiceIdentity(
            Optional.ofNullable(getServiceIdentity())
                .map(
                    serviceIdentityInfoDpo ->
                        serviceIdentityInfoDpo.asServiceIdentityInfoModel(serviceIdentityProvider))
                .orElse(null))
        .setProperties(getProperties())
        .build();
  }
}
