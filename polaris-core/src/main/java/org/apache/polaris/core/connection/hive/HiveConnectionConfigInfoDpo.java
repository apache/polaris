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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.HiveConnectionConfigInfo;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.registry.ServiceIdentityRegistry;
import org.apache.polaris.core.secrets.UserSecretsManager;

/**
 * The internal persistence-object counterpart to {@link
 * org.apache.polaris.core.admin.model.HiveConnectionConfigInfo} defined in the API model.
 */
public class HiveConnectionConfigInfoDpo extends ConnectionConfigInfoDpo {

  private final String warehouse;

  public HiveConnectionConfigInfoDpo(
      @JsonProperty(value = "uri", required = true) @Nonnull String uri,
      @JsonProperty(value = "authenticationParameters", required = false) @Nullable
          AuthenticationParametersDpo authenticationParameters,
      @JsonProperty(value = "warehouse", required = false) @Nullable String warehouse,
      @JsonProperty(value = "serviceIdentity", required = false) @Nullable
          ServiceIdentityInfoDpo serviceIdentity) {
    super(ConnectionType.HIVE.getCode(), uri, authenticationParameters, serviceIdentity);
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
        .toString();
  }

  @Override
  public @Nonnull Map<String, String> asIcebergCatalogProperties(
      UserSecretsManager secretsManager, PolarisCredentialManager polarisCredentialManager) {
    HashMap<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, getUri());
    if (getWarehouse() != null) {
      properties.put(CatalogProperties.WAREHOUSE_LOCATION, getWarehouse());
    }
    if (getAuthenticationParameters() != null) {
      properties.putAll(
          getAuthenticationParameters()
              .asIcebergCatalogProperties(secretsManager, polarisCredentialManager));
    }
    return properties;
  }

  @Override
  public ConnectionConfigInfoDpo withServiceIdentity(
      @Nonnull ServiceIdentityInfoDpo serviceIdentityInfo) {
    return new HiveConnectionConfigInfoDpo(
        getUri(), getAuthenticationParameters(), warehouse, serviceIdentityInfo);
  }

  @Override
  public ConnectionConfigInfo asConnectionConfigInfoModel(
      ServiceIdentityRegistry serviceIdentityRegistry) {
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
                        serviceIdentityInfoDpo.asServiceIdentityInfoModel(serviceIdentityRegistry))
                .orElse(null))
        .build();
  }
}
