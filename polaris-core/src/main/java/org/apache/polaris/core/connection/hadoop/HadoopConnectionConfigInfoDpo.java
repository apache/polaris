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
package org.apache.polaris.core.connection.hadoop;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.HadoopConnectionConfigInfo;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.secrets.UserSecretsManager;

/**
 * The internal persistence-object counterpart to {@link
 * org.apache.polaris.core.admin.model.HadoopConnectionConfigInfo} defined in the API model.
 */
public class HadoopConnectionConfigInfoDpo extends ConnectionConfigInfoDpo {

  private final String warehouse;

  public HadoopConnectionConfigInfoDpo(
      @JsonProperty(value = "uri", required = true) @Nonnull String uri,
      @JsonProperty(value = "authenticationParameters", required = true) @Nonnull
          AuthenticationParametersDpo authenticationParameters,
      @JsonProperty(value = "warehouse", required = false) @Nullable String remoteCatalogName) {
    super(ConnectionType.HADOOP.getCode(), uri, authenticationParameters);
    this.warehouse = remoteCatalogName;
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
      UserSecretsManager secretsManager) {
    HashMap<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, getUri());
    if (getWarehouse() != null) {
      properties.put(CatalogProperties.WAREHOUSE_LOCATION, getWarehouse());
    }
    properties.putAll(getAuthenticationParameters().asIcebergCatalogProperties(secretsManager));
    return properties;
  }

  @Override
  public ConnectionConfigInfo asConnectionConfigInfoModel() {
    return HadoopConnectionConfigInfo.builder()
        .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.HADOOP)
        .setUri(getUri())
        .setWarehouse(getWarehouse())
        .setAuthenticationParameters(
            getAuthenticationParameters().asAuthenticationParametersModel())
        .build();
  }
}
