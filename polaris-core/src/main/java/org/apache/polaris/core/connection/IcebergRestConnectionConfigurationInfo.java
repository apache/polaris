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
package org.apache.polaris.core.connection;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;

public class IcebergRestConnectionConfigurationInfo extends PolarisConnectionConfigurationInfo
    implements IcebergCatalogPropertiesProvider {

  private final String remoteCatalogName;

  private final PolarisAuthenticationParameters authenticationParameters;

  public IcebergRestConnectionConfigurationInfo(
      @JsonProperty(value = "connectionType", required = true) @Nonnull
          ConnectionType connectionType,
      @JsonProperty(value = "uri", required = true) @Nonnull String uri,
      @JsonProperty(value = "remoteCatalogName", required = false) @Nullable
          String remoteCatalogName,
      @JsonProperty(value = "authenticationParameters", required = false) @Nonnull
          PolarisAuthenticationParameters authenticationParameters) {
    super(connectionType, uri);
    this.remoteCatalogName = remoteCatalogName;
    this.authenticationParameters = authenticationParameters;
  }

  public String getRemoteCatalogName() {
    return remoteCatalogName;
  }

  public PolarisAuthenticationParameters getAuthenticationParameters() {
    return authenticationParameters;
  }

  @Override
  public @Nonnull Map<String, String> asIcebergCatalogProperties() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, getUri());
    if (getRemoteCatalogName() != null) {
      properties.put(CatalogProperties.WAREHOUSE_LOCATION, getRemoteCatalogName());
    }
    properties.putAll(authenticationParameters.asIcebergCatalogProperties());
    return properties;
  }

  @Override
  public ConnectionConfigInfo asConnectionConfigInfoModel() {
    return IcebergRestConnectionConfigInfo.builder()
        .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
        .setUri(getUri())
        .setRemoteCatalogName(getRemoteCatalogName())
        .setAuthenticationParameters(authenticationParameters.asAuthenticationParametersModel())
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("connectionType", getConnectionType())
        .add("connectionType", getConnectionType().name())
        .add("uri", getUri())
        .add("remoteCatalogName", getRemoteCatalogName())
        .add("authenticationParameters", getAuthenticationParameters().toString())
        .toString();
  }
}
