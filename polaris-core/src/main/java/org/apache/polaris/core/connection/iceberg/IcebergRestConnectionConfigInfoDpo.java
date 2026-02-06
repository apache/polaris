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
package org.apache.polaris.core.connection.iceberg;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;

/**
 * The internal persistence-object counterpart to IcebergRestConnectionConfigInfo defined in the API
 * model.
 */
public class IcebergRestConnectionConfigInfoDpo extends ConnectionConfigInfoDpo
    implements IcebergCatalogPropertiesProvider {

  public static final String GOOGLE_USER_PROJECT_HEADER_KEY = "header.x-goog-user-project";

  private static final List<String> ALLOWED_PROPERTIES = List.of(GOOGLE_USER_PROJECT_HEADER_KEY);

  private final String remoteCatalogName;

  /**
   * @param properties Properties that might be specifically needed for a particular implementation
   *     of a REST API.
   */
  public IcebergRestConnectionConfigInfoDpo(
      @JsonProperty(value = "uri", required = true) @Nonnull String uri,
      @JsonProperty(value = "authenticationParameters", required = true) @Nonnull
          AuthenticationParametersDpo authenticationParameters,
      @JsonProperty(value = "serviceIdentity", required = false) @Nullable
          ServiceIdentityInfoDpo serviceIdentityInfo,
      @JsonProperty(value = "remoteCatalogName", required = false) @Nullable
          String remoteCatalogName,
      @JsonProperty(value = "properties", required = false) @Nullable
          Map<String, String> properties) {
    super(
        ConnectionType.ICEBERG_REST.getCode(),
        uri,
        authenticationParameters,
        serviceIdentityInfo,
        properties);
    this.remoteCatalogName = remoteCatalogName;
  }

  public String getRemoteCatalogName() {
    return remoteCatalogName;
  }

  @Override
  public @Nonnull Map<String, String> asIcebergCatalogProperties(
      PolarisCredentialManager credentialManager) {
    HashMap<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, getUri());
    if (getRemoteCatalogName() != null) {
      properties.put(CatalogProperties.WAREHOUSE_LOCATION, getRemoteCatalogName());
    }
    // Add authentication-specific metadata (non-credential properties)
    properties.putAll(getAuthenticationParameters().asIcebergCatalogProperties(credentialManager));

    for (String headerKey : ALLOWED_PROPERTIES) {
      if (getProperties().containsKey(headerKey)) {
        properties.put(headerKey, getProperties().get(headerKey));
      }
    }

    // Add connection credentials from Polaris credential manager
    ConnectionCredentials connectionCredentials = credentialManager.getConnectionCredentials(this);
    properties.putAll(connectionCredentials.credentials());
    return properties;
  }

  @Override
  public ConnectionConfigInfoDpo withServiceIdentity(
      @Nonnull ServiceIdentityInfoDpo serviceIdentityInfo) {
    return new IcebergRestConnectionConfigInfoDpo(
        getUri(),
        getAuthenticationParameters(),
        serviceIdentityInfo,
        getRemoteCatalogName(),
        getProperties());
  }

  @Override
  public ConnectionConfigInfo asConnectionConfigInfoModel(
      ServiceIdentityProvider serviceIdentityProvider) {
    return IcebergRestConnectionConfigInfo.builder()
        .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
        .setUri(getUri())
        .setRemoteCatalogName(getRemoteCatalogName())
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("connectionTypeCode", getConnectionTypeCode())
        .add("uri", getUri())
        .add("remoteCatalogName", getRemoteCatalogName())
        .add("authenticationParameters", getAuthenticationParameters().toString())
        .add("serviceIdentity", getServiceIdentity())
        .add("properties", getProperties())
        .toString();
  }
}
