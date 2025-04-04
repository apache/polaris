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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.secrets.UserSecretReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "connectionType", visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = IcebergRestConnectionConfigInfoDpo.class, name = "ICEBERG_REST"),
})
public abstract class ConnectionConfigInfoDpo implements IcebergCatalogPropertiesProvider {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionConfigInfoDpo.class);

  // The type of the connection
  private final ConnectionType connectionType;

  // The URI of the remote catalog
  private final String uri;

  // The authentication parameters for the connection
  private final AuthenticationParametersDpo authenticationParameters;

  public ConnectionConfigInfoDpo(
      @JsonProperty(value = "connectionType", required = true) @Nonnull
          ConnectionType connectionType,
      @JsonProperty(value = "uri", required = true) @Nonnull String uri,
      @JsonProperty(value = "authenticationParameters", required = true) @Nonnull
          AuthenticationParametersDpo authenticationParameters) {
    this(connectionType, uri, authenticationParameters, true);
  }

  protected ConnectionConfigInfoDpo(
      @Nonnull ConnectionType connectionType,
      @Nonnull String uri,
      @Nonnull AuthenticationParametersDpo authenticationParameters,
      boolean validateUri) {
    this.connectionType = connectionType;
    this.uri = uri;
    this.authenticationParameters = authenticationParameters;
    if (validateUri) {
      validateUri(uri);
    }
  }

  public ConnectionType getConnectionType() {
    return connectionType;
  }

  public String getUri() {
    return uri;
  }

  public AuthenticationParametersDpo getAuthenticationParameters() {
    return authenticationParameters;
  }

  private static final ObjectMapper DEFAULT_MAPPER;

  static {
    DEFAULT_MAPPER = new ObjectMapper();
    DEFAULT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    DEFAULT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  }

  public String serialize() {
    try {
      return DEFAULT_MAPPER.writeValueAsString(this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static ConnectionConfigInfoDpo deserialize(
      @Nonnull PolarisDiagnostics diagnostics, final @Nonnull String jsonStr) {
    try {
      return DEFAULT_MAPPER.readValue(jsonStr, ConnectionConfigInfoDpo.class);
    } catch (JsonProcessingException exception) {
      diagnostics.fail(
          "fail_to_deserialize_connection_configuration", exception, "jsonStr={}", jsonStr);
    }
    return null;
  }

  /** Validates the remote URI. */
  protected void validateUri(String uri) {
    try {
      URI uriObj = URI.create(uri);
      URL url = uriObj.toURL();
    } catch (IllegalArgumentException | MalformedURLException e) {
      throw new IllegalArgumentException("Invalid remote URI: " + uri, e);
    }
  }

  public static ConnectionConfigInfoDpo fromConnectionConfigInfoModelWithSecrets(
      ConnectionConfigInfo connectionConfigurationModel,
      Map<String, UserSecretReference> secretReferences) {
    ConnectionConfigInfoDpo config = null;
    switch (connectionConfigurationModel.getConnectionType()) {
      case ICEBERG_REST:
        IcebergRestConnectionConfigInfo icebergRestConfigModel =
            (IcebergRestConnectionConfigInfo) connectionConfigurationModel;
        AuthenticationParametersDpo authenticationParameters =
            AuthenticationParametersDpo.fromAuthenticationParametersModelWithSecrets(
                icebergRestConfigModel.getAuthenticationParameters(), secretReferences);
        config =
            new IcebergRestConnectionConfigInfoDpo(
                ConnectionType.ICEBERG_REST,
                icebergRestConfigModel.getUri(),
                authenticationParameters,
                icebergRestConfigModel.getRemoteCatalogName());
        break;
      default:
        throw new IllegalStateException(
            "Unsupported connection type: " + connectionConfigurationModel.getConnectionType());
    }
    return config;
  }

  public abstract ConnectionConfigInfo asConnectionConfigInfoModel();
}
