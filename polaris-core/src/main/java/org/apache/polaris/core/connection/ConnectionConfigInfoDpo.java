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
import jakarta.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.HadoopConnectionConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.connection.hadoop.HadoopConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.iceberg.IcebergCatalogPropertiesProvider;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.apache.polaris.core.secrets.UserSecretReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The internal persistence-object counterpart to ConnectionConfigInfo defined in the API model.
 * Important: JsonSubTypes must be kept in sync with {@link ConnectionType}.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "connectionTypeCode")
@JsonSubTypes({
  @JsonSubTypes.Type(value = IcebergRestConnectionConfigInfoDpo.class, name = "1"),
  @JsonSubTypes.Type(value = HadoopConnectionConfigInfoDpo.class, name = "2"),
})
public abstract class ConnectionConfigInfoDpo implements IcebergCatalogPropertiesProvider {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionConfigInfoDpo.class);

  // The type of the connection
  private final int connectionTypeCode;

  // The URI of the remote catalog
  private final String uri;

  // The authentication parameters for the connection
  private final AuthenticationParametersDpo authenticationParameters;

  public ConnectionConfigInfoDpo(
      @JsonProperty(value = "connectionTypeCode", required = true) int connectionTypeCode,
      @JsonProperty(value = "uri", required = true) @Nonnull String uri,
      @JsonProperty(value = "authenticationParameters", required = true) @Nonnull
          AuthenticationParametersDpo authenticationParameters) {
    this(connectionTypeCode, uri, authenticationParameters, true);
  }

  protected ConnectionConfigInfoDpo(
      int connectionTypeCode,
      @Nonnull String uri,
      @Nonnull AuthenticationParametersDpo authenticationParameters,
      boolean validateUri) {
    this.connectionTypeCode = connectionTypeCode;
    this.uri = uri;
    this.authenticationParameters = authenticationParameters;
    if (validateUri) {
      validateUri(uri);
    }
  }

  public int getConnectionTypeCode() {
    return connectionTypeCode;
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

  public static @Nullable ConnectionConfigInfoDpo deserialize(
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

  /**
   * Converts from the API-model ConnectionConfigInfo by merging basic carryover fields with
   * expected associated secretReference(s) that have been previously scrubbed or resolved from
   * inline secret fields of the API request.
   */
  public static ConnectionConfigInfoDpo fromConnectionConfigInfoModelWithSecrets(
      ConnectionConfigInfo connectionConfigurationModel,
      Map<String, UserSecretReference> secretReferences) {
    ConnectionConfigInfoDpo config = null;
    final AuthenticationParametersDpo authenticationParameters;
    switch (connectionConfigurationModel.getConnectionType()) {
      case ICEBERG_REST:
        IcebergRestConnectionConfigInfo icebergRestConfigModel =
            (IcebergRestConnectionConfigInfo) connectionConfigurationModel;
        authenticationParameters =
            AuthenticationParametersDpo.fromAuthenticationParametersModelWithSecrets(
                icebergRestConfigModel.getAuthenticationParameters(), secretReferences);
        config =
            new IcebergRestConnectionConfigInfoDpo(
                icebergRestConfigModel.getUri(),
                authenticationParameters,
                icebergRestConfigModel.getRemoteCatalogName());
        break;
      case HADOOP:
        HadoopConnectionConfigInfo hadoopConfigModel =
            (HadoopConnectionConfigInfo) connectionConfigurationModel;
        authenticationParameters =
            AuthenticationParametersDpo.fromAuthenticationParametersModelWithSecrets(
                hadoopConfigModel.getAuthenticationParameters(), secretReferences);
        config =
            new HadoopConnectionConfigInfoDpo(
                hadoopConfigModel.getUri(),
                authenticationParameters,
                hadoopConfigModel.getWarehouse());
        break;
      default:
        throw new IllegalStateException(
            "Unsupported connection type: " + connectionConfigurationModel.getConnectionType());
    }
    return config;
  }

  /**
   * Produces the correponding API-model ConnectionConfigInfo for this persistence object; many
   * fields are one-to-one direct mappings, but some fields, such as secretReferences, might only be
   * applicable/present in the persistence object, but not the API model object.
   */
  public abstract ConnectionConfigInfo asConnectionConfigInfoModel();
}
