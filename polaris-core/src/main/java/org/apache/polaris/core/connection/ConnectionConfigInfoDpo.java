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

import com.fasterxml.jackson.annotation.JsonIgnore;
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
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.HadoopConnectionConfigInfo;
import org.apache.polaris.core.admin.model.HiveConnectionConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.connection.hadoop.HadoopConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.hive.HiveConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.iceberg.IcebergCatalogPropertiesProvider;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.registry.ServiceIdentityRegistry;
import org.apache.polaris.core.secrets.SecretReference;
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
  @JsonSubTypes.Type(value = HiveConnectionConfigInfoDpo.class, name = "3"),
})
public abstract class ConnectionConfigInfoDpo implements IcebergCatalogPropertiesProvider {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionConfigInfoDpo.class);

  // The type of the connection
  private final int connectionTypeCode;

  // The URI of the remote catalog
  private final String uri;

  // The authentication parameters for the connection
  private final AuthenticationParametersDpo authenticationParameters;

  // The Polaris service identity info of the connection
  private final ServiceIdentityInfoDpo serviceIdentity;

  public ConnectionConfigInfoDpo(
      @JsonProperty(value = "connectionTypeCode", required = true) int connectionTypeCode,
      @JsonProperty(value = "uri", required = true) @Nonnull String uri,
      @JsonProperty(value = "authenticationParameters", required = true) @Nullable
          AuthenticationParametersDpo authenticationParameters,
      @JsonProperty(value = "serviceIdentity", required = false) @Nullable
          ServiceIdentityInfoDpo serviceIdentity) {
    this(connectionTypeCode, uri, authenticationParameters, serviceIdentity, true);
  }

  protected ConnectionConfigInfoDpo(
      int connectionTypeCode,
      @Nonnull String uri,
      @Nullable AuthenticationParametersDpo authenticationParameters,
      @Nullable ServiceIdentityInfoDpo serviceIdentity,
      boolean validateUri) {
    this.connectionTypeCode = connectionTypeCode;
    this.uri = uri;
    this.authenticationParameters = authenticationParameters;
    this.serviceIdentity = serviceIdentity;
    if (validateUri) {
      validateUri(uri);
    }
  }

  public int getConnectionTypeCode() {
    return connectionTypeCode;
  }

  @JsonIgnore
  public ConnectionType getConnectionType() {
    return ConnectionType.fromCode(connectionTypeCode);
  }

  public String getUri() {
    return uri;
  }

  public AuthenticationParametersDpo getAuthenticationParameters() {
    return authenticationParameters;
  }

  public @Nullable ServiceIdentityInfoDpo getServiceIdentity() {
    return serviceIdentity;
  }

  private static final ObjectMapper DEFAULT_MAPPER;

  static {
    DEFAULT_MAPPER = new ObjectMapper();
    DEFAULT_MAPPER.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
    DEFAULT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  }

  public String serialize() {
    try {
      return DEFAULT_MAPPER.writeValueAsString(this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static ConnectionConfigInfoDpo deserialize(final @Nonnull String jsonStr) {
    try {
      return DEFAULT_MAPPER.readValue(jsonStr, ConnectionConfigInfoDpo.class);
    } catch (JsonProcessingException ex) {
      throw new RuntimeException("deserialize failed: " + ex.getMessage(), ex);
    }
  }

  /** Validates the remote URI. */
  protected void validateUri(String uri) {
    try {
      URI uriObj = URI.create(uri);
      if (connectionTypeCode == ConnectionType.HIVE.getCode()
          && uriObj.getScheme().equals("thrift")) {
        // Hive metastore runs a thrift server.
        return;
      }
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
      Map<String, SecretReference> secretReferences) {
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
                null /*Service Identity Info*/,
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
                null /*Service Identity Info*/,
                hadoopConfigModel.getWarehouse());
        break;
      case HIVE:
        HiveConnectionConfigInfo hiveConfigModel =
            (HiveConnectionConfigInfo) connectionConfigurationModel;
        authenticationParameters =
            AuthenticationParametersDpo.fromAuthenticationParametersModelWithSecrets(
                hiveConfigModel.getAuthenticationParameters(), secretReferences);
        config =
            new HiveConnectionConfigInfoDpo(
                hiveConfigModel.getUri(),
                authenticationParameters,
                hiveConfigModel.getWarehouse(),
                null /*Service Identity Info*/);
        break;
      default:
        throw new IllegalStateException(
            "Unsupported connection type: " + connectionConfigurationModel.getConnectionType());
    }
    return config;
  }

  /**
   * Creates a new copy of the ConnectionConfigInfoDpo with the given service identity info.
   *
   * @param serviceIdentityInfo The service identity info to set.
   * @return A new copy of the ConnectionConfigInfoDpo with the given service identity info.
   */
  public abstract ConnectionConfigInfoDpo withServiceIdentity(
      @Nonnull ServiceIdentityInfoDpo serviceIdentityInfo);

  /**
   * Produces the correponding API-model ConnectionConfigInfo for this persistence object; many
   * fields are one-to-one direct mappings, but some fields, such as secretReferences, might only be
   * applicable/present in the persistence object, but not the API model object.
   */
  public abstract ConnectionConfigInfo asConnectionConfigInfoModel(
      ServiceIdentityRegistry serviceIdentityRegistry);
}
