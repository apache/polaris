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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.annotation.Nonnull;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.BearerAuthenticationParameters;
import org.apache.polaris.core.admin.model.OAuthClientCredentialsParameters;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "restAuthenticationType", visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = PolarisOAuthClientCredentialsParameters.class, name = "OAUTH"),
  @JsonSubTypes.Type(value = PolarisBearerAuthenticationParameters.class, name = "BEARER"),
})
public abstract class PolarisAuthenticationParameters implements IcebergCatalogPropertiesProvider {

  @JsonProperty(value = "restAuthenticationType")
  private final RestAuthenticationType restAuthenticationType;

  public PolarisAuthenticationParameters(
      @JsonProperty(value = "restAuthenticationType", required = true) @Nonnull
          RestAuthenticationType restAuthenticationType) {
    this.restAuthenticationType = restAuthenticationType;
  }

  public @Nonnull RestAuthenticationType getRestAuthenticationType() {
    return restAuthenticationType;
  }

  public abstract AuthenticationParameters asAuthenticationParametersModel();

  public static PolarisAuthenticationParameters fromAuthenticationParametersModel(
      AuthenticationParameters restAuthenticationParameters) {
    PolarisAuthenticationParameters config = null;
    switch (restAuthenticationParameters.getRestAuthenticationType()) {
      case OAUTH:
        OAuthClientCredentialsParameters oauthRestAuthenticationModel =
            (OAuthClientCredentialsParameters) restAuthenticationParameters;
        config =
            new PolarisOAuthClientCredentialsParameters(
                RestAuthenticationType.OAUTH,
                oauthRestAuthenticationModel.getTokenUri(),
                oauthRestAuthenticationModel.getClientId(),
                oauthRestAuthenticationModel.getClientSecret(),
                oauthRestAuthenticationModel.getScopes());
        break;
      case BEARER:
        BearerAuthenticationParameters bearerRestAuthenticationModel =
            (BearerAuthenticationParameters) restAuthenticationParameters;
        config =
            new PolarisBearerAuthenticationParameters(
                RestAuthenticationType.BEARER, bearerRestAuthenticationModel.getBearerToken());
        break;
      default:
        throw new IllegalStateException(
            "Unsupported authentication type: "
                + restAuthenticationParameters.getRestAuthenticationType());
    }
    return config;
  }
}
