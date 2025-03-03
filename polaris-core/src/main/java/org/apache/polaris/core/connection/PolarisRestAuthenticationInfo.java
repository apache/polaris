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
import java.util.Map;
import org.apache.polaris.core.admin.model.BearerRestAuthenticationInfo;
import org.apache.polaris.core.admin.model.OauthRestAuthenticationInfo;
import org.apache.polaris.core.admin.model.RestAuthenticationInfo;
import org.jetbrains.annotations.NotNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "restAuthenticationType", visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = PolarisOauthRestAuthenticationInfo.class, name = "OAUTH"),
  @JsonSubTypes.Type(value = PolarisBearerRestAuthenticationInfo.class, name = "BEARER"),
})
public abstract class PolarisRestAuthenticationInfo implements IcebergCatalogPropertiesProvider {

  @JsonProperty(value = "restAuthenticationType")
  private final RestAuthenticationType restAuthenticationType;

  public PolarisRestAuthenticationInfo(
      @JsonProperty(value = "restAuthenticationType", required = true) @Nonnull
          RestAuthenticationType restAuthenticationType) {
    this.restAuthenticationType = restAuthenticationType;
  }

  public @Nonnull RestAuthenticationType getRestAuthenticationType() {
    return restAuthenticationType;
  }

  @Override
  public abstract @NotNull Map<String, String> asIcebergCatalogProperties();

  public abstract RestAuthenticationInfo asRestAuthenticationInfoModel();

  public static PolarisRestAuthenticationInfo fromRestAuthenticationInfoModel(
      RestAuthenticationInfo restAuthenticationInfo) {
    PolarisRestAuthenticationInfo config = null;
    switch (restAuthenticationInfo.getRestAuthenticationType()) {
      case OAUTH:
        OauthRestAuthenticationInfo oauthRestAuthenticationModel =
            (OauthRestAuthenticationInfo) restAuthenticationInfo;
        config =
            new PolarisOauthRestAuthenticationInfo(
                RestAuthenticationType.OAUTH,
                oauthRestAuthenticationModel.getTokenUri(),
                oauthRestAuthenticationModel.getClientId(),
                oauthRestAuthenticationModel.getClientSecret(),
                oauthRestAuthenticationModel.getScopes());
        break;
      case BEARER:
        BearerRestAuthenticationInfo bearerRestAuthenticationModel =
            (BearerRestAuthenticationInfo) restAuthenticationInfo;
        config =
            new PolarisBearerRestAuthenticationInfo(
                RestAuthenticationType.BEARER, bearerRestAuthenticationModel.getBearerToken());
        break;
      default:
        throw new IllegalStateException(
            "Unsupported authentication type: "
                + restAuthenticationInfo.getRestAuthenticationType());
    }
    return config;
  }
}
