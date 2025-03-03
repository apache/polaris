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
import java.util.Map;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.polaris.core.admin.model.BearerRestAuthenticationInfo;
import org.apache.polaris.core.admin.model.RestAuthenticationInfo;
import org.jetbrains.annotations.NotNull;

public class PolarisBearerRestAuthenticationInfo extends PolarisRestAuthenticationInfo {

  @JsonProperty(value = "bearerToken")
  private final String bearerToken;

  public PolarisBearerRestAuthenticationInfo(
      @JsonProperty(value = "restAuthenticationType", required = true) @Nonnull
          RestAuthenticationType restAuthenticationType,
      @JsonProperty(value = "bearerToken", required = true) @Nonnull String bearerToken) {
    super(restAuthenticationType);
    this.bearerToken = bearerToken;
  }

  public @Nonnull String getBearerToken() {
    return bearerToken;
  }

  @Override
  public @NotNull Map<String, String> asIcebergCatalogProperties() {
    return Map.of(OAuth2Properties.TOKEN, getBearerToken());
  }

  @Override
  public RestAuthenticationInfo asRestAuthenticationInfoModel() {
    // TODO: redact secrets from the model
    return BearerRestAuthenticationInfo.builder()
        .setRestAuthenticationType(RestAuthenticationInfo.RestAuthenticationTypeEnum.BEARER)
        .setBearerToken(getBearerToken())
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("authenticationType", getRestAuthenticationType())
        .add("bearerToken", getBearerToken())
        .toString();
  }
}
