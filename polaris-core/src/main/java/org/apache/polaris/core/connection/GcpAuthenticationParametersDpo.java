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
import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.GcpAuthenticationParameters;
import org.apache.polaris.core.credentials.PolarisCredentialManager;

/**
 * See {@link org.apache.iceberg.rest.RESTUtil#configHeaders(Map)} and {@link
 * org.apache.iceberg.rest.auth.AuthManagers#loadAuthManager(String, Map)} for why we do this.
 */
public class GcpAuthenticationParametersDpo extends AuthenticationParametersDpo {
  private final String quotaProject;
  private final String remoteWarehouseName;

  public GcpAuthenticationParametersDpo(
      @JsonProperty(value = "quotaProject", required = true) @Nonnull String quotaProject,
      @JsonProperty(value = "remoteWarehouseName", required = true) @Nonnull
          String remoteWarehouseName) {
    super(AuthenticationType.GCP.getCode());
    this.quotaProject = quotaProject;
    this.remoteWarehouseName = remoteWarehouseName;
  }

  @Nonnull
  @Override
  public Map<String, String> asIcebergCatalogProperties(
      PolarisCredentialManager credentialManager) {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("header.x-goog-user-project", getQuotaProject());
    properties.put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_GOOGLE);
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, getRemoteWarehouseName());
    return properties;
  }

  @Nonnull
  @Override
  public GcpAuthenticationParameters asAuthenticationParametersModel() {
    return GcpAuthenticationParameters.builder()
        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.GCP)
        .setQuotaProject(getQuotaProject())
        .setRemoteWarehouseName(getRemoteWarehouseName())
        .build();
  }

  @Override
  public String toString() {
    return "GcpAuthenticationParametersDpo{"
        + "quotaProject='"
        + quotaProject
        + '\''
        + ", remoteWarehouseName='"
        + remoteWarehouseName
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof GcpAuthenticationParametersDpo that)) return false;
    return Objects.equals(quotaProject, that.quotaProject)
        && Objects.equals(remoteWarehouseName, that.remoteWarehouseName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(quotaProject, remoteWarehouseName);
  }

  public String getRemoteWarehouseName() {
    return remoteWarehouseName;
  }

  public String getQuotaProject() {
    return quotaProject;
  }
}
