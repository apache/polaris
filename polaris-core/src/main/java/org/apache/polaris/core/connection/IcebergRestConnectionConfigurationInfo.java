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

public class IcebergRestConnectionConfigurationInfo extends PolarisConnectionConfigurationInfo {

  private final String remoteCatalogName;

  private final PolarisRestAuthenticationInfo restAuthentication;

  public IcebergRestConnectionConfigurationInfo(
      @JsonProperty(value = "connectionType", required = true) @Nonnull
          ConnectionType connectionType,
      @JsonProperty(value = "remoteUri", required = true) @Nonnull String remoteUri,
      @JsonProperty(value = "remoteCatalogName", required = false) @Nullable
          String remoteCatalogName,
      @JsonProperty(value = "restAuthentication", required = false) @Nonnull
          PolarisRestAuthenticationInfo restAuthentication) {
    super(connectionType, remoteUri);
    this.remoteCatalogName = remoteCatalogName;
    this.restAuthentication = restAuthentication;
  }

  public String getRemoteCatalogName() {
    return remoteCatalogName;
  }

  public PolarisRestAuthenticationInfo getRestAuthentication() {
    return restAuthentication;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("connectionType", getConnectionType())
        .add("connectionType", getConnectionType().name())
        .add("remoteUri", getRemoteUri())
        .add("remoteCatalogName", getRemoteCatalogName())
        .add("restAuthentication", getRestAuthentication().toString())
        .toString();
  }
}
