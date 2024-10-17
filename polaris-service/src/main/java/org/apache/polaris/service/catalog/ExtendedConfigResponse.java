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
package org.apache.polaris.service.catalog;

import com.google.common.base.MoreObjects;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.responses.ConfigResponse;

// TODO: Replace with Iceberg's ConfigResponse after integrating Iceberg 1.7.0
// Related PR: https://github.com/apache/iceberg/pull/10929#issuecomment-2418591566
public class ExtendedConfigResponse extends ConfigResponse {
  private final ConfigResponse configResponse;
  private List<String> endpoints;

  public ExtendedConfigResponse(ConfigResponse configResponse, List<String> endpoints) {
    this.configResponse = configResponse;
    this.endpoints = endpoints;
  }

  public List<String> endpoints() {
    return endpoints;
  }

  public void setEndpoints(List<String> endpoints) {
    this.endpoints = endpoints;
  }

  @Override
  public Map<String, String> defaults() {
    return configResponse.defaults();
  }

  @Override
  public Map<String, String> overrides() {
    return configResponse.overrides();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("defaults", this.defaults())
        .add("overrides", this.overrides())
        .add("endpoints", this.endpoints())
        .toString();
  }

  public static Builder extendedBuilder() {
    return new Builder();
  }

  public static class Builder {
    private final ConfigResponse.Builder configBuilder;
    private List<String> endpoints;

    public Builder() {
      this.configBuilder = ConfigResponse.builder();
    }

    public Builder withDefault(String key, String value) {
      configBuilder.withDefault(key, value);
      return this;
    }

    public Builder withDefaults(Map<String, String> defaultsToAdd) {
      configBuilder.withDefaults(defaultsToAdd);
      return this;
    }

    public Builder withOverride(String key, String value) {
      configBuilder.withOverride(key, value);
      return this;
    }

    public Builder withOverrides(Map<String, String> overridesToAdd) {
      configBuilder.withOverrides(overridesToAdd);
      return this;
    }

    public Builder withEndpoints(List<String> endpoints) {
      this.endpoints = endpoints;
      return this;
    }

    public ExtendedConfigResponse build() {
      ConfigResponse configResponse = configBuilder.build();
      return new ExtendedConfigResponse(configResponse, endpoints);
    }
  }
}
