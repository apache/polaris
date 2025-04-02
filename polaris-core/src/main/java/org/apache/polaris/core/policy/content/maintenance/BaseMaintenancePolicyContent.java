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
package org.apache.polaris.core.policy.content.maintenance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.policy.content.PolicyContent;
import org.apache.polaris.core.policy.content.StrictBooleanDeserializer;
import org.apache.polaris.core.policy.validator.InvalidPolicyException;

public abstract class BaseMaintenancePolicyContent implements PolicyContent {
  @JsonDeserialize(using = StrictBooleanDeserializer.class)
  private Boolean enable;

  private String version;
  private Map<String, String> config;

  @JsonCreator
  public BaseMaintenancePolicyContent(
      @JsonProperty(value = "enable", required = true) boolean enable) {
    this.enable = enable;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public Boolean enabled() {
    return enable;
  }

  public void setEnabled(Boolean enable) {
    this.enable = enable;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }

  static void validateVersion(
      String content,
      BaseMaintenancePolicyContent policy,
      String defaultVersion,
      Set<String> allVersions) {
    if (policy == null) {
      throw new InvalidPolicyException("Invalid policy: " + content);
    }

    if (Strings.isNullOrEmpty(policy.getVersion())) {
      policy.setVersion(defaultVersion);
    }

    if (!allVersions.contains(policy.getVersion())) {
      throw new InvalidPolicyException("Invalid policy version: " + policy.getVersion());
    }
  }
}
