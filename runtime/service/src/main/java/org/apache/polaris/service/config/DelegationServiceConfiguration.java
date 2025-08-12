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
package org.apache.polaris.service.config;

/**
 * Configuration for the Polaris Delegation Service client.
 *
 * <p>Contains settings for connecting to and interacting with the delegation service.
 */
public class DelegationServiceConfiguration {

  private boolean enabled = false;
  private String baseUrl = "http://localhost:8282";
  private int timeoutSeconds = 30;
  private int maxRetries = 3;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public void setBaseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  public int getTimeoutSeconds() {
    return timeoutSeconds;
  }

  public void setTimeoutSeconds(int timeoutSeconds) {
    this.timeoutSeconds = timeoutSeconds;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  /**
   * Validates the configuration for external delegation services.
   *
   * @throws IllegalArgumentException if configuration is invalid
   */
  public void validate() {
    if (enabled && (baseUrl == null || baseUrl.trim().isEmpty())) {
      throw new IllegalArgumentException(
          "Delegation service baseUrl must be provided when enabled");
    }

    if (timeoutSeconds <= 0) {
      throw new IllegalArgumentException("Delegation service timeout must be positive");
    }

    if (maxRetries < 0) {
      throw new IllegalArgumentException("Delegation service maxRetries cannot be negative");
    }
  }
}
