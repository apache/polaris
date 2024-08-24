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
package io.polaris.service.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class CorsConfiguration {
  private List<String> allowedOrigins = List.of("*");
  private List<String> allowedTimingOrigins = List.of("*");
  private List<String> allowedMethods = List.of("*");
  private List<String> allowedHeaders = List.of("*");
  private List<String> exposedHeaders = List.of("*");
  private Integer preflightMaxAge = 600;
  private String allowCredentials = "true";

  public List<String> getAllowedOrigins() {
    return allowedOrigins;
  }

  @JsonProperty("allowed-origins")
  public void setAllowedOrigins(List<String> allowedOrigins) {
    this.allowedOrigins = allowedOrigins;
  }

  public void setAllowedTimingOrigins(List<String> allowedTimingOrigins) {
    this.allowedTimingOrigins = allowedTimingOrigins;
  }

  @JsonProperty("allowed-timing-origins")
  public List<String> getAllowedTimingOrigins() {
    return allowedTimingOrigins;
  }

  public List<String> getAllowedMethods() {
    return allowedMethods;
  }

  @JsonProperty("allowed-methods")
  public void setAllowedMethods(List<String> allowedMethods) {
    this.allowedMethods = allowedMethods;
  }

  public List<String> getAllowedHeaders() {
    return allowedHeaders;
  }

  @JsonProperty("allowed-headers")
  public void setAllowedHeaders(List<String> allowedHeaders) {
    this.allowedHeaders = allowedHeaders;
  }

  public List<String> getExposedHeaders() {
    return exposedHeaders;
  }

  @JsonProperty("exposed-headers")
  public void setExposedHeaders(List<String> exposedHeaders) {
    this.exposedHeaders = exposedHeaders;
  }

  public Integer getPreflightMaxAge() {
    return preflightMaxAge;
  }

  @JsonProperty("preflight-max-age")
  public void setPreflightMaxAge(Integer preflightMaxAge) {
    this.preflightMaxAge = preflightMaxAge;
  }

  public String getAllowCredentials() {
    return allowCredentials;
  }

  @JsonProperty("allowed-credentials")
  public void setAllowCredentials(String allowCredentials) {
    this.allowCredentials = allowCredentials;
  }
}
