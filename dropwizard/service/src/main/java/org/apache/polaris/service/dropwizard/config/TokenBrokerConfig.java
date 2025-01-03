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
package org.apache.polaris.service.dropwizard.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;
import org.apache.polaris.service.auth.TokenBrokerFactoryConfig;

/**
 * This object receives the subsection of the server config YAML (`polaris-server.yml`) that is
 * related to JWT brokers.
 */
public class TokenBrokerConfig implements TokenBrokerFactoryConfig {
  private String type = "none";
  private int maxTokenGenerationInSeconds = 3600;
  private String file;
  private String secret;

  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }

  @JsonProperty("file")
  public void setFile(String file) {
    this.file = file;
  }

  @JsonProperty("secret")
  public void setSecret(String secret) {
    this.secret = secret;
  }

  @JsonProperty("maxTokenGenerationInSeconds")
  public void setMaxTokenGenerationInSeconds(int maxTokenGenerationInSeconds) {
    this.maxTokenGenerationInSeconds = maxTokenGenerationInSeconds;
  }

  public String getType() {
    return type;
  }

  @Override
  public int maxTokenGenerationInSeconds() {
    return maxTokenGenerationInSeconds;
  }

  @Nullable
  @Override
  public String file() {
    return file;
  }

  @Nullable
  @Override
  public String secret() {
    return secret;
  }
}
