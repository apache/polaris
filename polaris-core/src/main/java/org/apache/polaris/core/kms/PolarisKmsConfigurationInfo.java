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
package org.apache.polaris.core.kms;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.polaris.core.kms.aws.AwsKmsConfigurationInfo;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Polaris KMS configuration information persisted with catalog metadata.
 *
 * <p>This configuration describes how Polaris should access a key-management service. Runtime
 * credentials produced from this configuration belong in KMS access config objects, not here.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({@JsonSubTypes.Type(value = AwsKmsConfigurationInfo.class)})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class PolarisKmsConfigurationInfo {

  private static final ObjectMapper DEFAULT_MAPPER;

  static {
    DEFAULT_MAPPER =
        JsonMapper.builder()
            .defaultPropertyInclusion(
                JsonInclude.Value.construct(
                    JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL))
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();
  }

  /** Optional logical name for resolving service-local KMS credentials. */
  @Nullable
  public abstract String getKmsName();

  public abstract KmsType getKmsType();

  public String serialize() {
    try {
      return DEFAULT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("serialize failed: " + e.getMessage(), e);
    }
  }

  /**
   * Deserialize a json string into a Polaris KMS configuration object.
   *
   * @param jsonStr a json string
   * @return the Polaris KMS configuration object
   */
  public static PolarisKmsConfigurationInfo deserialize(final @NonNull String jsonStr) {
    try {
      return DEFAULT_MAPPER.readValue(jsonStr, PolarisKmsConfigurationInfo.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("deserialize failed: " + e.getMessage(), e);
    }
  }

  public enum KmsType {
    AWS
  }
}
