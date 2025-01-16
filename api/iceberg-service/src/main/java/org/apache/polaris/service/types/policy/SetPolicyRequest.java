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
package org.apache.polaris.service.types.policy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@jakarta.annotation.Generated(
    value = "org.openapitools.codegen.languages.JavaResteasyServerCodegen",
    date = "2025-01-15T21:05:26.610355-08:00[America/Los_Angeles]",
    comments = "Generator version: 7.10.0")
public class SetPolicyRequest {

  @NotNull @Valid private final EntityIdentifier entity;
  private final Map<String, String> parameters;

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty(value = "entity", required = true)
  public EntityIdentifier getEntity() {
    return entity;
  }

  /** */
  @ApiModelProperty(value = "")
  @JsonProperty(value = "parameters")
  public Map<String, String> getParameters() {
    return parameters;
  }

  @JsonCreator
  public SetPolicyRequest(
      @JsonProperty(value = "entity", required = true) EntityIdentifier entity,
      @JsonProperty(value = "parameters") Map<String, String> parameters) {
    this.entity = entity;
    this.parameters = Objects.requireNonNullElse(parameters, new HashMap<>());
  }

  public SetPolicyRequest(EntityIdentifier entity) {
    this.entity = entity;
    this.parameters = new HashMap<>();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(EntityIdentifier entity) {
    return new Builder(entity);
  }

  public static final class Builder {
    private EntityIdentifier entity;
    private Map<String, String> parameters;

    private Builder() {}

    private Builder(EntityIdentifier entity) {
      this.entity = entity;
    }

    public Builder setEntity(EntityIdentifier entity) {
      this.entity = entity;
      return this;
    }

    public Builder setParameters(Map<String, String> parameters) {
      this.parameters = parameters;
      return this;
    }

    public SetPolicyRequest build() {
      SetPolicyRequest inst = new SetPolicyRequest(entity, parameters);
      return inst;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SetPolicyRequest setPolicyRequest = (SetPolicyRequest) o;
    return Objects.equals(this.entity, setPolicyRequest.entity)
        && Objects.equals(this.parameters, setPolicyRequest.parameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entity, parameters);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SetPolicyRequest {\n");

    sb.append("    entity: ").append(toIndentedString(entity)).append("\n");
    sb.append("    parameters: ").append(toIndentedString(parameters)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
