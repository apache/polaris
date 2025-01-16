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
import java.util.Objects;

@jakarta.annotation.Generated(
    value = "org.openapitools.codegen.languages.JavaResteasyServerCodegen",
    date = "2025-01-15T21:05:26.610355-08:00[America/Los_Angeles]",
    comments = "Generator version: 7.10.0")
public class UnsetPolicyRequest {

  @NotNull @Valid private final EntityIdentifier entity;

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty(value = "entity", required = true)
  public EntityIdentifier getEntity() {
    return entity;
  }

  @JsonCreator
  public UnsetPolicyRequest(
      @JsonProperty(value = "entity", required = true) EntityIdentifier entity) {
    this.entity = entity;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(EntityIdentifier entity) {
    return new Builder(entity);
  }

  public static final class Builder {
    private EntityIdentifier entity;

    private Builder() {}

    private Builder(EntityIdentifier entity) {
      this.entity = entity;
    }

    public Builder setEntity(EntityIdentifier entity) {
      this.entity = entity;
      return this;
    }

    public UnsetPolicyRequest build() {
      UnsetPolicyRequest inst = new UnsetPolicyRequest(entity);
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
    UnsetPolicyRequest unsetPolicyRequest = (UnsetPolicyRequest) o;
    return Objects.equals(this.entity, unsetPolicyRequest.entity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entity);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class UnsetPolicyRequest {\n");

    sb.append("    entity: ").append(toIndentedString(entity)).append("\n");
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
