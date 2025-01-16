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
import jakarta.validation.constraints.*;
import java.util.Objects;

@jakarta.annotation.Generated(
    value = "org.openapitools.codegen.languages.JavaResteasyServerCodegen",
    date = "2025-01-15T21:05:26.610355-08:00[America/Los_Angeles]",
    comments = "Generator version: 7.10.0")
public class Policy {

  private final String ownerId;
  @NotNull private final String policyId;
  @NotNull private final String policyType;
  @NotNull private final String name;
  private final String description;
  @NotNull private final Object content;
  @NotNull private final Integer version;
  private final Long createdAtMs;
  private final Long updatedAtMs;

  /** */
  @ApiModelProperty(value = "")
  @JsonProperty(value = "owner-id")
  public String getOwnerId() {
    return ownerId;
  }

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty(value = "policy-id", required = true)
  public String getPolicyId() {
    return policyId;
  }

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty(value = "policy-type", required = true)
  public String getPolicyType() {
    return policyType;
  }

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty(value = "name", required = true)
  public String getName() {
    return name;
  }

  /** */
  @ApiModelProperty(value = "")
  @JsonProperty(value = "description")
  public String getDescription() {
    return description;
  }

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty(value = "content", required = true)
  public Object getContent() {
    return content;
  }

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty(value = "version", required = true)
  public Integer getVersion() {
    return version;
  }

  /** */
  @ApiModelProperty(value = "")
  @JsonProperty(value = "created-at-ms")
  public Long getCreatedAtMs() {
    return createdAtMs;
  }

  /** */
  @ApiModelProperty(value = "")
  @JsonProperty(value = "updated-at-ms")
  public Long getUpdatedAtMs() {
    return updatedAtMs;
  }

  @JsonCreator
  public Policy(
      @JsonProperty(value = "owner-id") String ownerId,
      @JsonProperty(value = "policy-id", required = true) String policyId,
      @JsonProperty(value = "policy-type", required = true) String policyType,
      @JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "description") String description,
      @JsonProperty(value = "content", required = true) Object content,
      @JsonProperty(value = "version", required = true) Integer version,
      @JsonProperty(value = "created-at-ms") Long createdAtMs,
      @JsonProperty(value = "updated-at-ms") Long updatedAtMs) {
    this.ownerId = ownerId;
    this.policyId = policyId;
    this.policyType = policyType;
    this.name = name;
    this.description = description;
    this.content = Objects.requireNonNullElse(content, null);
    this.version = version;
    this.createdAtMs = createdAtMs;
    this.updatedAtMs = updatedAtMs;
  }

  public Policy(String policyId, String policyType, String name, Object content, Integer version) {
    this.ownerId = null;
    this.policyId = policyId;
    this.policyType = policyType;
    this.name = name;
    this.description = null;
    this.content = Objects.requireNonNullElse(content, null);
    this.version = version;
    this.createdAtMs = null;
    this.updatedAtMs = null;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(
      String policyId, String policyType, String name, Object content, Integer version) {
    return new Builder(policyId, policyType, name, content, version);
  }

  public static final class Builder {
    private String ownerId;
    private String policyId;
    private String policyType;
    private String name;
    private String description;
    private Object content;
    private Integer version;
    private Long createdAtMs;
    private Long updatedAtMs;

    private Builder() {}

    private Builder(
        String policyId, String policyType, String name, Object content, Integer version) {
      this.policyId = policyId;
      this.policyType = policyType;
      this.name = name;
      this.content = Objects.requireNonNullElse(content, null);
      this.version = version;
    }

    public Builder setOwnerId(String ownerId) {
      this.ownerId = ownerId;
      return this;
    }

    public Builder setPolicyId(String policyId) {
      this.policyId = policyId;
      return this;
    }

    public Builder setPolicyType(String policyType) {
      this.policyType = policyType;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder setContent(Object content) {
      this.content = content;
      return this;
    }

    public Builder setVersion(Integer version) {
      this.version = version;
      return this;
    }

    public Builder setCreatedAtMs(Long createdAtMs) {
      this.createdAtMs = createdAtMs;
      return this;
    }

    public Builder setUpdatedAtMs(Long updatedAtMs) {
      this.updatedAtMs = updatedAtMs;
      return this;
    }

    public Policy build() {
      Policy inst =
          new Policy(
              ownerId,
              policyId,
              policyType,
              name,
              description,
              content,
              version,
              createdAtMs,
              updatedAtMs);
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
    Policy policy = (Policy) o;
    return Objects.equals(this.ownerId, policy.ownerId)
        && Objects.equals(this.policyId, policy.policyId)
        && Objects.equals(this.policyType, policy.policyType)
        && Objects.equals(this.name, policy.name)
        && Objects.equals(this.description, policy.description)
        && Objects.equals(this.content, policy.content)
        && Objects.equals(this.version, policy.version)
        && Objects.equals(this.createdAtMs, policy.createdAtMs)
        && Objects.equals(this.updatedAtMs, policy.updatedAtMs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        ownerId,
        policyId,
        policyType,
        name,
        description,
        content,
        version,
        createdAtMs,
        updatedAtMs);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Policy {\n");

    sb.append("    ownerId: ").append(toIndentedString(ownerId)).append("\n");
    sb.append("    policyId: ").append(toIndentedString(policyId)).append("\n");
    sb.append("    policyType: ").append(toIndentedString(policyType)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
    sb.append("    content: ").append(toIndentedString(content)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    createdAtMs: ").append(toIndentedString(createdAtMs)).append("\n");
    sb.append("    updatedAtMs: ").append(toIndentedString(updatedAtMs)).append("\n");
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
