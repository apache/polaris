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
public class CreatePolicyRequest {

  @NotNull private final String name;
  @NotNull private final String type;
  private final String description;
  @NotNull private final Object content;

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty(value = "name", required = true)
  public String getName() {
    return name;
  }

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty(value = "type", required = true)
  public String getType() {
    return type;
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

  @JsonCreator
  public CreatePolicyRequest(
      @JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "type", required = true) String type,
      @JsonProperty(value = "description") String description,
      @JsonProperty(value = "content", required = true) Object content) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.content = Objects.requireNonNullElse(content, null);
  }

  public CreatePolicyRequest(String name, String type, Object content) {
    this.name = name;
    this.type = type;
    this.description = null;
    this.content = Objects.requireNonNullElse(content, null);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(String name, String type, Object content) {
    return new Builder(name, type, content);
  }

  public static final class Builder {
    private String name;
    private String type;
    private String description;
    private Object content;

    private Builder() {}

    private Builder(String name, String type, Object content) {
      this.name = name;
      this.type = type;
      this.content = Objects.requireNonNullElse(content, null);
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setType(String type) {
      this.type = type;
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

    public CreatePolicyRequest build() {
      CreatePolicyRequest inst = new CreatePolicyRequest(name, type, description, content);
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
    CreatePolicyRequest createPolicyRequest = (CreatePolicyRequest) o;
    return Objects.equals(this.name, createPolicyRequest.name)
        && Objects.equals(this.type, createPolicyRequest.type)
        && Objects.equals(this.description, createPolicyRequest.description)
        && Objects.equals(this.content, createPolicyRequest.content);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, description, content);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CreatePolicyRequest {\n");

    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
    sb.append("    content: ").append(toIndentedString(content)).append("\n");
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
