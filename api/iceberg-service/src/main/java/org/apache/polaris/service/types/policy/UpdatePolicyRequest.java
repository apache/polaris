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
import java.util.Objects;

@jakarta.annotation.Generated(
    value = "org.openapitools.codegen.languages.JavaResteasyServerCodegen",
    date = "2025-01-15T21:05:26.610355-08:00[America/Los_Angeles]",
    comments = "Generator version: 7.10.0")
public class UpdatePolicyRequest {

  private final String name;
  private final String description;
  private final Object content;

  /** */
  @ApiModelProperty(value = "")
  @JsonProperty(value = "name")
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
  @ApiModelProperty(value = "")
  @JsonProperty(value = "content")
  public Object getContent() {
    return content;
  }

  @JsonCreator
  public UpdatePolicyRequest(
      @JsonProperty(value = "name") String name,
      @JsonProperty(value = "description") String description,
      @JsonProperty(value = "content") Object content) {
    this.name = name;
    this.description = description;
    this.content = Objects.requireNonNullElse(content, null);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String name;
    private String description;
    private Object content;

    private Builder() {}

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

    public UpdatePolicyRequest build() {
      UpdatePolicyRequest inst = new UpdatePolicyRequest(name, description, content);
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
    UpdatePolicyRequest updatePolicyRequest = (UpdatePolicyRequest) o;
    return Objects.equals(this.name, updatePolicyRequest.name)
        && Objects.equals(this.description, updatePolicyRequest.description)
        && Objects.equals(this.content, updatePolicyRequest.content);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, content);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class UpdatePolicyRequest {\n");

    sb.append("    name: ").append(toIndentedString(name)).append("\n");
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
