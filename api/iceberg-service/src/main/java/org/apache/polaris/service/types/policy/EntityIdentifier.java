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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModelProperty;
import jakarta.validation.constraints.*;
import java.util.Objects;

@jakarta.annotation.Generated(
    value = "org.openapitools.codegen.languages.JavaResteasyServerCodegen",
    date = "2025-01-15T21:05:26.610355-08:00[America/Los_Angeles]",
    comments = "Generator version: 7.10.0")
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type",
    visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CatalogIdentifier.class, name = "catalog"),
  @JsonSubTypes.Type(value = NamespaceIdentifier.class, name = "namespace"),
  @JsonSubTypes.Type(value = TableLikeIdentifier.class, name = "table-like"),
})
public class EntityIdentifier {

  /** Gets or Sets type */
  public enum TypeEnum {
    CATALOG("catalog"),

    NAMESPACE("namespace"),

    TABLE_LIKE("table-like");
    private String value;

    TypeEnum(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return String.valueOf(value);
    }
  }

  @NotNull private final TypeEnum type;

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty(value = "type", required = true)
  public TypeEnum getType() {
    return type;
  }

  @JsonCreator
  public EntityIdentifier(@JsonProperty(value = "type", required = true) TypeEnum type) {
    this.type = type;
  }

  public static final class Builder {
    private TypeEnum type;

    private Builder() {}

    private Builder(TypeEnum type) {
      this.type = type;
    }

    public Builder setType(TypeEnum type) {
      this.type = type;
      return this;
    }

    public EntityIdentifier build() {
      EntityIdentifier inst = new EntityIdentifier(type);
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
    EntityIdentifier entityIdentifier = (EntityIdentifier) o;
    return Objects.equals(this.type, entityIdentifier.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EntityIdentifier {\n");

    sb.append("    type: ").append(toIndentedString(type)).append("\n");
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
