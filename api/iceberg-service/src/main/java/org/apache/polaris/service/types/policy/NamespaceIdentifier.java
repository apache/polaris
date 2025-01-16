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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@jakarta.annotation.Generated(
    value = "org.openapitools.codegen.languages.JavaResteasyServerCodegen",
    date = "2025-01-15T21:05:26.610355-08:00[America/Los_Angeles]",
    comments = "Generator version: 7.10.0")
public class NamespaceIdentifier extends EntityIdentifier {

  @NotNull private final String catalog;
  @NotNull private final List<String> namespace;

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty(value = "catalog", required = true)
  public String getCatalog() {
    return catalog;
  }

  /** Reference to one or more levels of a namespace */
  @ApiModelProperty(
      example = "[\"accounting\",\"tax\"]",
      required = true,
      value = "Reference to one or more levels of a namespace")
  @JsonProperty(value = "namespace", required = true)
  public List<String> getNamespace() {
    return namespace;
  }

  @JsonCreator
  public NamespaceIdentifier(
      @JsonProperty(value = "catalog", required = true) String catalog,
      @JsonProperty(value = "namespace", required = true) List<String> namespace,
      @JsonProperty(value = "type", required = true) TypeEnum type) {
    super(type);
    this.catalog = catalog;
    this.namespace = Objects.requireNonNullElse(namespace, new ArrayList<>());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(String catalog, List<String> namespace, TypeEnum type) {
    return new Builder(catalog, namespace, type);
  }

  public static final class Builder {
    private String catalog;
    private List<String> namespace;
    private TypeEnum type;

    private Builder() {}

    private Builder(String catalog, List<String> namespace, TypeEnum type) {
      this.catalog = catalog;
      this.namespace = Objects.requireNonNullElse(namespace, new ArrayList<>());
      this.type = type;
    }

    public Builder setCatalog(String catalog) {
      this.catalog = catalog;
      return this;
    }

    public Builder setNamespace(List<String> namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder setType(TypeEnum type) {
      this.type = type;
      return this;
    }

    public NamespaceIdentifier build() {
      NamespaceIdentifier inst = new NamespaceIdentifier(catalog, namespace, type);
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
    NamespaceIdentifier namespaceIdentifier = (NamespaceIdentifier) o;
    return super.equals(o)
        && Objects.equals(this.catalog, namespaceIdentifier.catalog)
        && Objects.equals(this.namespace, namespaceIdentifier.namespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), catalog, super.hashCode(), namespace);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class NamespaceIdentifier {\n");
    sb.append("    ").append(toIndentedString(super.toString())).append("\n");
    sb.append("    catalog: ").append(toIndentedString(catalog)).append("\n");
    sb.append("    namespace: ").append(toIndentedString(namespace)).append("\n");
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
