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
package org.apache.polaris.spark.rest;

import jakarta.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonProperty;

// TODO: auto generate the class based on spec
public class CreateGenericTableRequest {

  @NotNull private final String name;
  @NotNull private final String format;
  private final String baseLocation;
  private final String doc;
  private final Map<String, String> properties;

  /** */
  @JsonProperty(value = "name", required = true)
  public String getName() {
    return name;
  }

  /** */
  @JsonProperty(value = "format", required = true)
  public String getFormat() {
    return format;
  }

  /** */
  @JsonProperty(value = "base-location")
  public String getBaseLocation() {
    return baseLocation;
  }

  /** */
  @JsonProperty(value = "doc")
  public String getDoc() {
    return doc;
  }

  /** */
  @JsonProperty(value = "properties")
  public Map<String, String> getProperties() {
    return properties;
  }

  @JsonCreator
  public CreateGenericTableRequest(
      @JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "format", required = true) String format,
      @JsonProperty(value = "base-location") String baseLocation,
      @JsonProperty(value = "doc") String doc,
      @JsonProperty(value = "properties") Map<String, String> properties) {
    this.name = name;
    this.format = format;
    this.baseLocation = baseLocation;
    this.doc = doc;
    this.properties = Objects.requireNonNullElse(properties, new HashMap<>());
  }

  public CreateGenericTableRequest(String name, String format) {
    this.name = name;
    this.format = format;
    this.baseLocation = null;
    this.doc = null;
    this.properties = new HashMap<>();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(String name, String format) {
    return new Builder(name, format);
  }

  public static final class Builder {
    private String name;
    private String format;
    private String baseLocation;
    private String doc;
    private Map<String, String> properties;

    private Builder() {}

    private Builder(String name, String format) {
      this.name = name;
      this.format = format;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setFormat(String format) {
      this.format = format;
      return this;
    }

    public Builder setBaseLocation(String baseLocation) {
      this.baseLocation = baseLocation;
      return this;
    }

    public Builder setDoc(String doc) {
      this.doc = doc;
      return this;
    }

    public Builder setProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    public CreateGenericTableRequest build() {
      CreateGenericTableRequest inst =
          new CreateGenericTableRequest(name, format, baseLocation, doc, properties);
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
    CreateGenericTableRequest createGenericTableRequest = (CreateGenericTableRequest) o;
    return Objects.equals(this.name, createGenericTableRequest.name)
        && Objects.equals(this.format, createGenericTableRequest.format)
        && Objects.equals(this.baseLocation, createGenericTableRequest.baseLocation)
        && Objects.equals(this.doc, createGenericTableRequest.doc)
        && Objects.equals(this.properties, createGenericTableRequest.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, format, baseLocation, doc, properties);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CreateGenericTableRequest {\n");

    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    format: ").append(toIndentedString(format)).append("\n");
    sb.append("    baseLocation: ").append(toIndentedString(baseLocation)).append("\n");
    sb.append("    doc: ").append(toIndentedString(doc)).append("\n");
    sb.append("    properties: ").append(toIndentedString(properties)).append("\n");
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
