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

import jakarta.validation.Valid;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonProperty;

// TODO: auto generate the class based on spec
@jakarta.annotation.Generated(
    value = "org.openapitools.codegen.languages.JavaResteasyServerCodegen",
    date = "2025-06-16T22:51:23.661280-07:00[America/Los_Angeles]",
    comments = "Generator version: 7.12.0")
public class ListGenericTablesResponse {

  private final String nextPageToken;
  @Valid private final Set<@Valid TableIdentifier> identifiers;

  /**
   * An opaque token that allows clients to make use of pagination for list APIs (e.g. ListTables).
   * Clients may initiate the first paginated request by sending an empty query parameter
   * &#x60;pageToken&#x60; to the server. Servers that support pagination should identify the
   * &#x60;pageToken&#x60; parameter and return a &#x60;next-page-token&#x60; in the response if
   * there are more results available. After the initial request, the value of
   * &#x60;next-page-token&#x60; from each response must be used as the &#x60;pageToken&#x60;
   * parameter value for the next request. The server must return &#x60;null&#x60; value for the
   * &#x60;next-page-token&#x60; in the last response. Servers that support pagination must return
   * all results in a single response with the value of &#x60;next-page-token&#x60; set to
   * &#x60;null&#x60; if the query parameter &#x60;pageToken&#x60; is not set in the request.
   * Servers that do not support pagination should ignore the &#x60;pageToken&#x60; parameter and
   * return all results in a single response. The &#x60;next-page-token&#x60; must be omitted from
   * the response. Clients must interpret either &#x60;null&#x60; or missing response value of
   * &#x60;next-page-token&#x60; as the end of the listing results.
   */
  @JsonProperty(value = "next-page-token")
  public String getNextPageToken() {
    return nextPageToken;
  }

  /** */
  @JsonProperty(value = "identifiers")
  public Set<@Valid TableIdentifier> getIdentifiers() {
    return identifiers;
  }

  @JsonCreator
  public ListGenericTablesResponse(
      @JsonProperty(value = "next-page-token") String nextPageToken,
      @JsonProperty(value = "identifiers") Set<@Valid TableIdentifier> identifiers) {
    this.nextPageToken = nextPageToken;
    this.identifiers = Objects.requireNonNullElse(identifiers, new LinkedHashSet<>());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String nextPageToken;
    private Set<@Valid TableIdentifier> identifiers;

    private Builder() {}

    public Builder setNextPageToken(String nextPageToken) {
      this.nextPageToken = nextPageToken;
      return this;
    }

    public Builder setIdentifiers(Set<@Valid TableIdentifier> identifiers) {
      this.identifiers = identifiers;
      return this;
    }

    public ListGenericTablesResponse build() {
      ListGenericTablesResponse inst = new ListGenericTablesResponse(nextPageToken, identifiers);
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
    ListGenericTablesResponse listGenericTablesResponse = (ListGenericTablesResponse) o;
    return Objects.equals(this.nextPageToken, listGenericTablesResponse.nextPageToken)
        && Objects.equals(this.identifiers, listGenericTablesResponse.identifiers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nextPageToken, identifiers);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ListGenericTablesResponse {\n");

    sb.append("    nextPageToken: ").append(toIndentedString(nextPageToken)).append("\n");
    sb.append("    identifiers: ").append(toIndentedString(identifiers)).append("\n");
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
