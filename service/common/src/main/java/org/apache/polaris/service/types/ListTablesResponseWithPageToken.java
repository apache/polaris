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
package org.apache.polaris.service.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;

/**
 * Used in lieu of {@link ListTablesResponse} when there may be a {@link PageToken} associated with
 * the response. Callers can use this {@link PageToken} to continue the listing operation and obtain
 * more results.
 */
public class ListTablesResponseWithPageToken extends ListTablesResponse {
  private final PageToken pageToken;

  private final List<TableIdentifier> identifiers;

  public ListTablesResponseWithPageToken(PageToken pageToken, List<TableIdentifier> identifiers) {
    this.pageToken = pageToken;
    this.identifiers = identifiers;
    Preconditions.checkArgument(this.identifiers != null, "Invalid identifier list: null");
  }

  public static ListTablesResponseWithPageToken fromPage(Page<TableIdentifier> page) {
    return new ListTablesResponseWithPageToken(page.pageToken, page.items);
  }

  @JsonProperty("next-page-token")
  public String getPageToken() {
    if (pageToken == null) {
      return null;
    } else {
      return pageToken.toString();
    }
  }

  @Override
  public List<TableIdentifier> identifiers() {
    return this.identifiers != null ? this.identifiers : List.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("identifiers", this.identifiers)
        .add("pageToken", this.pageToken)
        .toString();
  }
}
