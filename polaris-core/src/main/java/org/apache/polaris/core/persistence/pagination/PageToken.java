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
package org.apache.polaris.core.persistence.pagination;

import static com.google.common.base.Preconditions.checkState;

import jakarta.annotation.Nullable;

/** A wrapper for pagination information passed in as part of a request. */
public class PageToken {
  private final @Nullable String encodedDataReference;
  private final int pageSize;

  PageToken(@Nullable String encodedDataReference, int pageSize) {
    this.encodedDataReference = encodedDataReference;
    this.pageSize = pageSize;
  }

  /** Represents a non-paginated request. */
  public static PageToken readEverything() {
    return new PageToken(null, -1);
  }

  /** Represents a request to start paginating with a particular page size. */
  public static PageToken fromLimit(int limit) {
    return new PageToken(null, limit);
  }

  /**
   * Reconstructs a page token from the API-level page token string (returned to the client in the
   * response to a previous request for similar data) and an API-level new requested page size.
   *
   * @param encodedPageToken page token from the {@link Page#encodedResponseToken() previous page}
   * @param requestedPageSize optional page size for the next page. If not set, the page size of the
   *     previous page (encoded in the page token string) will be reused.
   * @see Page#encodedResponseToken()
   */
  public static PageToken build(
      @Nullable String encodedPageToken, @Nullable Integer requestedPageSize) {
    return PageTokenUtil.decodePageRequest(encodedPageToken, requestedPageSize);
  }

  /**
   * Returns whether requests using this page token should produce paginated responses ({@code
   * true}) or return all available data ({@code false}).
   */
  public boolean paginationRequested() {
    return pageSize > 0;
  }

  /**
   * Returns whether this token has an opaque reference that should be used to produce the next
   * response page ({@code true}), or whether the response should start from the first page of
   * available data ({@code false}).
   */
  public boolean hasDataReference() {
    return paginationRequested() && encodedDataReference != null;
  }

  /**
   * Returns the encoded form of the internal pointer to a page of data. This data should be
   * interpreted by the code that actually handles pagination for this request (usually at the
   * Persistence layer). This piece of code is normally the code that produced the {@link Page} of
   * data in response to the previous request.
   *
   * <p>If this request is not related to a previous page, {@code null} will be returned.
   *
   * @throws IllegalStateException if this method is called when {@link #paginationRequested()} is
   *     {@code false}
   */
  public @Nullable String encodedDataReference() {
    checkState(paginationRequested(), "Pagination was not requested.");
    return encodedDataReference;
  }

  /**
   * Returns the requested results page size.
   *
   * @throws IllegalStateException if this method is called when {@link #paginationRequested()} is
   *     {@code false}
   */
  public int pageSize() {
    checkState(paginationRequested(), "Pagination was not requested.");
    return pageSize;
  }
}
