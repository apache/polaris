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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.polaris.immutables.PolarisImmutable;

/** A wrapper for pagination information passed in as part of a request. */
@PolarisImmutable
@JsonSerialize(as = ImmutablePageToken.class)
@JsonDeserialize(as = ImmutablePageToken.class)
public interface PageToken {
  // Serialization property names are intentionally short to reduce the size of the serialized
  // paging token.

  /** The requested page size (optional). */
  @JsonProperty("p")
  OptionalInt pageSize();

  /** Convenience for {@code pageSize().isPresent()}. */
  default boolean paginationRequested() {
    return pageSize().isPresent();
  }

  /**
   * Paging token value, if present. Serialized paging tokens always have a value, but "synthetic"
   * paging tokens like {@link #readEverything()} or {@link #fromLimit(int)} do not have a token
   * value.
   */
  @JsonProperty("v")
  Optional<Token> value();

  // Note: another property can be added to contain a (cryptographic) signature, if we want to
  // ensure that a paging-token hasn't been tampered.

  /**
   * Paging token value, if it is present and an instance of the given {@code type}. This is a
   * convenience to prevent duplication of type casts.
   */
  default <T extends Token> Optional<T> valueAs(Class<T> type) {
    return value()
        .flatMap(
            t ->
                type.isAssignableFrom(t.getClass()) ? Optional.of(type.cast(t)) : Optional.empty());
  }

  /** Represents a non-paginated request. */
  static PageToken readEverything() {
    return PageTokenUtil.READ_EVERYTHING;
  }

  /** Represents a request to start paginating with a particular page size. */
  static PageToken fromLimit(int limit) {
    return PageTokenUtil.fromLimit(limit);
  }

  /**
   * Reconstructs a page token from the API-level page token string (returned to the client in the
   * response to a previous request for similar data) and an API-level new requested page size.
   *
   * @param serializedPageToken page token from the {@link Page#encodedResponseToken() previous
   *     page}
   * @param requestedPageSize optional page size for the next page. If not set, the page size of the
   *     previous page (encoded in the page token string) will be reused.
   * @see Page#encodedResponseToken()
   */
  static PageToken build(
      @Nullable String serializedPageToken, @Nullable Integer requestedPageSize) {
    return PageTokenUtil.decodePageRequest(serializedPageToken, requestedPageSize);
  }
}
