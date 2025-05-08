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

import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a page token that can be used by operations like `listTables`. Clients that specify a
 * `pageSize` (or a `pageToken`) may receive a `next-page-token` in the response, the content of
 * which is a serialized PageToken.
 *
 * <p>By providing that in the next query's `pageToken`, the client can resume listing where they
 * left off. If the client provides a `pageToken` or `pageSize` but `next-page-token` is null in the
 * response, that means there is no more data to read.
 */
public abstract class PageToken {

  public int pageSize;

  public static final PageToken END = null;
  public static final int DEFAULT_PAGE_SIZE = 1000;

  protected void validate() {
    if (pageSize <= 0) {
      throw new IllegalArgumentException("Page size must be greater than zero");
    }
  }

  /**
   * Get a new PageTokenBuilder from a PageToken. The PageTokenBuilder type should match the
   * PageToken type. Implementations may also provide a static `builder` method to obtain the same
   * PageTokenBuilder.
   */
  protected abstract PageTokenBuilder<?> getBuilder();

  /** Allows `PageToken` implementations to implement methods like `fromLimit` */
  public abstract static class PageTokenBuilder<T extends PageToken> {

    /**
     * A prefix that tokens are expected to start with, ideally unique across `PageTokenBuilder`
     * implementations.
     */
    public abstract String tokenPrefix();

    /**
     * The number of expected components in a token. This should match the number of components
     * returned by getComponents and shouldn't account for the prefix or the checksum.
     */
    public abstract int expectedComponents();

    /** Deserialize a string into a {@link PageToken} */
    public final PageToken fromString(String tokenString) {
      if (tokenString == null || tokenString.isEmpty()) {
        throw new IllegalArgumentException("Cannot build page token from empty string");
      } else {
        try {
          String decoded =
              new String(Base64.getDecoder().decode(tokenString), StandardCharsets.UTF_8);
          String[] parts = decoded.split(":");

          // +2 to account for the prefix and checksum.
          if (parts.length != expectedComponents() + 2 || !parts[0].equals(tokenPrefix())) {
            throw new IllegalArgumentException("Invalid token format in token: " + tokenString);
          }

          // Cut off prefix and checksum
          T result = fromStringComponents(Arrays.asList(parts).subList(1, parts.length - 1));
          result.validate();
          return result;
        } catch (RuntimeException e) {
          throw new IllegalArgumentException("Invalid page token: " + tokenString, e);
        }
      }
    }

    /** Construct a {@link PageToken} from a plain limit */
    public final PageToken fromLimit(Integer limit) {
      if (limit == null) {
        return ReadEverythingPageToken.get();
      } else {
        return fromLimitImpl(limit);
      }
    }

    /** Construct a {@link PageToken} from a plain limit */
    protected abstract T fromLimitImpl(int limit);

    /**
     * {@link PageTokenBuilder} implementations should implement this to build a {@link PageToken}
     * from components in a string token. These components should be the same ones returned by
     * {@link #getComponents()} and won't include the token prefix or the checksum.
     */
    protected abstract T fromStringComponents(List<String> components);
  }

  /** Convert this into components that the serialized token string will be built from. */
  protected abstract List<String> getComponents();

  /**
   * Builds a new page token to reflect new data that's been read. If the amount of data read is
   * less than the pageSize, this will return {@link PageToken#END}(null)
   */
  protected abstract PageToken updated(List<?> newData);

  /**
   * Builds a {@link Page <T>} from a {@link List<T>}. The {@link PageToken} attached to the new
   * {@link Page <T>} is the same as the result of calling {@link #updated(List)} on this {@link
   * PageToken}.
   */
  public final <T> Page<T> buildNextPage(List<T> data) {
    return new Page<T>(updated(data), data);
  }

  /**
   * Return a new {@link PageToken} with an updated page size. If the pageSize provided is null, the
   * existing page size will be preserved.
   */
  public abstract PageToken withPageSize(@Nullable Integer pageSize);

  /** Serialize a {@link PageToken} into a string */
  @Override
  public final String toString() {
    List<String> components = getComponents();
    String prefix = getBuilder().tokenPrefix();
    String componentString = String.join(":", components);
    String checksum = String.valueOf(componentString.hashCode());
    List<String> allElements =
        Stream.of(prefix, componentString, checksum)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    String rawString = String.join(":", allElements);
    return Base64.getUrlEncoder().encodeToString(rawString.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public final boolean equals(Object o) {
    if (o instanceof PageToken) {
      return this.toString().equals(o.toString());
    } else {
      return false;
    }
  }

  @Override
  public final int hashCode() {
    if (toString() == null) {
      return 0;
    } else {
      return toString().hashCode();
    }
  }
}
