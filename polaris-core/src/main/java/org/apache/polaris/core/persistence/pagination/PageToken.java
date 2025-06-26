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

import java.util.List;
import java.util.Objects;

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

  /** Build a new PageToken that reads everything */
  public static PageToken readEverything() {
    return build(null, null);
  }

  /** Build a new PageToken from an input String, without a specified page size */
  public static PageToken fromString(String token) {
    return build(token, null);
  }

  /** Build a new PageToken from a limit */
  public static PageToken fromLimit(Integer pageSize) {
    return build(null, pageSize);
  }

  /** Build a {@link PageToken} from the input string and page size */
  public static PageToken build(String token, Integer pageSize) {
    if (token == null || token.isEmpty()) {
      if (pageSize != null) {
        return new LimitPageToken(pageSize);
      } else {
        return new ReadEverythingPageToken();
      }
    } else {
      // TODO implement, split out by the token's prefix
      throw new IllegalArgumentException("Unrecognized page token: " + token);
    }
  }

  /** Serialize a {@link PageToken} into a string */
  public abstract String toTokenString();

  /**
   * Builds a new page token to reflect new data that's been read. If the amount of data read is
   * less than the pageSize, this will return a {@link DonePageToken}
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

  @Override
  public final boolean equals(Object o) {
    if (o instanceof PageToken) {
      return Objects.equals(this.toTokenString(), ((PageToken) o).toTokenString());
    } else {
      return false;
    }
  }

  @Override
  public final int hashCode() {
    if (toTokenString() == null) {
      return 0;
    } else {
      return toTokenString().hashCode();
    }
  }
}
