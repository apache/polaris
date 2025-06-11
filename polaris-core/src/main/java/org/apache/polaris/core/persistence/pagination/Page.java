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

import static java.util.Spliterators.iterator;

import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An immutable page of items plus their paging cursor. The {@link #encodedResponseToken()} here can
 * be used to continue the listing operation that generated the `items`.
 */
public class Page<T> {
  private final PageToken request;
  private final List<T> items;
  @Nullable private final String encodedDataReference;

  private Page(PageToken request, @Nullable String encodedDataReference, List<T> items) {
    this.request = request;
    this.encodedDataReference = encodedDataReference;
    this.items = items;
  }

  /**
   * Builds a complete response page for the full list of relevant items. No subsequence pages of
   * related data exist.
   */
  public static <T> Page<T> fromItems(List<T> items) {
    return new Page<>(PageToken.readEverything(), null, items);
  }

  /**
   * Produces a response page by consuming the number of items from the provided stream according to
   * the {@code request} parameter. Source items can be converted to a different type by providing a
   * {@code mapper} function. The page token for the response will be produced from the request data
   * combined with the pointer to the next page of data provided by the {@code dataPointer}
   * function.
   *
   * @param request defines pagination parameters that were uses to produce this page of data.
   * @param items stream of source data
   * @param mapper converter from source data types to response data types.
   * @param dataPointer determines the internal pointer to the next page of data given the last item
   *     from the previous page. The output of this function will be available from {@link
   *     PageToken#encodedDataReference()} associated with the request for the next page.
   */
  public static <R, T> Page<R> mapped(
      PageToken request, Stream<T> items, Function<T, R> mapper, Function<T, String> dataPointer) {
    List<R> data;
    if (request.paginationRequested()) {
      data = new ArrayList<>(request.pageSize());
    } else {
      data = new ArrayList<>();
    }

    T last = null;
    Iterator<T> it = iterator(items.spliterator());
    while (it.hasNext() && (!request.paginationRequested() || data.size() < request.pageSize())) {
      last = it.next();
      data.add(mapper.apply(last));
    }

    if (request.paginationRequested() && data.size() < request.pageSize()) {
      // the page was not filled, inform the client that there's no next page
      last = null;
    }

    return new Page<>(request, dataPointer.apply(last), data);
  }

  public List<T> items() {
    return items;
  }

  /**
   * Returns a page token in encoded form suitable for returning to API clients. The string returned
   * from this method is expected to be parsed by {@link PageToken#build(String, Integer)} when
   * servicing the request for the next page of related data.
   */
  public @Nullable String encodedResponseToken() {
    return PageTokenUtil.encodePageToken(request, encodedDataReference);
  }

  /**
   * Converts this page of data to objects of a different type, while maintaining the underlying
   * pointer to the next page of source data.
   */
  public <R> Page<R> map(Function<T, R> mapper) {
    return new Page<>(
        request, encodedDataReference, items.stream().map(mapper).collect(Collectors.toList()));
  }
}
