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

import java.util.Optional;

/**
 * A wrapper for pagination information passed in as part of a request. This can potentially be
 * translated into a `PageToken`
 */
public class PageRequest {
  private final Optional<String> pageTokenStringOpt;
  private final Optional<Integer> pageSizeOpt;

  public PageRequest(String pageTokenString, Integer pageSize) {
    this.pageTokenStringOpt = Optional.ofNullable(pageTokenString);
    this.pageSizeOpt = Optional.ofNullable(pageSize);
  }

  public static PageRequest readEverything() {
    return new PageRequest(null, null);
  }

  public static PageRequest fromLimit(Integer pageSize) {
    return new PageRequest(LimitPageToken.PREFIX, pageSize);
  }

  public boolean isPaginationRequested() {
    return pageTokenStringOpt.isPresent() || pageSizeOpt.isPresent();
  }

  public Optional<String> getPageTokenString() {
    return pageTokenStringOpt;
  }

  public Optional<Integer> getPageSize() {
    return pageSizeOpt;
  }
}
