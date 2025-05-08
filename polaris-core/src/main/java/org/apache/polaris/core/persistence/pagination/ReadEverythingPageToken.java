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

/**
 * A {@link PageToken} implementation for readers who want to read everything. The behavior when
 * using this token should be the same as when reading without a token.
 */
public class ReadEverythingPageToken extends PageToken {

  private ReadEverythingPageToken() {
    this.pageSize = Integer.MAX_VALUE;
    validate();
  }

  /** Get a {@link ReadEverythingPageToken} */
  public static ReadEverythingPageToken get() {
    return new ReadEverythingPageToken();
  }

  public static PageTokenBuilder<ReadEverythingPageToken> builder() {
    return new ReadEverythingPageTokenBuilder();
  }

  @Override
  protected PageTokenBuilder<?> getBuilder() {
    return builder();
  }

  /** A {@link PageTokenBuilder} implementation for {@link ReadEverythingPageToken} */
  public static class ReadEverythingPageTokenBuilder
      extends PageTokenBuilder<ReadEverythingPageToken> {

    private ReadEverythingPageTokenBuilder() {}

    @Override
    public String tokenPrefix() {
      return "polaris-read-everything";
    }

    @Override
    public int expectedComponents() {
      return 0;
    }

    @Override
    protected ReadEverythingPageToken fromStringComponents(List<String> components) {
      return ReadEverythingPageToken.get();
    }

    @Override
    protected ReadEverythingPageToken fromLimitImpl(int limit) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected List<String> getComponents() {
    return List.of();
  }

  /** Any time {@link ReadEverythingPageToken} is updated, everything has been read */
  @Override
  public PageToken updated(List<?> newData) {
    return DonePageToken.get();
  }

  /** {@link ReadEverythingPageToken} does not support page size */
  @Override
  public PageToken withPageSize(Integer pageSize) {
    if (pageSize == null || pageSize == Integer.MAX_VALUE) {
      return ReadEverythingPageToken.get();
    } else {
      throw new UnsupportedOperationException();
    }
  }
}
