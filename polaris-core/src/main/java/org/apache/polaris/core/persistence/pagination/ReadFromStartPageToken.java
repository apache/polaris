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
 * This is needed until real pagination is implemented just to capture limit / page-size without any
 * particular start position TODO: Remove this type
 */
public class ReadFromStartPageToken extends PageToken {

  private ReadFromStartPageToken(int pageSize) {
    this.pageSize = pageSize;
  }

  /** Get a new `ReadFromStartPageTokenBuilder` instance */
  public static PageTokenBuilder<ReadFromStartPageToken> builder() {
    return new ReadFromStartPageToken.ReadFromStartPageTokenBuilder();
  }

  @Override
  protected PageTokenBuilder<?> getBuilder() {
    return ReadFromStartPageToken.builder();
  }

  @Override
  protected List<String> getComponents() {
    return List.of(String.valueOf(pageSize));
  }

  @Override
  protected PageToken updated(List<?> newData) {
    return new ReadFromStartPageToken(pageSize);
  }

  @Override
  public PageToken withPageSize(Integer pageSize) {
    if (pageSize != null) {
      return new ReadFromStartPageToken(pageSize);
    } else {
      return new ReadFromStartPageToken(Integer.MAX_VALUE);
    }
  }

  /** A {@link PageTokenBuilder} implementation for {@link ReadFromStartPageToken} */
  public static class ReadFromStartPageTokenBuilder
      extends PageTokenBuilder<ReadFromStartPageToken> {

    private ReadFromStartPageTokenBuilder() {}

    @Override
    public String tokenPrefix() {
      return "polaris-read-from-start";
    }

    @Override
    public int expectedComponents() {
      return 1;
    }

    @Override
    protected ReadFromStartPageToken fromStringComponents(List<String> components) {
      return new ReadFromStartPageToken(Integer.parseInt(components.get(0)));
    }

    @Override
    protected ReadFromStartPageToken fromLimitImpl(int limit) {
      return new ReadFromStartPageToken(limit);
    }
  }
}
