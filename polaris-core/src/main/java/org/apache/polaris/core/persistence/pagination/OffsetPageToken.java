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
 * A simple {@link PageToken} implementation that tracks the number of records returned. Entities
 * are meant to be filtered during listing such that when a token with offset N is supplied, the
 * first N records are omitted from the results.
 */
public class OffsetPageToken extends PageToken {

  /**
   * The offset of the token. If this is `5` for example, the first 5 entities returned by a list
   * operation that uses this token will be skipped.
   */
  public final int offset;

  /** The offset to use to start with. */
  private static final int BASE_OFFSET = 0;

  private OffsetPageToken(int offset, int pageSize) {
    this.offset = offset;
    this.pageSize = pageSize;
    validate();
  }

  @Override
  protected void validate() {
    if (offset < 0) {
      throw new IllegalArgumentException("Offset must be greater than zero");
    }
    super.validate();
  }

  /** Get a new `EntityIdPageTokenBuilder` instance */
  public static PageTokenBuilder<OffsetPageToken> builder() {
    return new OffsetPageTokenBuilder();
  }

  @Override
  protected PageTokenBuilder<?> getBuilder() {
    return OffsetPageToken.builder();
  }

  @Override
  protected List<String> getComponents() {
    return List.of(String.valueOf(this.offset), String.valueOf(this.pageSize));
  }

  /** A {@link PageTokenBuilder} implementation for {@link OffsetPageToken} */
  public static class OffsetPageTokenBuilder extends PageTokenBuilder<OffsetPageToken> {

    private OffsetPageTokenBuilder() {}

    @Override
    public String tokenPrefix() {
      return "polaris-offset";
    }

    @Override
    public int expectedComponents() {
      // offset + limit
      return 2;
    }

    @Override
    protected OffsetPageToken fromStringComponents(List<String> components) {
      return new OffsetPageToken(
          Integer.parseInt(components.get(0)), Integer.parseInt(components.get(1)));
    }

    @Override
    protected OffsetPageToken fromLimitImpl(int limit) {
      return new OffsetPageToken(BASE_OFFSET, limit);
    }
  }

  @Override
  public PageToken updated(List<?> newData) {
    if (newData == null || newData.size() < this.pageSize) {
      return PageToken.DONE;
    } else {
      return new OffsetPageToken(this.offset + newData.size(), pageSize);
    }
  }

  @Override
  public OffsetPageToken withPageSize(Integer pageSize) {
    if (pageSize == null) {
      return new OffsetPageToken(BASE_OFFSET, this.pageSize);
    } else {
      return new OffsetPageToken(this.offset, pageSize);
    }
  }
}
