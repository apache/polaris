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
 * A {@link PageToken} implementation that has a page size, but no start offset. This can be used to
 * represent a `limit`. When updated, it returns {@link DonePageToken}. As such it should never be
 * user-facing and doesn't truly paginate.
 */
public class LimitPageToken extends PageToken implements HasPageSize {

  public static final String PREFIX = "limit";

  private final int pageSize;

  public LimitPageToken(int pageSize) {
    this.pageSize = pageSize;
  }

  @Override
  public int getPageSize() {
    return pageSize;
  }

  @Override
  public String toTokenString() {
    return String.format("%s/%d", PREFIX, pageSize);
  }

  @Override
  protected PageToken updated(List<?> newData) {
    return new DonePageToken();
  }
}
