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
 * A wrapper for a {@link List} of data and a {@link PageToken} that can be used to continue the
 * listing operation that generated that data.
 */
public class PolarisPage<T> {
  public final PageToken pageToken;
  public final List<T> data;

  public PolarisPage(PageToken pageToken, List<T> data) {
    this.pageToken = pageToken;
    this.data = data;
  }

  /**
   * Used to wrap a {@link List<T>} of data into a {@link PolarisPage<T>} when there is no more data
   */
  public static <T> PolarisPage<T> fromData(List<T> data) {
    return new PolarisPage<>(PageToken.DONE, data);
  }
}
