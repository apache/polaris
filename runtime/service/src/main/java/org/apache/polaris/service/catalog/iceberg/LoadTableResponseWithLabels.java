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
package org.apache.polaris.service.catalog.iceberg;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import java.util.Map;
import org.apache.iceberg.rest.responses.LoadTableResponse;

/**
 * Wraps a standard LoadTableResponse with an optional labels field for catalog metadata
 * enrichment. Labels are ephemeral API-level annotations — not part of table state.
 *
 * <p>This is a proof-of-concept for the IRC Labels proposal. Labels are derived from Polaris
 * entity internal properties (prefix "label."), which are catalog-scoped and separate from Iceberg
 * table metadata.
 */
public class LoadTableResponseWithLabels {

  @JsonUnwrapped private final LoadTableResponse delegate;
  private final Labels labels;

  public LoadTableResponseWithLabels(LoadTableResponse delegate, Labels labels) {
    this.delegate = delegate;
    this.labels = labels;
  }

  public LoadTableResponse getDelegate() {
    return delegate;
  }

  public Labels getLabels() {
    return labels;
  }

  public static class Labels {
    private final Map<String, String> table;

    public Labels(Map<String, String> table) {
      this.table = table;
    }

    public Map<String, String> getTable() {
      return table;
    }
  }
}
