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

import java.util.OptionalLong;

/** Test configuration for {@link TableMetadataCache}. */
public record TestTableMetadataCacheConfiguration(
    OptionalLong maxBytes, double fractionOfMaxHeapSize, long maxContentLength)
    implements TableMetadataCacheConfiguration {

  private static final double DEFAULT_FRACTION_OF_MAX_HEAP_SIZE = 0.05;
  private static final long DEFAULT_MAX_CONTENT_LENGTH = 8 * 1024 * 1024;

  public TestTableMetadataCacheConfiguration(long maxBytes, long maxContentLength) {
    this(OptionalLong.of(maxBytes), DEFAULT_FRACTION_OF_MAX_HEAP_SIZE, maxContentLength);
  }

  public static TestTableMetadataCacheConfiguration defaults() {
    return withFractionOfMaxHeapSize(DEFAULT_FRACTION_OF_MAX_HEAP_SIZE);
  }

  public static TestTableMetadataCacheConfiguration disabled() {
    return new TestTableMetadataCacheConfiguration(0, DEFAULT_MAX_CONTENT_LENGTH);
  }

  public static TestTableMetadataCacheConfiguration withMaxBytes(long maxBytes) {
    return new TestTableMetadataCacheConfiguration(maxBytes, DEFAULT_MAX_CONTENT_LENGTH);
  }

  public static TestTableMetadataCacheConfiguration withFractionOfMaxHeapSize(
      double fractionOfMaxHeapSize) {
    return new TestTableMetadataCacheConfiguration(
        OptionalLong.empty(), fractionOfMaxHeapSize, DEFAULT_MAX_CONTENT_LENGTH);
  }
}
