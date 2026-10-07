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

import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** {@link TableMetadataCacheConfiguration} for tests, with the same defaults as the mapping. */
@PolarisImmutable
public interface TableMetadataCacheTestConfiguration extends TableMetadataCacheConfiguration {

  static TableMetadataCacheTestConfiguration disabled() {
    return withMaxBytes(0);
  }

  static TableMetadataCacheTestConfiguration withMaxBytes(long maxBytes) {
    return ImmutableTableMetadataCacheTestConfiguration.builder().maxBytes(maxBytes).build();
  }

  @Override
  @Value.Default
  default double fractionOfMaxHeapSize() {
    return 0.05;
  }

  @Override
  @Value.Default
  default long maxBytesPerEntry() {
    return 8 * 1024 * 1024;
  }
}
