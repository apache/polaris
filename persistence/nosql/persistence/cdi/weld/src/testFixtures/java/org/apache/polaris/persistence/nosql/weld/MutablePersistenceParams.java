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
package org.apache.polaris.persistence.nosql.weld;

import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.commit.RetryConfig;

/**
 * Test-only {@link PersistenceParams} that can be swapped per test method, matching the pattern of
 * {@code MutableMaintenanceConfig}.
 */
public class MutablePersistenceParams implements PersistenceParams {

  private static PersistenceParams current =
      PersistenceParams.BuildablePersistenceParams.builder().build();

  public static void setCurrent(PersistenceParams config) {
    current = config;
  }

  public static void reset() {
    current = PersistenceParams.BuildablePersistenceParams.builder().build();
  }

  @Override
  public int referencePreviousHeadCount() {
    return current.referencePreviousHeadCount();
  }

  @Override
  public int maxIndexStripes() {
    return current.maxIndexStripes();
  }

  @Override
  public MemorySize maxEmbeddedIndexSize() {
    return current.maxEmbeddedIndexSize();
  }

  @Override
  public MemorySize maxIndexStripeSize() {
    return current.maxIndexStripeSize();
  }

  @Override
  public RetryConfig retryConfig() {
    return current.retryConfig();
  }

  @Override
  public int bucketizedBulkFetchSize() {
    return current.bucketizedBulkFetchSize();
  }

  @Override
  public MemorySize maxSerializedValueSize() {
    return current.maxSerializedValueSize();
  }
}
