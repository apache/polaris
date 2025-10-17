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
package org.apache.polaris.persistence.nosql.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.stream.Stream;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.commit.RetryConfig;
import org.immutables.value.Value;

@ConfigMapping(prefix = "polaris.persistence")
@JsonSerialize(as = ImmutableBuildablePersistenceParams.class)
@JsonDeserialize(as = ImmutableBuildablePersistenceParams.class)
public interface PersistenceParams {
  String DEFAULT_REFERENCE_PREVIOUS_HEAD_COUNT_STRING = "20";
  int DEFAULT_REFERENCE_PREVIOUS_HEAD_COUNT =
      Integer.parseInt(DEFAULT_REFERENCE_PREVIOUS_HEAD_COUNT_STRING);

  @WithDefault(DEFAULT_REFERENCE_PREVIOUS_HEAD_COUNT_STRING)
  int referencePreviousHeadCount();

  String DEFAULT_MAX_INDEX_STRIPES_STRING = "20";
  int DEFAULT_MAX_INDEX_STRIPES = Integer.parseInt(DEFAULT_MAX_INDEX_STRIPES_STRING);

  @WithDefault(DEFAULT_MAX_INDEX_STRIPES_STRING)
  int maxIndexStripes();

  String DEFAULT_MAX_EMBEDDED_INDEX_SIZE_STRING = "32k";
  MemorySize DEFAULT_MAX_EMBEDDED_INDEX_SIZE =
      MemorySize.valueOf(DEFAULT_MAX_EMBEDDED_INDEX_SIZE_STRING);

  @WithDefault(DEFAULT_MAX_EMBEDDED_INDEX_SIZE_STRING)
  MemorySize maxEmbeddedIndexSize();

  String DEFAULT_MAX_INDEX_STRIPE_SIZE_STRING = "128k";
  MemorySize DEFAULT_MAX_INDEX_STRIPE_SIZE =
      MemorySize.valueOf(DEFAULT_MAX_INDEX_STRIPE_SIZE_STRING);

  @WithDefault(DEFAULT_MAX_INDEX_STRIPE_SIZE_STRING)
  MemorySize maxIndexStripeSize();

  @Value.Default
  default RetryConfig retryConfig() {
    return RetryConfig.DEFAULT_RETRY_CONFIG;
  }

  String DEFAULT_BUCKETIZED_BULK_FETCH_SIZE_STRING = "16";
  int DEFAULT_BUCKETIZED_BULK_FETCH_SIZE =
      Integer.parseInt(DEFAULT_BUCKETIZED_BULK_FETCH_SIZE_STRING);

  /**
   * The number of objects to fetch at once via {@link Persistence#bucketizedBulkFetches(Stream,
   * Class)}.
   */
  @WithDefault(DEFAULT_BUCKETIZED_BULK_FETCH_SIZE_STRING)
  int bucketizedBulkFetchSize();

  String DEFAULT_MAX_SERIALIZED_VALUE_SIZE_STRING = "350k";
  MemorySize DEFAULT_MAX_SERIALIZED_VALUE_SIZE =
      MemorySize.valueOf(DEFAULT_MAX_SERIALIZED_VALUE_SIZE_STRING);

  /** The maximum size of a serialized value in a persisted database row. */
  @WithDefault(DEFAULT_MAX_SERIALIZED_VALUE_SIZE_STRING)
  MemorySize maxSerializedValueSize();

  @PolarisImmutable
  interface BuildablePersistenceParams extends PersistenceParams {
    static ImmutableBuildablePersistenceParams.Builder builder() {
      return ImmutableBuildablePersistenceParams.builder();
    }

    @Override
    @Value.Default
    default int referencePreviousHeadCount() {
      return DEFAULT_REFERENCE_PREVIOUS_HEAD_COUNT;
    }

    @Override
    @Value.Default
    default int maxIndexStripes() {
      return DEFAULT_MAX_INDEX_STRIPES;
    }

    @Override
    @Value.Default
    default MemorySize maxEmbeddedIndexSize() {
      return DEFAULT_MAX_EMBEDDED_INDEX_SIZE;
    }

    @Override
    @Value.Default
    default MemorySize maxIndexStripeSize() {
      return DEFAULT_MAX_INDEX_STRIPE_SIZE;
    }

    @Override
    @Value.Default
    default RetryConfig retryConfig() {
      return RetryConfig.BuildableRetryConfig.builder().build();
    }

    @Override
    @Value.Default
    default int bucketizedBulkFetchSize() {
      return DEFAULT_BUCKETIZED_BULK_FETCH_SIZE;
    }

    @Override
    @Value.Default
    default MemorySize maxSerializedValueSize() {
      return DEFAULT_MAX_SERIALIZED_VALUE_SIZE;
    }
  }
}
