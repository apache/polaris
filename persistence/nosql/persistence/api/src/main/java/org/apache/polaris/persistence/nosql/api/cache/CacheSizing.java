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
package org.apache.polaris.persistence.nosql.api.cache;

import static com.google.common.base.Preconditions.checkState;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.smallrye.config.WithDefault;
import java.util.Optional;
import java.util.OptionalDouble;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.immutables.value.Value;

/**
 * Parameters to size the persistence cache. It is recommended to leave the defaults. If changes are
 * necessary, prefer the heap-size relative options over a fixed cache size, because relative sizing
 * is portable across instances with different heap sizes.
 */
@PolarisImmutable
public interface CacheSizing {

  double DEFAULT_HEAP_FRACTION = .6d;

  /**
   * Fraction of Java’s max heap size to use for cache objects, set to 0 to disable. Must not be
   * used with fixed cache sizing. If neither this value nor a fixed size is configured, a default
   * of {@code .4} (40%) is assumed, if {@code enable-soft-references} is enabled, else {@code .6}
   * (60%) is assumed.
   */
  OptionalDouble fractionOfMaxHeapSize();

  String DEFAULT_MIN_SIZE_STRING = "64M";
  MemorySize DEFAULT_MIN_SIZE = MemorySize.valueOf(DEFAULT_MIN_SIZE_STRING);

  /** When using fractional cache sizing, this amount in MB is the minimum cache size. */
  @WithDefault(DEFAULT_MIN_SIZE_STRING)
  Optional<MemorySize> fractionMinSize();

  String DEFAULT_HEAP_SIZE_KEEP_FREE_STRING = "256M";
  MemorySize DEFAULT_HEAP_SIZE_KEEP_FREE = MemorySize.valueOf(DEFAULT_HEAP_SIZE_KEEP_FREE_STRING);

  /**
   * When using fractional cache sizing, this amount in MB of the heap will always be "kept free"
   * when calculating the cache size.
   */
  @WithDefault(DEFAULT_HEAP_SIZE_KEEP_FREE_STRING)
  Optional<MemorySize> fractionAdjustment();

  /** Capacity of the persistence cache in MiB. */
  Optional<MemorySize> fixedSize();

  double DEFAULT_CACHE_CAPACITY_OVERSHOOT = 0.1d;
  String DEFAULT_CACHE_CAPACITY_OVERSHOOT_STRING = "0.1";

  /**
   * Admitted cache-capacity-overshoot fraction, defaults to {@code 0.1} (10 %).
   *
   * <p>New elements are admitted to be added to the cache, if the cache's size is less than {@code
   * cache-capacity * (1 + cache-capacity-overshoot}.
   *
   * <p>Cache eviction happens asynchronously. Situations when eviction cannot keep up with the
   * amount of data added could lead to out-of-memory situations.
   *
   * <p>The value, if present, must be greater than 0.
   */
  @WithDefault(DEFAULT_CACHE_CAPACITY_OVERSHOOT_STRING)
  OptionalDouble cacheCapacityOvershoot();

  default long calculateEffectiveSize(long maxHeapInBytes, double defaultHeapFraction) {
    if (fixedSize().isPresent()) {
      return fixedSize().get().asLong();
    }

    long fractionAsBytes =
        (long) (fractionOfMaxHeapSize().orElse(defaultHeapFraction) * maxHeapInBytes);

    long freeHeap = maxHeapInBytes - fractionAsBytes;
    long minFree = fractionAdjustment().orElse(DEFAULT_HEAP_SIZE_KEEP_FREE).asLong();

    long capacityInBytes = (minFree > freeHeap) ? maxHeapInBytes - minFree : fractionAsBytes;

    long fractionMin = fractionMinSize().orElse(DEFAULT_MIN_SIZE).asLong();
    if (capacityInBytes < fractionMin) {
      capacityInBytes = fractionMin;
    }

    return capacityInBytes;
  }

  static Builder builder() {
    return ImmutableCacheSizing.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder fixedSize(MemorySize fixedSize);

    @CanIgnoreReturnValue
    Builder fixedSize(Optional<? extends MemorySize> fixedSize);

    @CanIgnoreReturnValue
    Builder fractionOfMaxHeapSize(double fractionOfMaxHeapSize);

    @CanIgnoreReturnValue
    Builder fractionOfMaxHeapSize(OptionalDouble fractionOfMaxHeapSize);

    @CanIgnoreReturnValue
    Builder fractionMinSize(MemorySize fractionMinSize);

    @CanIgnoreReturnValue
    Builder fractionMinSize(Optional<? extends MemorySize> fractionMinSize);

    @CanIgnoreReturnValue
    Builder fractionAdjustment(MemorySize fractionAdjustment);

    @CanIgnoreReturnValue
    Builder fractionAdjustment(Optional<? extends MemorySize> fractionAdjustment);

    @CanIgnoreReturnValue
    Builder cacheCapacityOvershoot(double cacheCapacityOvershoot);

    @CanIgnoreReturnValue
    Builder cacheCapacityOvershoot(OptionalDouble cacheCapacityOvershoot);

    CacheSizing build();
  }

  @Value.Check
  default void check() {
    if (fractionOfMaxHeapSize().isPresent()) {
      checkState(
          fractionOfMaxHeapSize().getAsDouble() > 0d && fractionOfMaxHeapSize().getAsDouble() < 1d,
          "Cache sizing: fractionOfMaxHeapSize must be > 0 and < 1, but is %s",
          fractionOfMaxHeapSize());
    }
    if (fixedSize().isPresent()) {
      long fixed = fixedSize().get().asLong();
      checkState(
          fixed >= 0, "Cache sizing: sizeInBytes must be greater than 0, but is %s", fixedSize());
    }
    checkState(
        fractionAdjustment().orElse(DEFAULT_HEAP_SIZE_KEEP_FREE).asLong() > 64L * 1024L * 1024L,
        "Cache sizing: heapSizeAdjustment must be greater than 64 MB, but is %s",
        fractionAdjustment());
    checkState(
        cacheCapacityOvershoot().orElse(DEFAULT_CACHE_CAPACITY_OVERSHOOT) > 0d,
        "Cache sizing: cacheCapacityOvershoot must be greater than 0, but is %s",
        cacheCapacityOvershoot());
  }
}
