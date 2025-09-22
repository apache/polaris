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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;
import java.util.function.LongSupplier;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** Persistence cache configuration. */
@ConfigMapping(prefix = "polaris.persistence.cache")
@JsonSerialize(as = ImmutableBuildableCacheConfig.class)
@JsonDeserialize(as = ImmutableBuildableCacheConfig.class)
public interface CacheConfig {

  String INVALID_REFERENCE_NEGATIVE_TTL =
      "Cache reference-negative-TTL, if present, must be positive.";
  String INVALID_REFERENCE_TTL =
      "Cache reference-TTL must be positive, 0 disables reference caching.";

  String DEFAULT_REFERENCE_TTL_STRING = "PT15M";
  Duration DEFAULT_REFERENCE_TTL = Duration.parse(DEFAULT_REFERENCE_TTL_STRING);

  boolean DEFAULT_ENABLE = true;

  /**
   * Optionally disable the cache, the default value is {@code true}, meaning that the cache is
   * <em>enabled</em> by default.
   */
  @WithDefault("" + DEFAULT_ENABLE)
  Optional<Boolean> enable();

  /** Duration to cache the state of references. */
  @WithDefault(DEFAULT_REFERENCE_TTL_STRING)
  Optional<Duration> referenceTtl();

  /** Duration to cache whether a reference does <em>not</em> exist (negative caching). */
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> referenceNegativeTtl();

  Optional<CacheSizing> sizing();

  @Value.Default
  @JsonIgnore
  default LongSupplier clockNanos() {
    return System::nanoTime;
  }

  @PolarisImmutable
  interface BuildableCacheConfig extends CacheConfig {

    static Builder builder() {
      return ImmutableBuildableCacheConfig.builder();
    }

    @Value.Check
    default void check() {
      var referenceTtl = referenceTtl().orElse(DEFAULT_REFERENCE_TTL);
      checkState(referenceTtl.compareTo(Duration.ZERO) >= 0, INVALID_REFERENCE_TTL);
      referenceNegativeTtl()
          .ifPresent(
              ttl ->
                  checkState(
                      referenceTtl.compareTo(Duration.ZERO) > 0 && ttl.compareTo(Duration.ZERO) > 0,
                      INVALID_REFERENCE_NEGATIVE_TTL));
    }

    interface Builder {
      @CanIgnoreReturnValue
      Builder referenceTtl(Duration referenceTtl);

      @CanIgnoreReturnValue
      Builder referenceNegativeTtl(Duration referenceNegativeTtl);

      @CanIgnoreReturnValue
      Builder sizing(CacheSizing sizing);

      @CanIgnoreReturnValue
      Builder clockNanos(LongSupplier clockNanos);

      CacheConfig build();
    }
  }
}
