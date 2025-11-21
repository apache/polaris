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
package org.apache.polaris.persistence.nosql.api.commit;

import io.smallrye.config.WithDefault;
import java.time.Duration;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

public interface RetryConfig {
  RetryConfig DEFAULT_RETRY_CONFIG = BuildableRetryConfig.builder().build();

  /**
   * Maximum allowed time until a retry-loop and {@linkplain Committer#commit(CommitRetryable)
   * commits} fails with a {@link RetryTimeoutException}, defaults to {@value #DEFAULT_TIMEOUT}.
   */
  @WithDefault(DEFAULT_TIMEOUT)
  Duration timeout();

  /** Maximum number of allowed retries, defaults to {@value #DEFAULT_RETRIES}. */
  @WithDefault(DEFAULT_RETRIES)
  int retries();

  /**
   * Initial lower bound for a retry-sleep duration for the retry-loop, defaults to {@link
   * #DEFAULT_RETRY_INITIAL_SLEEP_LOWER}. This value will be doubled after each retry, as long as
   * {@link #maxSleep()} is not exceeded. A concrete sleep duration will be randomly chosen between
   * the current lower and upper bounds.
   */
  @WithDefault(DEFAULT_RETRY_INITIAL_SLEEP_LOWER)
  Duration initialSleepLower();

  /**
   * Initial upper bound for a retry-sleep duration for the retry-loop, defaults to {@link
   * #DEFAULT_RETRY_INITIAL_SLEEP_UPPER}. This value will be doubled after each retry, as long as
   * {@link #maxSleep()} is not exceeded. A concrete sleep duration will be randomly chosen between
   * the current lower and upper bounds.
   */
  @WithDefault(DEFAULT_RETRY_INITIAL_SLEEP_UPPER)
  Duration initialSleepUpper();

  /** Maximum retry-sleep duration, defaults to {@link #DEFAULT_RETRY_MAX_SLEEP}. */
  @WithDefault(DEFAULT_RETRY_MAX_SLEEP)
  Duration maxSleep();

  /**
   * Without mitigation, very frequently started retry-loops running against highly contended
   * resources can result in some retry-loops invocations never making any progress and eventually
   * time out.
   *
   * <p>The default "fair retries type" helps in these scenarios with sacrificing the overall
   * throughput too much.
   */
  @WithDefault("SLEEPING")
  FairRetriesType fairRetries();

  String DEFAULT_TIMEOUT = "PT15S";
  String DEFAULT_RETRIES = "10000";
  String DEFAULT_RETRY_INITIAL_SLEEP_LOWER = "PT0.010S";
  String DEFAULT_RETRY_INITIAL_SLEEP_UPPER = "PT0.020S";
  String DEFAULT_RETRY_MAX_SLEEP = "PT0.250S";

  @PolarisImmutable
  interface BuildableRetryConfig extends RetryConfig {

    static ImmutableBuildableRetryConfig.Builder builder() {
      return ImmutableBuildableRetryConfig.builder();
    }

    @Override
    @Value.Default
    default Duration timeout() {
      return Duration.parse(DEFAULT_TIMEOUT);
    }

    @Override
    @Value.Default
    default int retries() {
      return Integer.parseInt(DEFAULT_RETRIES);
    }

    @Override
    @Value.Default
    default Duration initialSleepLower() {
      return Duration.parse(DEFAULT_RETRY_INITIAL_SLEEP_LOWER);
    }

    @Override
    @Value.Default
    default Duration initialSleepUpper() {
      return Duration.parse(DEFAULT_RETRY_INITIAL_SLEEP_UPPER);
    }

    @Override
    @Value.Default
    default Duration maxSleep() {
      return Duration.parse(DEFAULT_RETRY_MAX_SLEEP);
    }

    @Override
    @Value.Default
    default FairRetriesType fairRetries() {
      return FairRetriesType.SLEEPING;
    }
  }
}
