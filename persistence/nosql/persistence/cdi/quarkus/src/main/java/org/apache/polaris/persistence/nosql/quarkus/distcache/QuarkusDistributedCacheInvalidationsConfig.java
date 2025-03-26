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
package org.apache.polaris.persistence.nosql.quarkus.distcache;

import static org.apache.polaris.persistence.nosql.quarkus.distcache.QuarkusDistributedCacheInvalidationsConfig.CACHE_INVALIDATIONS_CONFIG_PREFIX;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;

@PolarisImmutable
@ConfigMapping(prefix = CACHE_INVALIDATIONS_CONFIG_PREFIX)
public interface QuarkusDistributedCacheInvalidationsConfig {

  String CACHE_INVALIDATIONS_CONFIG_PREFIX = "polaris.persistence.distributed-cache-invalidations";
  String CONFIG_VALID_TOKENS = "valid-tokens";
  String CONFIG_SERVICE_NAMES = "service-names";
  String CONFIG_URI = "uri";
  String CONFIG_BATCH_SIZE = "batch-size";
  String CONFIG_SERVICE_NAME_LOOKUP_INTERVAL = "service-name-lookup-interval";
  String CONFIG_REQUEST_TIMEOUT = "request-timeout";

  /**
   * Host names or IP addresses or kubernetes headless-service name of all Polaris server instances
   * accessing the same repository.
   *
   * <p>This value is automatically configured via the <a href="../../guides/kubernetes/">Polaris
   * Helm chart</a> or the Kubernetes operator (not released yet), you don't need any additional
   * configuration for distributed cache invalidations - it's setup and configured automatically. If
   * you have your own Helm chart or custom deployment, make sure to configure the IPs of all
   * Polaris instances here.
   *
   * <p>Names that start with an equal sign are not resolved but used "as is".
   */
  @WithName(CONFIG_SERVICE_NAMES)
  Optional<List<String>> cacheInvalidationServiceNames();

  /** List of cache-invalidation tokens to authenticate incoming cache-invalidation messages. */
  @WithName(CONFIG_VALID_TOKENS)
  Optional<List<String>> cacheInvalidationValidTokens();

  /**
   * URI of the cache-invalidation endpoint, only available on the Quarkus management port, defaults
   * to 9000.
   */
  @WithName(CONFIG_URI)
  @WithDefault("/polaris-management/cache-coherency")
  String cacheInvalidationUri();

  /**
   * Interval of service-name lookups to resolve the {@linkplain #cacheInvalidationServiceNames()
   * service names} into IP addresses.
   */
  @WithName(CONFIG_SERVICE_NAME_LOOKUP_INTERVAL)
  @WithDefault("PT10S")
  Duration cacheInvalidationServiceNameLookupInterval();

  @WithName(CONFIG_BATCH_SIZE)
  @WithDefault("20")
  int cacheInvalidationBatchSize();

  @WithName(CONFIG_REQUEST_TIMEOUT)
  Optional<Duration> cacheInvalidationRequestTimeout();
}
