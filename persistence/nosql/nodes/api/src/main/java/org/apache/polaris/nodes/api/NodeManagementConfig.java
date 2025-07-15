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
package org.apache.polaris.nodes.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.IdGeneratorSpec;
import org.apache.polaris.ids.api.SnowflakeIdGenerator;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** Node management configuration. */
@ConfigMapping(prefix = "polaris.node")
@JsonSerialize(as = ImmutableBuildableNodeManagementConfig.class)
@JsonDeserialize(as = ImmutableBuildableNodeManagementConfig.class)
public interface NodeManagementConfig {
  /** Duration of a node-lease. */
  @WithDefault(DEFAULT_LEASE_DURATION)
  Duration leaseDuration();

  /** Time window before the end of a node lease when the lease will be renewed. */
  @WithDefault(DEFAULT_RENEWAL_PERIOD)
  Duration renewalPeriod();

  /**
   * Maximum number of concurrently active Polaris nodes. Do not change this value or the ID
   * generator spec, it is a rather internal property. See ID generator spec below.
   */
  @WithDefault(DEFAULT_NUM_NODES)
  int numNodes();

  /**
   * Configuration needed to build an {@linkplain IdGenerator ID generator}. This configuration
   * cannot be changed after one Polaris node has been successfully started. This specification will
   * be ignored if a persisted one exists, but a warning shall be logged if both are different.
   */
  IdGeneratorSpec idGeneratorSpec();

  String DEFAULT_LEASE_DURATION = "PT1H";
  String DEFAULT_RENEWAL_PERIOD = "PT15M";
  String DEFAULT_NUM_NODES = "" + (1 << SnowflakeIdGenerator.DEFAULT_NODE_ID_BITS);

  @PolarisImmutable
  interface BuildableNodeManagementConfig extends NodeManagementConfig {

    static ImmutableBuildableNodeManagementConfig.Builder builder() {
      return ImmutableBuildableNodeManagementConfig.builder();
    }

    @Override
    @Value.Default
    default Duration leaseDuration() {
      return Duration.parse(DEFAULT_LEASE_DURATION);
    }

    @Override
    @Value.Default
    default Duration renewalPeriod() {
      return Duration.parse(DEFAULT_RENEWAL_PERIOD);
    }

    @Override
    default int numNodes() {
      return 1 << SnowflakeIdGenerator.DEFAULT_NODE_ID_BITS;
    }
  }
}
