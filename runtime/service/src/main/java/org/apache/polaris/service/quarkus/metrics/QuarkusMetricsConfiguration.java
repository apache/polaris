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
package org.apache.polaris.service.quarkus.metrics;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.validation.constraints.Min;
import java.util.Map;

@ConfigMapping(prefix = "polaris.metrics")
public interface QuarkusMetricsConfiguration {

  /** Additional tags to include in the metrics. */
  Map<String, String> tags();

  /** Configuration for the Realm ID metric tag. */
  RealmIdTag realmIdTag();

  interface RealmIdTag {

    /**
     * Whether to include the Realm ID tag in the API request metrics.
     *
     * <p>Beware that if the cardinality of this tag is too high, it can cause performance issues or
     * even crash the server.
     */
    @WithDefault("false")
    boolean enableInApiMetrics();

    /**
     * Whether to include the Realm ID tag in the HTTP server request metrics.
     *
     * <p>Beware that if the cardinality of this tag is too high, it can cause performance issues or
     * even crash the server.
     */
    @WithDefault("false")
    boolean enableInHttpMetrics();

    /**
     * The maximum number of Realm ID tag values allowed for the HTTP server request metrics.
     *
     * <p>This is used to prevent the number of tags from growing indefinitely and causing
     * performance issues or crashing the server.
     *
     * <p>If the number of tags exceeds this value, a warning will be logged and no more HTTP server
     * request metrics will be recorded.
     */
    @WithDefault("100")
    @Min(1)
    int httpMetricsMaxCardinality();
  }
}
