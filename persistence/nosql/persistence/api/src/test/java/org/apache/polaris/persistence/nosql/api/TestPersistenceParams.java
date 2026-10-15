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

import static org.assertj.core.api.Assertions.assertThat;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.time.Duration;
import java.util.Map;
import org.apache.polaris.persistence.nosql.api.commit.RetryConfig.BuildableRetryConfig;
import org.junit.jupiter.api.Test;

public class TestPersistenceParams {
  @Test
  public void retryConfig() {
    var params =
        new SmallRyeConfigBuilder()
            .withMapping(PersistenceParams.class)
            .addDiscoveredConverters()
            .withSources(
                new PropertiesConfigSource(
                    Map.of("polaris.persistence.retry.timeout", "PT45S"), "configMap"))
            .build()
            .getConfigMapping(PersistenceParams.class);

    assertThat(BuildableRetryConfig.builder().from(params.retryConfig()).build())
        .isEqualTo(BuildableRetryConfig.builder().timeout(Duration.ofSeconds(45)).build());
  }
}
