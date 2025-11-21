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
package org.apache.polaris.persistence.nosql.standalone;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.Map;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.BackendConfiguration;
import org.apache.polaris.persistence.nosql.api.backend.BackendFactory;
import org.apache.polaris.persistence.nosql.api.backend.BackendLoader;
import org.apache.polaris.persistence.nosql.nodeids.api.NodeManagementConfig;

/**
 * Leverages smallrye-config to get a {@link BackendConfiguration} instance populated with the
 * necessary settings to build a {@link Backend} instance.
 *
 * <p>{@link #defaultBackendConfigurer()} is especially useful in standalone runnable Java code like
 * JMH based benchmarks and in the manually run correctness tests.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class PersistenceConfigurer {
  private final SmallRyeConfigBuilder smallRyeConfigBuilder;
  private final String name;

  private PersistenceConfigurer(SmallRyeConfigBuilder smallRyeConfigBuilder) {
    this.smallRyeConfigBuilder = smallRyeConfigBuilder;

    SmallRyeConfig rootConfig =
        smallRyeConfigBuilder
            .withMapping(BackendConfiguration.class)
            .withMapping(NodeManagementConfig.class)
            .build();

    var backendConfiguration = rootConfig.getConfigMapping(BackendConfiguration.class);

    this.name =
        backendConfiguration
            .type()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "No backend name provided, for example via the system property 'polaris.persistence.backend.type', available backend names: "
                            + BackendLoader.availableFactories()
                                .map(BackendFactory::name)
                                .toList()));
  }

  public <RUNTIME_CONFIG, CONFIG_INTERFACE>
      BackendFactory<RUNTIME_CONFIG, CONFIG_INTERFACE> buildBackendFactory() {
    return (BackendFactory) BackendLoader.findFactoryByName(name);
  }

  public Backend buildBackendFromConfiguration(BackendFactory factory) {
    var configInterface = (Class<Object>) factory.configurationInterface();
    var backendConfigObject =
        smallRyeConfigBuilder
            .withMapping(configInterface)
            .build()
            .getConfigMapping(configInterface);

    Object backendConfig = factory.buildConfiguration(backendConfigObject);
    return factory.buildBackend(backendConfig);
  }

  /**
   * Sets up a default {@link PersistenceConfigurer} instance that uses smallrye-config default
   * sources, which include environment variables and Java system properties as config sources.
   */
  public static PersistenceConfigurer defaultBackendConfigurer() {
    return backendConfigurer(Map.of());
  }

  public static PersistenceConfigurer backendConfigurer(Map<String, String> configMap) {
    return new PersistenceConfigurer(
        new SmallRyeConfigBuilder()
            .setAddDefaultSources(true)
            .setAddDiscoveredSources(true)
            .withValidateUnknown(false)
            .withSources(new PropertiesConfigSource(configMap, "configMap")));
  }
}
