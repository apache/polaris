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
package org.apache.polaris.persistence.weld;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import java.util.stream.Collectors;
import org.apache.polaris.persistence.api.backend.Backend;
import org.apache.polaris.persistence.api.backend.BackendConfiguration;
import org.apache.polaris.persistence.api.backend.BackendFactory;
import org.apache.polaris.persistence.api.backend.BackendLoader;
import org.apache.polaris.persistence.cdi.backend.NotObserved;

@ApplicationScoped
class BackendProvider {
  @Produces
  @ApplicationScoped
  @NotObserved
  Backend backend(
      BackendConfiguration backendConfiguration, Instance<Object> backendSpecificConfigs) {

    var factory =
        backendConfiguration
            .name()
            .map(BackendLoader::findFactoryByName)
            .map(
                f -> {
                  @SuppressWarnings("unchecked")
                  var r = (BackendFactory<Object, Object>) f;
                  return r;
                })
            .orElseGet(
                () -> {
                  try {
                    @SuppressWarnings("unchecked")
                    var r = (BackendFactory<Object, Object>) BackendLoader.findFactory(x -> true);
                    return r;
                  } catch (IllegalStateException e) {
                    throw new RuntimeException(
                        "Backend factory type is configured using the configuration option polaris.backend.name - available are: "
                            + BackendLoader.availableFactories()
                                .map(BackendFactory::name)
                                .collect(Collectors.joining(", ")),
                        e);
                  }
                });
    var configType = factory.configurationInterface();
    var config = backendSpecificConfigs.select(configType).get();
    var runtimeConfig = factory.buildConfiguration(config);
    return factory.buildBackend(runtimeConfig);
  }
}
