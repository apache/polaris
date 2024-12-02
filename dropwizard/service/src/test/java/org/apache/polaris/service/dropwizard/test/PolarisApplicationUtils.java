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
package org.apache.polaris.service.dropwizard.test;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.polaris.service.dropwizard.PolarisApplication;
import org.apache.polaris.service.dropwizard.config.PolarisApplicationConfig;

public class PolarisApplicationUtils {

  public static DropwizardAppExtension<PolarisApplicationConfig> createTestPolarisApplication(
      ConfigOverride... configOverrides) {
    List<ConfigOverride> appConfigOverrides = new ArrayList<>();
    // Bind to random port to support parallelism
    appConfigOverrides.add(ConfigOverride.config("server.applicationConnectors[0].port", "0"));
    appConfigOverrides.add(ConfigOverride.config("server.adminConnectors[0].port", "0"));
    // add the other input configurations
    appConfigOverrides.addAll(Arrays.asList(configOverrides));

    return new DropwizardAppExtension<>(
        PolarisApplication.class,
        ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
        appConfigOverrides.toArray(new ConfigOverride[0]));
  }
}
