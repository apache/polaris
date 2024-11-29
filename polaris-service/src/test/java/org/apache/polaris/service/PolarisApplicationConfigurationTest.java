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
package org.apache.polaris.service;

import static org.assertj.core.api.Assertions.assertThat;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.apache.polaris.extension.persistence.impl.eclipselink.EclipseLinkPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
public class PolarisApplicationConfigurationTest {

  public static final String CONFIG_PATH =
      ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml");
  // Bind to random ports to support parallelism
  public static final ConfigOverride RANDOM_APP_PORT =
      ConfigOverride.config("server.applicationConnectors[0].port", "0");
  public static final ConfigOverride RANDOM_ADMIN_PORT =
      ConfigOverride.config("server.adminConnectors[0].port", "0");

  @Nested
  class DefaultMetastore {
    private final DropwizardAppExtension<PolarisApplicationConfig> app =
        new DropwizardAppExtension<>(
            PolarisApplication.class, CONFIG_PATH, RANDOM_APP_PORT, RANDOM_ADMIN_PORT);

    @Test
    void testMetastoreType() {
      assertThat(app.getConfiguration().getMetaStoreManagerFactory())
          .isInstanceOf(InMemoryPolarisMetaStoreManagerFactory.class);
    }
  }

  @Nested
  class EclipseLinkMetastore {
    private final DropwizardAppExtension<PolarisApplicationConfig> app =
        new DropwizardAppExtension<>(
            PolarisApplication.class,
            CONFIG_PATH,
            RANDOM_APP_PORT,
            RANDOM_ADMIN_PORT,
            ConfigOverride.config("metaStoreManager.type", "eclipse-link"),
            ConfigOverride.config("metaStoreManager.persistence-unit", "test-unit"),
            ConfigOverride.config("metaStoreManager.conf-file", "/test-conf-file"));

    @Test
    void testMetastoreType() {
      assertThat(app.getConfiguration().getMetaStoreManagerFactory())
          .isInstanceOf(EclipseLinkPolarisMetaStoreManagerFactory.class)
          .extracting("persistenceUnitName", "confFile")
          .containsExactly("test-unit", "/test-conf-file");
    }
  }
}
