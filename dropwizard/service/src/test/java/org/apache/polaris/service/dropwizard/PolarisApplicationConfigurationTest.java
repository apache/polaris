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
package org.apache.polaris.service.dropwizard;

import static org.assertj.core.api.Assertions.assertThat;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.extension.persistence.impl.eclipselink.EclipseLinkPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.dropwizard.config.PolarisApplicationConfig;
import org.apache.polaris.service.dropwizard.test.PolarisApplicationUtils;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
public class PolarisApplicationConfigurationTest {

  @Nested
  class DefaultMetastore {
    private final DropwizardAppExtension<PolarisApplicationConfig> app =
        PolarisApplicationUtils.createTestPolarisApplication();

    @Test
    void testMetastoreType() {
      assertThat(app.getConfiguration().findService(MetaStoreManagerFactory.class))
          .isInstanceOf(InMemoryPolarisMetaStoreManagerFactory.class);
    }
  }

  @Nested
  class EclipseLinkMetastore {
    private final DropwizardAppExtension<PolarisApplicationConfig> app =
        PolarisApplicationUtils.createTestPolarisApplication(
            ConfigOverride.config("metaStoreManager.type", "eclipse-link"),
            ConfigOverride.config("metaStoreManager.persistence-unit", "test-unit"),
            ConfigOverride.config("metaStoreManager.conf-file", "/test-conf-file"));

    @Test
    void testMetastoreType() {
      assertThat(app.getConfiguration().findService(MetaStoreManagerFactory.class))
          .isInstanceOf(EclipseLinkPolarisMetaStoreManagerFactory.class)
          .extracting("persistenceUnitName", "confFile")
          .containsExactly("test-unit", "/test-conf-file");
    }
  }
}
