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

import io.dropwizard.core.cli.ConfiguredCommand;
import io.dropwizard.core.setup.Bootstrap;
import java.util.Map;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.auth.PolarisSecretsManager.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command for bootstrapping root level service principals for each realm. This command will invoke
 * a default implementation which generates random user id and secret. These credentials will be
 * printed out to the log and standard output (stdout).
 */
public class BootstrapRealmsCommand extends ConfiguredCommand<PolarisApplicationConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapRealmsCommand.class);

  public BootstrapRealmsCommand() {
    super("bootstrap", "bootstraps principal credentials for all realms and prints them to log");
  }

  @Override
  protected void run(
      Bootstrap<PolarisApplicationConfig> bootstrap,
      Namespace namespace,
      PolarisApplicationConfig configuration) {
    MetaStoreManagerFactory metaStoreManagerFactory = configuration.getMetaStoreManagerFactory();

    PolarisConfigurationStore configurationStore = configuration.getConfigurationStore();

    // Execute the bootstrap
    Map<String, PrincipalSecretsResult> results =
        metaStoreManagerFactory.bootstrapRealms(configuration.getDefaultRealms());

    // Log any errors:
    boolean success = true;
    for (Map.Entry<String, PrincipalSecretsResult> result : results.entrySet()) {
      if (!result.getValue().isSuccess()) {
        LOGGER.error(
            "Bootstrapping `{}` failed: {}",
            result.getKey(),
            result.getValue().getReturnStatus().toString());
        success = false;
      }
    }

    if (success) {
      LOGGER.info("Bootstrap completed successfully.");
    } else {
      LOGGER.error("Bootstrap encountered errors during operation.");
    }
  }
}
