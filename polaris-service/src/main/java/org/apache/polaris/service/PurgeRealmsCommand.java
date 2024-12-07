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
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Command for purging all metadata associated with a realm */
public class PurgeRealmsCommand extends ConfiguredCommand<PolarisApplicationConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PurgeRealmsCommand.class);

  public PurgeRealmsCommand() {
    super("purge", "purge principal credentials for all realms and prints them to log");
  }

  @Override
  protected void run(
      Bootstrap<PolarisApplicationConfig> bootstrap,
      Namespace namespace,
      PolarisApplicationConfig configuration)
      throws Exception {
    MetaStoreManagerFactory metaStoreManagerFactory =
        configuration.findService(MetaStoreManagerFactory.class);

    metaStoreManagerFactory.purgeRealms(configuration.getDefaultRealms());

    LOGGER.info("Purge completed successfully.");
  }
}
