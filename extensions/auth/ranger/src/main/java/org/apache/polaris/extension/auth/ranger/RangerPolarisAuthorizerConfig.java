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
package org.apache.polaris.extension.auth.ranger;

import io.smallrye.config.ConfigMapping;
import java.util.Optional;
import java.util.Properties;
import org.apache.polaris.extension.auth.ranger.utils.RangerUtils;
import org.apache.polaris.immutables.PolarisImmutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PolarisImmutable
@ConfigMapping(prefix = "polaris.authorization.ranger")
public interface RangerPolarisAuthorizerConfig {
  Logger LOG = LoggerFactory.getLogger(RangerPolarisAuthorizerConfig.class);

  String INVALID_CONFIG_FILE_NAME_ERROR =
      "Ranger Authorization failed due to incorrect ranger authorization plugin configuration";

  Optional<String> configFileName();

  default void validate() {
    boolean isConfigFileNamePresent = configFileName().isPresent();

    if (!isConfigFileNamePresent) {
      LOG.info(
          "polaris.authorization.ranger.config-file-name is not configured, all authorization will fail.");

      throw new IllegalStateException(INVALID_CONFIG_FILE_NAME_ERROR);
    }

    Properties prop = RangerUtils.loadProperties(configFileName().get());

    if (prop.isEmpty()) {
      LOG.info(
          "ranger configuration file: {} does not have any valid configuration.",
          configFileName().get());

      throw new IllegalStateException(INVALID_CONFIG_FILE_NAME_ERROR);
    }
  }
}
