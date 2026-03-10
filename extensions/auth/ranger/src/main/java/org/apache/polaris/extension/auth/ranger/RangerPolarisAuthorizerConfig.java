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
import org.apache.commons.lang3.StringUtils;
import org.apache.polaris.extension.auth.ranger.utils.RangerUtils;
import org.apache.polaris.immutables.PolarisImmutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PolarisImmutable
@ConfigMapping(prefix = "polaris.authorization.ranger")
public interface RangerPolarisAuthorizerConfig {
  Logger LOG = LoggerFactory.getLogger(RangerPolarisAuthorizerConfig.class);

  String PROP_RANGER_CONFIG_FILE_NAME = "polaris.authorization.ranger.config-file-name";
  String PROP_POLARIS_SERVICE_NAME = "ranger.plugin.polaris.service.name";

  String ERR_INVALID_AUTHORIZER_CONFIG_MISSING_CONFIG_FILE_NAME =
      "Invalid Ranger authorizer configuration: missing configuration "
          + PROP_RANGER_CONFIG_FILE_NAME;
  String ERR_INVALID_CONFIG_FILE_MISSING_CONFIGURATION =
      "Invalid Ranger authorizer configuration file %s: missing mandatory configuration %s";

  Optional<String> configFileName();

  default void validate() {
    boolean isConfigFileNamePresent = configFileName().isPresent();

    if (!isConfigFileNamePresent) {
      LOG.error(ERR_INVALID_AUTHORIZER_CONFIG_MISSING_CONFIG_FILE_NAME);

      throw new IllegalStateException(ERR_INVALID_AUTHORIZER_CONFIG_MISSING_CONFIG_FILE_NAME);
    }

    String fileName = configFileName().get();

    Properties prop = RangerUtils.loadProperties(fileName);

    String serviceName = prop.getProperty(PROP_POLARIS_SERVICE_NAME);

    if (StringUtils.isBlank(serviceName)) {
      LOG.error(
          "{}",
          ERR_INVALID_CONFIG_FILE_MISSING_CONFIGURATION.formatted(
              fileName, PROP_POLARIS_SERVICE_NAME));

      throw new IllegalStateException(
          ERR_INVALID_CONFIG_FILE_MISSING_CONFIGURATION.formatted(
              fileName, PROP_POLARIS_SERVICE_NAME));
    }
  }
}
