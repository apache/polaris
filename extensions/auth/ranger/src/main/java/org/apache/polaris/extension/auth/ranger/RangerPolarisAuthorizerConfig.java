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

@ConfigMapping(prefix = "polaris.authorization.ranger")
public interface RangerPolarisAuthorizerConfig {
  Optional<String> configFileName();

  default void validate() {
    boolean isConfigFileNamePresent = configFileName().isPresent();

    if (!isConfigFileNamePresent) {
      throw new IllegalStateException(
          "Invalid Ranger authorizer configuration: missing configuration polaris.authorization.ranger.config-file-name");
    }

    String fileName = configFileName().get();

    Properties prop = RangerUtils.loadProperties(fileName);

    String cfgName = "ranger.plugin.polaris.service.name";
    String serviceName = prop.getProperty(cfgName);

    if (StringUtils.isBlank(serviceName)) {
      throw new IllegalStateException(
          "Invalid Ranger authorizer configuration file "
              + fileName
              + ": missing mandatory configuration "
              + cfgName);
    }
  }
}
