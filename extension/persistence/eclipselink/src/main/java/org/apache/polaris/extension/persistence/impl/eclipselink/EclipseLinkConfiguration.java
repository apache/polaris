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
package org.apache.polaris.extension.persistence.impl.eclipselink;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.nio.file.Path;
import java.util.Optional;

@ConfigMapping(prefix = "polaris.persistence.eclipselink")
public interface EclipseLinkConfiguration {

  /**
   * The path to the EclipseLink configuration file. If not provided, the default (built-in)
   * configuration file will be used.
   */
  Optional<Path> configurationFile();

  /**
   * The name of the persistence unit to use. If not provided, or if {@link #configurationFile()} is
   * not provided, the default persistence unit name ({@code polaris}) will be used.
   */
  @WithDefault("polaris")
  String persistenceUnit();
}
