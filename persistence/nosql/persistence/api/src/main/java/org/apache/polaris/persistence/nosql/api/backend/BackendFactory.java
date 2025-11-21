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
package org.apache.polaris.persistence.nosql.api.backend;

import jakarta.annotation.Nonnull;

/**
 * Factory responsible to produce {@link Backend} instances. Usually only one {@link Backend}
 * instance is ever produced and active in a production environment.
 */
public interface BackendFactory<RUNTIME_CONFIG, CONFIG_INTERFACE> {
  /** Human-readable name. */
  String name();

  @Nonnull
  Backend buildBackend(@Nonnull RUNTIME_CONFIG backendConfig);

  Class<CONFIG_INTERFACE> configurationInterface();

  RUNTIME_CONFIG buildConfiguration(CONFIG_INTERFACE config);
}
