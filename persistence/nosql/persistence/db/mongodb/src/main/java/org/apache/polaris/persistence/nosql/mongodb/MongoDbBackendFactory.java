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
package org.apache.polaris.persistence.nosql.mongodb;

import com.mongodb.client.MongoClients;
import jakarta.annotation.Nonnull;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.BackendFactory;

public class MongoDbBackendFactory
    implements BackendFactory<MongoDbBackendConfig, MongoDbConfiguration> {
  public static final String NAME = "MongoDb";

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  @Nonnull
  public Backend buildBackend(@Nonnull MongoDbBackendConfig backendConfig) {
    return new MongoDbBackend(backendConfig);
  }

  @Override
  public Class<MongoDbConfiguration> configurationInterface() {
    return MongoDbConfiguration.class;
  }

  @Override
  public MongoDbBackendConfig buildConfiguration(MongoDbConfiguration config) {
    return new MongoDbBackendConfig(
        config
            .databaseName()
            .orElseThrow(
                () -> new IllegalStateException("Mandatory MongoDb database name missing")),
        MongoClients.create(
            config
                .connectionString()
                .orElseThrow(
                    () ->
                        new IllegalStateException("Mandatory MongoDb connection string missing"))),
        true,
        config.allowPrefixDeletion().orElse(false));
  }
}
