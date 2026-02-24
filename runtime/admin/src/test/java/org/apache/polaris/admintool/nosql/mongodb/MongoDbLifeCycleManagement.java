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
package org.apache.polaris.admintool.nosql.mongodb;

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.testcontainers.mongodb.MongoDBContainer;

public class MongoDbLifeCycleManagement
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  private static final String MONGO_DB_NAME = "polaris_test";
  private MongoDBContainer mongoContainer;
  private DevServicesContext context;

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.context = context;
  }

  @Override
  @SuppressWarnings("resource")
  public Map<String, String> start() {
    mongoContainer =
        new MongoDBContainer(
            containerSpecHelper("mongodb", MongoDbLifeCycleManagement.class)
                .dockerImageName(null)
                .asCompatibleSubstituteFor("mongo"));

    context.containerNetworkId().ifPresent(mongoContainer::withNetworkMode);
    mongoContainer.start();

    return Map.of(
        "polaris.persistence.type",
        "nosql",
        "polaris.persistence.nosql.backend",
        "MongoDb",
        "quarkus.mongodb.connection-string",
        mongoContainer.getReplicaSetUrl(MONGO_DB_NAME),
        "quarkus.mongodb.database",
        MONGO_DB_NAME);
  }

  @Override
  public void stop() {
    if (mongoContainer != null) {
      try {
        mongoContainer.stop();
      } finally {
        mongoContainer = null;
      }
    }
  }
}

