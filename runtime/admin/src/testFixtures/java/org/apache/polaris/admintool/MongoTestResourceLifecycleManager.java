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
package org.apache.polaris.admintool;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.apache.polaris.persistence.nosql.mongodb.MongoDbBackendTestFactory;

public class MongoTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  private MongoDbBackendTestFactory mongoDbBackendTestFactory;
  private DevServicesContext context;

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.context = context;
  }

  @Override
  @SuppressWarnings("resource")
  public Map<String, String> start() {
    mongoDbBackendTestFactory = new MongoDbBackendTestFactory();
    mongoDbBackendTestFactory.start(context.containerNetworkId());
    return Map.of(
        "quarkus.mongodb.connection-string", mongoDbBackendTestFactory.connectionString());
  }

  @Override
  public void stop() {
    if (mongoDbBackendTestFactory != null) {
      try {
        mongoDbBackendTestFactory.stop();
      } finally {
        mongoDbBackendTestFactory = null;
      }
    }
  }
}
