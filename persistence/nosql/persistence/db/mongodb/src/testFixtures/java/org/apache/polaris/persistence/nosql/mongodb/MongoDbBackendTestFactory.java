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

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;

import java.util.Optional;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.testextension.BackendTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.mongodb.MongoDBContainer;

public class MongoDbBackendTestFactory implements BackendTestFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbBackendTestFactory.class);
  public static final String NAME = MongoDbBackendFactory.NAME;
  public static final String MONGO_DB_NAME = "test";
  public static final int MONGO_PORT = 27017;

  private MongoDBContainer container;
  private String connectionString;

  @Override
  public Backend createNewBackend() {
    var factory = new MongoDbBackendFactory();
    var config =
        factory.buildConfiguration(
            new MongoDbConfiguration() {
              @Override
              public Optional<String> connectionString() {
                return Optional.of(connectionString);
              }

              @Override
              public Optional<String> databaseName() {
                return Optional.of(MONGO_DB_NAME);
              }

              @Override
              public Optional<Boolean> allowPrefixDeletion() {
                return Optional.empty();
              }
            });
    return factory.buildBackend(config);
  }

  public String connectionString() {
    return connectionString;
  }

  @Override
  public void start() {
    start(Optional.empty());
  }

  @Override
  public void start(Optional<String> containerNetworkId) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    var dockerImage =
        containerSpecHelper("mongodb", MongoDbBackendTestFactory.class)
            .dockerImageName(null)
            .asCompatibleSubstituteFor("mongo");

    for (var retry = 0; ; retry++) {
      var c = new MongoDBContainer(dockerImage).withLogConsumer(new Slf4jLogConsumer(LOGGER));
      containerNetworkId.ifPresent(c::withNetworkMode);
      try {
        c.start();
        container = c;
        break;
      } catch (ContainerLaunchException e) {
        c.close();
        if (e.getCause() != null && retry < 3) {
          LOGGER.warn("Launch of container {} failed, will retry...", c.getDockerImageName(), e);
          continue;
        }
        LOGGER.error("Launch of container {} failed", c.getDockerImageName(), e);
        throw new RuntimeException(e);
      }
    }

    connectionString = container.getReplicaSetUrl(MONGO_DB_NAME);

    if (containerNetworkId.isPresent()) {
      var hostPort = container.getHost() + ':' + container.getMappedPort(MONGO_PORT);
      var networkHostPort =
          container.getCurrentContainerInfo().getConfig().getHostName() + ':' + MONGO_PORT;
      connectionString = connectionString.replace(hostPort, networkHostPort);
    }
  }

  @Override
  public void stop() {
    try {
      if (container != null) {
        container.stop();
      }
    } finally {
      container = null;
    }
  }

  @Override
  public String name() {
    return NAME;
  }
}
