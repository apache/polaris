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
package org.apache.polaris.persistence.nosql.dynamodb;

import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.testextension.BackendTestFactory;
import org.apache.polaris.test.floci.aws.FlociAwsContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;

public class DynamoDbBackendTestFactory implements BackendTestFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDbBackendTestFactory.class);

  public static final String NAME = "DynamoDb";

  private FlociAwsContainer container;
  private String endpointURI;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Backend createNewBackend() {
    var config = ImmutableBuildableDynamoDbConfiguration.builder().endpointURI(endpointURI).build();
    var factory = new DynamoDbBackendFactory();
    var backendConfig = factory.buildConfiguration(config);
    return factory.buildBackend(backendConfig);
  }

  @Override
  public void start() {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    for (int retry = 0; ; retry++) {
      @SuppressWarnings("resource")
      var c = new FlociAwsContainer();
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

    Integer port = container.getFirstMappedPort();
    String host = container.getHost();

    endpointURI = String.format("http://%s:%d", host, port);
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
}
