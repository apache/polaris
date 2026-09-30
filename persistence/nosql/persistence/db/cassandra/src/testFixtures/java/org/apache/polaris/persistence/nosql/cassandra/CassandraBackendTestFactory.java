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
package org.apache.polaris.persistence.nosql.cassandra;

import static java.lang.String.format;
import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraBackendConfig.DEFAULT_KEYSPACE;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.testextension.BackendTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class CassandraBackendTestFactory implements BackendTestFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraBackendTestFactory.class);

  public static final Integer CQL_PORT = 9042;

  private static final String JVM_OPTS_TEST =
      "-Dcassandra.skip_wait_for_gossip_to_settle=0 "
          + "-Dcassandra.num_tokens=1 "
          + "-Dcassandra.initial_token=0";

  public static final String NAME = "Cassandra";

  private final Duration timeout;

  private CassandraContainer container;
  private String localDc;
  private String host;
  private Integer port;

  public CassandraBackendTestFactory() {
    // Increase some timeouts to avoid flakiness
    this.timeout = Duration.ofSeconds(15);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Backend createNewBackend() {
    var factory = new CassandraBackendFactory();
    var config =
        factory.buildConfiguration(
            CassandraConfiguration.BuildableCassandraConfiguration.builder()
                .driverOptions(
                    Map.of(
                        "basic.contact-points",
                        host + ":" + port,
                        "basic.load-balancing-policy.local-datacenter",
                        localDc,
                        "advanced.connection.connect-timeout",
                        timeout.toString()))
                .ddlTimeout(timeout)
                .dmlTimeout(timeout)
                .build());

    maybeCreateKeyspace(config.client());

    return factory.buildBackend(config);
  }

  private void maybeCreateKeyspace(CqlSession session) {
    var replicationFactor = 1;

    var metadata = session.getMetadata();

    var datacenters =
        metadata.getNodes().values().stream()
            .map(Node::getDatacenter)
            .distinct()
            .map(dc -> format("'%s': %d", dc, replicationFactor))
            .collect(Collectors.joining(", "));

    session.execute(
        format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', %s};",
            DEFAULT_KEYSPACE, datacenters));

    session.refreshSchema();
  }

  @Override
  public void start(Optional<String> containerNetworkId) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    var dockerImageName =
        containerSpecHelper("cassandra", CassandraBackendTestFactory.class)
            .dockerImageName(null)
            .asCompatibleSubstituteFor("cassandra");

    for (var retry = 0; ; retry++) {
      var c = new CassandraContainer(dockerImageName).withLogConsumer(new Slf4jLogConsumer(LOGGER));
      c.withEnv("JVM_OPTS", JVM_OPTS_TEST);
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

    port = containerNetworkId.isPresent() ? CQL_PORT : container.getMappedPort(CQL_PORT);
    host =
        containerNetworkId.isPresent()
            ? container.getCurrentContainerInfo().getConfig().getHostName()
            : container.getHost();

    localDc = container.getLocalDatacenter();
  }

  @Override
  public void start() {
    start(Optional.empty());
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
