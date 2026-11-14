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

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Map;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupContext;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class CassandraStartupAction implements PolarisServerStartupAction {
  private static final int CQL_PORT = 9042;
  private static final String TLS_RESOURCE_ROOT = "cassandra-mtls/";

  private MtlsCassandraContainer container;

  @Override
  public void start(PolarisServerStartupContext context) {
    var image =
        containerSpecHelper("cassandra", CassandraBackendTestFactory.class)
            .dockerImageName(null)
            .asCompatibleSubstituteFor("cassandra");
    container = new MtlsCassandraContainer(image);
    container.withSsl(TLS_RESOURCE_ROOT + "client.crt", TLS_RESOURCE_ROOT + "client.key");
    container.withInitScript(TLS_RESOURCE_ROOT + "init.cql");
    container.start();

    context
        .getSystemProperties()
        .putAll(
            Map.ofEntries(
                Map.entry("polaris.persistence.type", "nosql"),
                Map.entry("polaris.persistence.auto-bootstrap-types", "nosql"),
                Map.entry("polaris.persistence.nosql.backend", "Cassandra"),
                Map.entry(
                    "polaris.persistence.nosql.cassandra.driver.basic.contact-points",
                    container.getHost() + ":" + container.getMappedPort(CQL_PORT)),
                Map.entry(
                    "polaris.persistence.nosql.cassandra.driver.basic.load-balancing-policy.local-datacenter",
                    container.getLocalDatacenter()),
                Map.entry("polaris.persistence.nosql.cassandra.keyspace", "polaris"),
                Map.entry(
                    "polaris.persistence.nosql.cassandra.tls-configuration-name", "cassandra"),
                Map.entry("polaris.persistence.nosql.cassandra.tls-hostname-validation", "false"),
                Map.entry(
                    "quarkus.tls.cassandra.key-store.pem.client.key", resourcePath("client.key")),
                Map.entry(
                    "quarkus.tls.cassandra.key-store.pem.client.cert", resourcePath("client.crt")),
                Map.entry("quarkus.tls.cassandra.trust-all", "true")));
  }

  @Override
  public void close() {
    if (container != null) {
      try {
        container.stop();
      } finally {
        container = null;
      }
    }
  }

  private static String resourcePath(String resource) {
    try {
      return Path.of(
              CassandraStartupAction.class
                  .getClassLoader()
                  .getResource(TLS_RESOURCE_ROOT + resource)
                  .toURI())
          .toString();
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Unable to resolve Cassandra mTLS test resource.", e);
    }
  }

  private static final class MtlsCassandraContainer extends CassandraContainer {
    private MtlsCassandraContainer(DockerImageName image) {
      super(image);
      withCommand("bash", "/etc/cassandra/cassandra-mtls-start.sh");
    }

    @Override
    protected void configure() {
      super.configure();
      withCopyFileToContainer(
          MountableFile.forClasspathResource(TLS_RESOURCE_ROOT + "client.key"),
          "/etc/cassandra/tls/client.key");
      withCopyFileToContainer(
          MountableFile.forClasspathResource(TLS_RESOURCE_ROOT + "client.crt"),
          "/etc/cassandra/tls/client.crt");
      withCopyFileToContainer(
          MountableFile.forClasspathResource(TLS_RESOURCE_ROOT + "cassandra-mtls-start.sh"),
          "/etc/cassandra/cassandra-mtls-start.sh");
    }
  }
}
