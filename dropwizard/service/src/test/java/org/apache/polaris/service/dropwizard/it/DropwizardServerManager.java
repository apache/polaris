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
package org.apache.polaris.service.dropwizard.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.polaris.service.dropwizard.PolarisApplication;
import org.apache.polaris.service.dropwizard.config.PolarisApplicationConfig;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.Server;
import org.apache.polaris.service.it.ext.PolarisServerManager;
import org.junit.jupiter.api.extension.ExtensionContext;

public class DropwizardServerManager implements PolarisServerManager {
  // referenced in polaris-server-integrationtest.yml
  public static final String TEST_REALM = "POLARIS";
  public static final String SERVER_CONFIG_PATH = "polaris-server-integrationtest.yml";

  @Override
  public Server serverForContext(ExtensionContext context) {
    try {
      return new Holder();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class Holder implements Server {
    private final DropwizardAppExtension<PolarisApplicationConfig> ext;
    private final Path logDir;

    public Holder() throws IOException {
      logDir = Files.createTempDirectory("polaris-dw-");
      Path logFile = logDir.resolve("application.log").toAbsolutePath();

      Map<String, String> config = new HashMap<>();
      // Bind to random port to support parallelism
      config.put("server.applicationConnectors[0].port", "0");
      config.put("server.adminConnectors[0].port", "0");
      config.put("logging.appenders[1].type", "file");
      config.put("logging.appenders[1].currentLogFilename", logFile.toString());

      ConfigOverride[] overrides =
          config.entrySet().stream()
              .map((e) -> ConfigOverride.config(e.getKey(), e.getValue()))
              .toList()
              .toArray(new ConfigOverride[0]);
      ext =
          new DropwizardAppExtension<>(
              PolarisApplication.class,
              ResourceHelpers.resourceFilePath(SERVER_CONFIG_PATH),
              overrides);

      try {
        ext.before();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      assertThat(logFile)
          .exists()
          .content()
          .hasSizeGreaterThan(0)
          .doesNotContain("ERROR", "FATAL")
          .contains("PolarisApplication: Server started successfully");
    }

    @Override
    public String realmId() {
      return TEST_REALM;
    }

    @Override
    public URI baseUri() {
      return URI.create(String.format("http://localhost:%d", ext.getLocalPort()));
    }

    @Override
    public ClientCredentials adminCredentials() {
      // These credentials are injected via env. variables from build scripts.
      // Cf. POLARIS_BOOTSTRAP_POLARIS_ROOT_CLIENT_ID
      return new ClientCredentials("test-admin", "test-secret", "root");
    }

    @Override
    public void close() throws IOException {
      ext.after();
      FileUtils.deleteDirectory(logDir.toFile());
    }
  }
}
