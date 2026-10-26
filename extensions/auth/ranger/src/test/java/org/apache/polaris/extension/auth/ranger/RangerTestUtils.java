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

package org.apache.polaris.extension.auth.ranger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;

public class RangerTestUtils {

  private static final String REALM_CONTEXT_NAME = "POLARIS";
  private static final String SERVICE_DEF_PLACEHOLDER = "@@POLARIS_RANGER_SERVICE_DEF@@";

  /**
   * Writes the {@code dev_polaris.json} authz fixture into {@code policyDir} by splicing the
   * shipped {@code polaris-ranger-servicedef.json} into the checked-in template, then points the
   * returned config at that directory via {@code LocalFolderPolicySource}. Keeping this in test
   * code (rather than a build-time Gradle task) keeps the fixture generation next to the test that
   * actually consumes it.
   */
  public static RangerPolarisAuthorizerConfig createConfig(Path policyDir) {
    writeAuthzTestFixture(policyDir);

    Map<String, String> properties = new HashMap<>();
    properties.put(
        "authz.default.policy.source.impl",
        "org.apache.ranger.admin.client.LocalFolderPolicySource");
    properties.put("authz.default.policy.source.local_folder.path", policyDir.toString());

    return createConfig("dev_polaris", properties);
  }

  private static void writeAuthzTestFixture(Path policyDir) {
    String serviceDef = readClasspathResource("/polaris-ranger-servicedef.json");
    String template =
        stripLicenseHeader(readClasspathResource("/authz_tests/dev_polaris.json.template"));
    String merged = template.replace(SERVICE_DEF_PLACEHOLDER, serviceDef);
    try {
      Files.writeString(policyDir.resolve("dev_polaris.json"), merged);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // Strips the ASF license header (required since this template is a checked-in source file)
  // that precedes the "{" starting the actual, not-quite-valid-JSON template content.
  private static String stripLicenseHeader(String text) {
    return text.substring(text.indexOf("*/") + 2).stripLeading();
  }

  private static String readClasspathResource(String resourcePath) {
    try (InputStream in = RangerTestUtils.class.getResourceAsStream(resourcePath)) {
      if (in == null) {
        throw new IOException(resourcePath + " not found on classpath");
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static RangerPolarisAuthorizerConfig createConfig(
      String serviceName, Map<String, String> properties) {
    return new RangerPolarisAuthorizerConfig() {
      @Override
      public Optional<String> serviceName() {
        return Optional.ofNullable(serviceName);
      }

      @Override
      public Map<String, String> properties() {
        return properties;
      }
    };
  }

  public static RealmConfig createRealmConfig() {
    return new RealmConfig() {
      @SuppressWarnings({"removal"})
      @Override
      public <T> T getConfig(String configName) {
        return null;
      }

      @SuppressWarnings({"removal"})
      @Override
      public <T> T getConfig(String configName, T defaultValue) {
        return null;
      }

      @Override
      public <T> T getConfig(PolarisConfiguration<T> config) {
        return config.defaultValue();
      }

      @Override
      public <T> T getConfig(PolarisConfiguration<T> config, CatalogEntity catalogEntity) {
        return config.defaultValue();
      }

      @Override
      public <T> T getConfig(
          PolarisConfiguration<T> config, Map<String, String> catalogProperties) {
        return config.defaultValue();
      }
    };
  }

  public static RealmContext createRealmContext() {
    return new RealmContext() {
      @Override
      public String getRealmIdentifier() {
        return REALM_CONTEXT_NAME;
      }
    };
  }
}
