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
package org.apache.polaris.extension.auth.opa.test;

import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.QuarkusTestProfile.TestResourceEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Shared Quarkus test profiles for OPA integration tests. */
public final class OpaTestProfiles {

  private OpaTestProfiles() {}

  /** OPA profile using a static bearer token. */
  public static class StaticToken implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.type", "opa");
      config.put("polaris.authorization.opa.auth.type", "bearer");
      config.put(
          "polaris.authorization.opa.auth.bearer.static-token.value",
          "test-opa-bearer-token-12345");
      config.put("polaris.authentication.authenticator.type", "passthrough");
      config.put("polaris.authentication.token-broker.type", "symmetric-key");
      config.put(
          "polaris.authentication.token-broker.symmetric-key.secret", OpaTestConstants.JWT_SECRET);
      config.put("polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"", "[\"FILE\"]");
      config.put("polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"", "true");
      config.put("polaris.readiness.ignore-severe-issues", "true");
      return config;
    }

    @Override
    public List<TestResourceEntry> testResources() {
      return List.of(new TestResourceEntry(OpaTestResource.class));
    }
  }

  /** OPA profile using a bearer token read from a file. */
  public static class FileToken implements QuarkusTestProfile {
    // Exposed for tests that may need to inspect the created token file.
    public static Path tokenFilePath;

    @Override
    public Map<String, String> getConfigOverrides() {
      try {
        tokenFilePath = Files.createTempFile("opa-test-token", ".txt");
        Files.writeString(tokenFilePath, "test-opa-bearer-token-from-file");

        Map<String, String> config = new HashMap<>();
        config.put("polaris.authorization.type", "opa");
        config.put("polaris.authorization.opa.auth.type", "bearer");
        config.put(
            "polaris.authorization.opa.auth.bearer.file-based.path", tokenFilePath.toString());
        config.put("polaris.authorization.opa.auth.bearer.file-based.refresh-interval", "PT1S");
        config.put("polaris.authentication.authenticator.type", "passthrough");
        config.put("polaris.authentication.token-broker.type", "symmetric-key");
        config.put(
            "polaris.authentication.token-broker.symmetric-key.secret",
            OpaTestConstants.JWT_SECRET);
        config.put("polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"", "[\"FILE\"]");
        config.put("polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"", "true");
        config.put("polaris.readiness.ignore-severe-issues", "true");
        return config;
      } catch (IOException e) {
        throw new RuntimeException("Failed to create test token file", e);
      }
    }

    @Override
    public List<TestResourceEntry> testResources() {
      return List.of(new TestResourceEntry(OpaTestResource.class));
    }
  }
}
