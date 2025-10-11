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
package org.apache.polaris.service.auth.opa;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/** Test resource that manages a temporary token file for OPA authentication testing. */
public class OpaFileTokenTestResource implements QuarkusTestResourceLifecycleManager {
  private Path tokenFile;

  @Override
  public Map<String, String> start() {
    try {
      // Create temporary token file
      tokenFile = Files.createTempFile("opa-test-token", ".txt");
      Files.writeString(tokenFile, "test-opa-bearer-token-from-file-67890");

      // Return configuration that will be added to the test
      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.opa.auth.bearer.file-based.path", tokenFile.toString());
      config.put(
          "polaris.authorization.opa.auth.bearer.file-based.refresh-interval",
          "1"); // 1 second for testing

      return config;
    } catch (IOException e) {
      throw new RuntimeException("Failed to create test token file", e);
    }
  }

  @Override
  public void stop() {
    if (tokenFile != null) {
      try {
        Files.deleteIfExists(tokenFile);
      } catch (IOException e) {
        System.err.println("Warning: Failed to delete test token file: " + e.getMessage());
      }
    }
  }

  /** Get the token file path for tests that need to modify it. */
  public Path getTokenFile() {
    return tokenFile;
  }
}
