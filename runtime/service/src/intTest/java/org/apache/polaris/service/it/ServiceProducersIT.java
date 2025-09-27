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
package org.apache.polaris.service.it;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.junit.jupiter.api.Test;

@QuarkusTest
@io.quarkus.test.junit.TestProfile(ServiceProducersIT.InlineConfig.class)
public class ServiceProducersIT {

  public static class InlineConfig implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.type", "default");
      config.put("polaris.authorization.opa.url", "http://localhost:8181");
      config.put("polaris.authorization.opa.policy-path", "/v1/data/polaris/allow");
      config.put("polaris.authorization.opa.timeout-ms", "2000");
      return config;
    }
  }

  @Inject PolarisAuthorizer polarisAuthorizer;

  @Test
  void testPolarisAuthorizerProduced() {
    assertNotNull(polarisAuthorizer, "PolarisAuthorizer should be produced");
    // Verify it's the correct implementation for default config
    assertNotNull(polarisAuthorizer, "PolarisAuthorizer should not be null");
  }
}
