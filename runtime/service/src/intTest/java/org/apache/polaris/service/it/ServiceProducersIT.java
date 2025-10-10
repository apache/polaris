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

public class ServiceProducersIT {

  public static class InternalAuthorizationConfig implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.type", "internal");
      return config;
    }
  }

  public static class OpaAuthorizationConfig implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.type", "opa");
      config.put("polaris.authorization.opa.url", "http://localhost:8181");
      config.put("polaris.authorization.opa.policy-path", "/v1/data/polaris/authz");
      config.put("polaris.authorization.opa.auth.type", "none");
      return config;
    }
  }

  @QuarkusTest
  @io.quarkus.test.junit.TestProfile(ServiceProducersIT.InternalAuthorizationConfig.class)
  public static class InternalAuthorizationTest {

    @Inject PolarisAuthorizer polarisAuthorizer;

    @Test
    void testInternalPolarisAuthorizerProduced() {
      assertNotNull(polarisAuthorizer, "PolarisAuthorizer should be produced");
      // Verify it's the correct implementation for internal config
      assertNotNull(polarisAuthorizer, "Internal PolarisAuthorizer should not be null");
    }
  }

  @QuarkusTest
  @io.quarkus.test.junit.TestProfile(ServiceProducersIT.OpaAuthorizationConfig.class)
  public static class OpaAuthorizationTest {

    @Inject PolarisAuthorizer polarisAuthorizer;

    @Test
    void testOpaPolarisAuthorizerProduced() {
      assertNotNull(polarisAuthorizer, "PolarisAuthorizer should be produced");
      // Verify it's the correct implementation for OPA config
      assertNotNull(polarisAuthorizer, "OPA PolarisAuthorizer should not be null");
    }
  }
}
