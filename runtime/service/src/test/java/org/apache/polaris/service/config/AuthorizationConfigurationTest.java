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
package org.apache.polaris.service.config;

import static org.apache.polaris.service.config.AuthorizationConfiguration.DEFAULT_REALM_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(AuthorizationConfigurationTest.Profile.class)
public class AuthorizationConfigurationTest {

  @Inject AuthorizationConfiguration authorizationConfig;

  @Test
  void smokeTest() {
    assertThat(authorizationConfig.realms()).hasSize(3);
    assertThat(authorizationConfig.realms().get(DEFAULT_REALM_KEY).type()).isEqualTo("internal");
    assertThat(authorizationConfig.realms().get("realm1").type()).isEqualTo("test-authorizer");
    assertThat(authorizationConfig.realms().get("realm2").type()).isEqualTo("opa");

    assertThat(authorizationConfig.forRealm("realm1").type()).isEqualTo("test-authorizer");
    assertThat(authorizationConfig.forRealm("unknown-realm").type()).isEqualTo("internal");
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.authorization.type", "internal")
          .put("polaris.authorization.realm1.type", "test-authorizer")
          .put("polaris.authorization.realm2.type", "opa")
          .build();
    }
  }
}
