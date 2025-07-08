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

package org.apache.polaris.service.quarkus.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.auth.ActiveRolesProvider;
import org.apache.polaris.service.auth.DefaultActiveRolesProvider;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(DeprecatedActiveRolesProviderConfigurationTest.Profile.class)
public class DeprecatedActiveRolesProviderConfigurationTest {

  @Inject Instance<ActiveRolesProvider> activeRolesProvider;

  @Test
  @ActivateRequestContext
  void testDeprecatedConfiguration() {
    // This test fails if the deprecated configuration is not honored.
    assertThat(activeRolesProvider.get()).isInstanceOf(DefaultActiveRolesProvider.class);
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.active-roles-provider.type", "default")
          .put("polaris.authentication.active-roles-provider.type", "non-existent")
          .build();
    }

    @Produces
    @RequestScoped
    @Alternative
    @Priority(1)
    public RealmContext realmContext() {
      return () -> "test";
    }
  }
}
