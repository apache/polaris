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
package org.apache.polaris.service.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.Profiles;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@QuarkusTest
@TestProfile(ExternalAuthenticatorTest.Profile.class)
class ExternalAuthenticatorTest {

  private static final String REALM_NAME = "external-realm";

  @Inject AuthenticationRealmConfiguration realmConfiguration;
  @Inject @Any Instance<Authenticator> authenticators;
  @Inject RealmContextHolder realmContextHolder;
  private Authenticator authenticator;

  @BeforeEach
  void setUp(TestInfo testInfo) {
    RealmContext realmContext = () -> REALM_NAME + "-" + testInfo.getDisplayName();
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
    realmContextHolder.set(realmContext);
    authenticator =
        authenticators
            .select(Identifier.Literal.of(realmConfiguration.authenticator().type()))
            .get();
  }

  @Test
  void selectsExternalAuthenticator() {
    assertThat(authenticator).isInstanceOf(ExternalAuthenticator.class);
  }

  @Test
  void buildsPrincipalFromClaims() {
    PolarisCredential credential =
        PolarisCredential.of(42L, "external-user", Set.of("roleA", "roleB"), true);

    PolarisPrincipal principal = authenticator.authenticate(credential);

    assertThat(principal.getName()).isEqualTo("external-user");
    assertThat(principal.getRoles()).containsExactlyInAnyOrder("roleA", "roleB");
    assertThat(principal.getProperties())
        .containsEntry("external", "true")
        .containsEntry("principalId", "42")
        .containsEntry("principalName", "external-user");
  }

  @Test
  void fallsBackToPrincipalIdWhenNameMissing() {
    PolarisCredential credential = PolarisCredential.of(7L, null, Set.of("roleA"), true);

    PolarisPrincipal principal = authenticator.authenticate(credential);

    assertThat(principal.getName()).isEqualTo("7");
    assertThat(principal.getRoles()).containsExactly("roleA");
  }

  @Test
  void rejectsMissingIdentifiers() {
    PolarisCredential credential = PolarisCredential.of(null, null, Set.of(), true);

    assertThatThrownBy(() -> authenticator.authenticate(credential))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  void marksCredentialAsExternalWhenFlagMissing() {
    PolarisCredential credential = PolarisCredential.of(9L, "fallback", Set.of("roleA"));

    PolarisPrincipal principal = authenticator.authenticate(credential);

    assertThat(principal.getProperties()).containsEntry("external", "true");
  }

  public static class Profile extends Profiles.DefaultProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> overrides = new HashMap<>(super.getConfigOverrides());
      overrides.put("polaris.authentication.type", "external");
      overrides.put("polaris.authentication.authenticator.type", "external");
      overrides.put("polaris.authentication.token-broker.type", "none");
      overrides.put("polaris.authentication.token-service.type", "disabled");
      return overrides;
    }
  }
}
