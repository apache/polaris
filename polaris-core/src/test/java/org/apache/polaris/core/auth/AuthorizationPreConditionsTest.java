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
package org.apache.polaris.core.auth;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.junit.jupiter.api.Test;

public class AuthorizationPreConditionsTest {

  private static final String ROTATION_KEY =
      PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE;

  private static RealmConfig realmConfigWithEnforcement(boolean enforce) {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(
            FeatureConfiguration.ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING))
        .thenReturn(enforce);
    return realmConfig;
  }

  @Test
  public void testFlagOff_noException() {
    PolarisPrincipal principal =
        PolarisPrincipal.of("alice", Map.of(ROTATION_KEY, "true"), Set.of("role"));

    assertThatCode(
            () ->
                AuthorizationPreConditions.checkCredentialRotationRequired(
                    principal,
                    PolarisAuthorizableOperation.LIST_CATALOGS,
                    realmConfigWithEnforcement(false)))
        .doesNotThrowAnyException();
  }

  @Test
  public void testFlagOn_rotateCredentialsOp_noException() {
    PolarisPrincipal principal =
        PolarisPrincipal.of("alice", Map.of(ROTATION_KEY, "true"), Set.of("role"));

    assertThatCode(
            () ->
                AuthorizationPreConditions.checkCredentialRotationRequired(
                    principal,
                    PolarisAuthorizableOperation.ROTATE_CREDENTIALS,
                    realmConfigWithEnforcement(true)))
        .doesNotThrowAnyException();
  }

  @Test
  public void testFlagOn_nonRotationOp_propertySet_throwsForbidden() {
    PolarisPrincipal principal =
        PolarisPrincipal.of("alice", Map.of(ROTATION_KEY, "true"), Set.of("role"));

    assertThatThrownBy(
            () ->
                AuthorizationPreConditions.checkCredentialRotationRequired(
                    principal,
                    PolarisAuthorizableOperation.LIST_CATALOGS,
                    realmConfigWithEnforcement(true)))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE");
  }

  @Test
  public void testFlagOn_nonRotationOp_propertyNotSet_noException() {
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    assertThatCode(
            () ->
                AuthorizationPreConditions.checkCredentialRotationRequired(
                    principal,
                    PolarisAuthorizableOperation.LIST_CATALOGS,
                    realmConfigWithEnforcement(true)))
        .doesNotThrowAnyException();
  }
}
