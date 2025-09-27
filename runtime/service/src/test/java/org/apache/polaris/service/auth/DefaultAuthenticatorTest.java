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
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Set;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;

@QuarkusTest
@TestProfile(PolarisAuthzTestBase.Profile.class)
public class DefaultAuthenticatorTest extends PolarisAuthzTestBase {

  private static final String PRINCIPAL_NO_ROLES = "principal-no-roles";

  @Inject
  @Identifier("default")
  DefaultAuthenticator authenticator;

  private PrincipalEntity principalNoRoles;

  @BeforeEach
  @Override
  public void before(TestInfo testInfo) {
    super.before(testInfo);

    PrincipalWithCredentials principal =
        adminService.createPrincipal(
            new PrincipalEntity.Builder()
                .setName(PRINCIPAL_NO_ROLES)
                .setCreateTimestamp(Instant.now().toEpochMilli())
                .build());

    principalNoRoles =
        rotateAndRefreshPrincipal(
            metaStoreManager, PRINCIPAL_NO_ROLES, principal.getCredentials(), polarisContext);
  }

  @Test
  void testNullPrincipalIdAndName() {
    // Given: credentials with both null principal ID and name
    PolarisCredential credentials =
        PolarisCredential.of(null, null, Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL));

    // When/Then: authentication should fail with NotAuthorizedException
    assertUnauthorized(credentials);
  }

  @Test
  void testPrincipalNotFoundByName() {
    // Given: credentials with a non-existent principal name
    PolarisCredential credentials =
        PolarisCredential.of(
            null, "non-existent-principal", Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL));

    // When/Then: authentication should fail with NotAuthorizedException
    assertUnauthorized(credentials);
  }

  @Test
  void testPrincipalNotFoundById() {
    // Given: credentials with a non-existent principal ID
    PolarisCredential credentials =
        PolarisCredential.of(999999L, null, Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL));

    // When/Then: authentication should fail with NotAuthorizedException
    assertUnauthorized(credentials);
  }

  @Test
  public void testFetchPrincipalThrowsServiceExceptionOnMetastoreException() {

    // Given: credentials with a non-existent principal ID
    PolarisCredential credentials =
        PolarisCredential.of(123L, null, Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL));

    metaStoreManager = Mockito.spy(metaStoreManager);
    when(metaStoreManager.loadEntity(polarisContext, 0L, 123L, PolarisEntityType.PRINCIPAL))
        .thenThrow(new RuntimeException("Metastore exception"));

    assertUnauthorized(credentials);
  }

  @Test
  void testAuthenticationByPrincipalId() {
    // Given: credentials with principal ID instead of name
    PolarisCredential credentials =
        PolarisCredential.of(
            principalEntity.getId(), null, Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL));

    // When: authenticating the principal
    PolarisPrincipal result = authenticator.authenticate(credentials);

    // Then: should return principal with all assigned roles
    assertPrincipal(result, principalEntity, PRINCIPAL_ROLE1, PRINCIPAL_ROLE2);
  }

  @Test
  void testPrincipalFoundByName() {
    // Given: credentials with existing principal name
    PolarisCredential credentials =
        PolarisCredential.of(null, PRINCIPAL_NAME, Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL));

    // When: authenticating the principal
    PolarisPrincipal result = authenticator.authenticate(credentials);

    // Then: should return principal with all assigned roles
    assertPrincipal(result, principalEntity, PRINCIPAL_ROLE1, PRINCIPAL_ROLE2);
  }

  @Test
  void testPrincipalFoundWithAllRolesRequested() {
    // Given: credentials requesting all roles for an existing principal
    PolarisCredential credentials =
        PolarisCredential.of(null, PRINCIPAL_NAME, Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL));

    // When: authenticating the principal
    PolarisPrincipal result = authenticator.authenticate(credentials);

    // Then: should return principal with all assigned roles
    assertPrincipal(result, principalEntity, PRINCIPAL_ROLE1, PRINCIPAL_ROLE2);
  }

  @Test
  void testPrincipalFoundWithSubsetOfRolesRequested() {
    // Given: credentials requesting only a subset of the principal's roles
    PolarisCredential credentials =
        PolarisCredential.of(
            null,
            PRINCIPAL_NAME,
            Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_PREFIX + PRINCIPAL_ROLE1));

    // When: authenticating the principal
    PolarisPrincipal result = authenticator.authenticate(credentials);

    // Then: should return principal with only the requested role
    assertPrincipal(result, principalEntity, PRINCIPAL_ROLE1);
  }

  @Test
  void testPrincipalFoundWithMultipleSpecificRolesRequested() {
    // Given: credentials requesting multiple specific roles
    PolarisCredential credentials =
        PolarisCredential.of(
            null,
            PRINCIPAL_NAME,
            Set.of(
                DefaultAuthenticator.PRINCIPAL_ROLE_PREFIX + PRINCIPAL_ROLE1,
                DefaultAuthenticator.PRINCIPAL_ROLE_PREFIX + PRINCIPAL_ROLE2));

    // When: authenticating the principal
    PolarisPrincipal result = authenticator.authenticate(credentials);

    // Then: should return principal with both requested roles
    assertPrincipal(result, principalEntity, PRINCIPAL_ROLE1, PRINCIPAL_ROLE2);
  }

  @Test
  void testPrincipalFoundButHasNoRolesAssigned() {
    // Given: credentials for a principal with no assigned roles
    PolarisCredential credentials =
        PolarisCredential.of(
            null, PRINCIPAL_NO_ROLES, Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL));

    // When: authenticating the principal
    PolarisPrincipal result = authenticator.authenticate(credentials);

    // Then: should return principal with empty roles set
    assertPrincipal(result, principalNoRoles);
  }

  @Test
  void testRequestedRolesDoNotMapToSystemRoles() {
    // Given: credentials requesting roles that don't exist in the system
    PolarisCredential credentials =
        PolarisCredential.of(
            null,
            PRINCIPAL_NAME,
            Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_PREFIX + "non-existent-role"));

    // When: authenticating the principal
    PolarisPrincipal result = authenticator.authenticate(credentials);

    // Then: should return principal with empty roles set (non-existent roles are filtered out)
    assertPrincipal(result, principalEntity);
  }

  @Test
  void testMixedValidAndInvalidRolesRequested() {
    // Given: credentials requesting both valid and invalid roles
    PolarisCredential credentials =
        PolarisCredential.of(
            null,
            PRINCIPAL_NAME,
            Set.of(
                DefaultAuthenticator.PRINCIPAL_ROLE_PREFIX + PRINCIPAL_ROLE1,
                DefaultAuthenticator.PRINCIPAL_ROLE_PREFIX
                    + "non-existent-role" // This should be ignored
                ));

    // When: authenticating the principal
    PolarisPrincipal result = authenticator.authenticate(credentials);

    // Then: should return principal with only the valid role
    assertPrincipal(result, principalEntity, PRINCIPAL_ROLE1);
  }

  @Test
  void testRolesWithoutPrefixAreIgnored() {
    // Given: credentials with roles that don't have the required prefix
    PolarisCredential credentials =
        PolarisCredential.of(
            null,
            PRINCIPAL_NAME,
            Set.of(
                DefaultAuthenticator.PRINCIPAL_ROLE_PREFIX + PRINCIPAL_ROLE1,
                "unprefixed-role", // This should be ignored
                "another-unprefixed-role" // This should also be ignored
                ));

    // When: authenticating the principal
    PolarisPrincipal result = authenticator.authenticate(credentials);

    // Then: should return principal with only the properly prefixed role
    assertPrincipal(result, principalEntity, PRINCIPAL_ROLE1);
  }

  @Test
  void testEmptyRolesRequestedReturnsEmptyRoles() {
    // Given: credentials with empty roles set
    PolarisCredential credentials =
        PolarisCredential.of(
            null, PRINCIPAL_NAME, Set.of() // Empty roles set
            );

    // When: authenticating the principal
    PolarisPrincipal result = authenticator.authenticate(credentials);

    // Then: should return principal with empty roles set
    assertPrincipal(result, principalEntity);
  }

  @Test
  void testPrincipalIdTakesPrecedenceOverName() {
    // Given: credentials with both principal ID and name (ID should take precedence)
    PolarisCredential credentials =
        PolarisCredential.of(
            principalEntity.getId(),
            "wrong-name", // This should be ignored since ID is provided
            Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL));

    // When: authenticating the principal
    PolarisPrincipal result = authenticator.authenticate(credentials);

    // Then: should return principal resolved by ID, not name
    assertPrincipal(result, principalEntity, PRINCIPAL_ROLE1, PRINCIPAL_ROLE2);
  }

  private void assertPrincipal(PolarisPrincipal result, PrincipalEntity entity, String... roles) {
    assertThat(result).isNotNull();
    assertThat(result.getName()).isEqualTo(entity.getName());
    assertThat(result.getRoles()).containsExactlyInAnyOrder(roles);
    assertThat(result.getProperties())
        .containsKey(PolarisEntityConstants.getClientIdPropertyName());
  }

  private void assertUnauthorized(PolarisCredential credentials) {
    assertThatThrownBy(() -> authenticator.authenticate(credentials))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessageContaining("Unable to authenticate");
  }
}
