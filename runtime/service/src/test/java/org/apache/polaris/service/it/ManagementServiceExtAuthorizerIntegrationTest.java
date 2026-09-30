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

import static org.apache.polaris.core.auth.PolarisAuthorizableOperation.LIST_CATALOGS;
import static org.apache.polaris.core.auth.PolarisAuthorizableOperation.LIST_PRINCIPALS;

import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.it.test.PolarisManagementServiceIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(Profiles.ManagementIntegrationProfile.class)
public class ManagementServiceExtAuthorizerIntegrationTest
    extends PolarisManagementServiceIntegrationTest {

  private static final ExampleNonRBACAuthorizer authorizer = new ExampleNonRBACAuthorizer();

  @BeforeAll
  static void installMocks() {
    QuarkusMock.installMockForType(authorizer, PolarisAuthorizer.class);
  }

  @AfterEach
  void resetAuth() {
    authorizer.reset();
  }

  @Override
  @Test
  public void testListPrincipalsUnauthorized() {
    authorizer.setDecision(LIST_PRINCIPALS, AuthorizationDecision.deny("test"));
    super.testListPrincipalsUnauthorized();
  }

  @Override
  @Test
  public void testListCatalogsUnauthorized() {
    authorizer.setDecision(LIST_CATALOGS, AuthorizationDecision.deny("test"));
    super.testListCatalogsUnauthorized();
  }

  @Override
  @Test
  @Disabled("Non-RBAC authorizers do not manage RBAC grants")
  public void testTableManageAccessCanGrantAndRevokeFromCatalogRoles() {}

  @Override
  @Test
  @Disabled("Non-RBAC authorizers do not manage RBAC grants")
  public void testServiceAdminCanTransferCatalogAdmin() {}

  @Override
  @Test
  @Disabled("Non-RBAC authorizers do not manage RBAC grants")
  public void testCatalogAdminGrantAndRevokeCatalogRolesFromWrongCatalog() {}

  @Override
  @Test
  @Disabled("Non-RBAC authorizers do not manage RBAC grants")
  public void testCreatePrincipalAndResetCredentialsWithCustomValues() {}

  @Override
  @Test
  @Disabled("Non-RBAC authorizers do not manage RBAC grants")
  public void testCreatePrincipalAndRotateCredentials() {}
}
