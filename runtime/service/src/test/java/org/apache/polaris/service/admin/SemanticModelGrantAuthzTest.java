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
package org.apache.polaris.service.admin;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.GrantResources;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.SemanticModelGrant;
import org.apache.polaris.core.admin.model.SemanticModelPrivilege;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.semantic.SemanticModelEntity;
import org.apache.polaris.service.Profiles;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@QuarkusTest
@TestProfile(Profiles.PolarisAuthzBaseProfile.class)
class SemanticModelGrantAuthzTest extends PolarisAuthzTestBase {
  private static final TableIdentifier MODEL = TableIdentifier.of(NS1, "model");

  @BeforeEach
  void createModel() {
    var namespace =
        metaStoreManager
            .readEntityByName(
                polarisContext,
                List.of(catalogEntity),
                PolarisEntityType.NAMESPACE,
                PolarisEntitySubType.NULL_SUBTYPE,
                NS1.level(0))
            .getEntity();
    var model =
        new SemanticModelEntity.Builder(NS1, MODEL.name())
            .setId(metaStoreManager.generateNewEntityId(polarisContext).getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .setCatalogId(catalogEntity.getId())
            .setParentId(namespace.getId())
            .setSpecVersion("0.1.1")
            .setContent("[]")
            .build();
    assertSuccess(
        metaStoreManager.createEntityIfNotExists(
            polarisContext, List.of(catalogEntity, namespace), model));
  }

  private PolarisAdminService caller() {
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            principalEntity.getName(),
            Map.of(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principalEntity),
            Set.of(PRINCIPAL_ROLE1));
    return new PolarisAdminService(
        callContext,
        resolutionManifestFactory,
        metaStoreManager,
        userSecretsManager,
        serviceIdentityProvider,
        principal,
        polarisAuthorizer,
        reservedProperties);
  }

  @TestFactory
  Stream<DynamicNode> grantRequiresManageGrants() {
    return authzTestsBuilder("grantPrivilegeOnSemanticModelToRole")
        .action(
            () ->
                caller()
                    .grantPrivilegeOnSemanticModelToRole(
                        CATALOG_NAME,
                        CATALOG_ROLE2,
                        MODEL,
                        PolarisPrivilege.SEMANTIC_MODEL_READ_PROPERTIES))
        .cleanupAction(
            () ->
                newRootAdminService()
                    .revokePrivilegeOnSemanticModelFromRole(
                        CATALOG_NAME,
                        CATALOG_ROLE2,
                        MODEL,
                        PolarisPrivilege.SEMANTIC_MODEL_READ_PROPERTIES))
        .shouldPassWith(PolarisPrivilege.SEMANTIC_MODEL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> revokeRequiresManageGrantsOnModelAndRole() {
    return authzTestsBuilder("revokePrivilegeOnSemanticModelFromRole")
        .action(
            () -> {
              assertSuccess(
                  newRootAdminService()
                      .grantPrivilegeOnSemanticModelToRole(
                          CATALOG_NAME,
                          CATALOG_ROLE2,
                          MODEL,
                          PolarisPrivilege.SEMANTIC_MODEL_READ_PROPERTIES));
              caller()
                  .revokePrivilegeOnSemanticModelFromRole(
                      CATALOG_NAME,
                      CATALOG_ROLE2,
                      MODEL,
                      PolarisPrivilege.SEMANTIC_MODEL_READ_PROPERTIES);
            })
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(
            PolarisPrivilege.SEMANTIC_MODEL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .createTests();
  }

  @ParameterizedTest
  @EnumSource(SemanticModelPrivilege.class)
  void grantsRoundTripThroughManagementApi(SemanticModelPrivilege privilege) throws Exception {
    var api =
        new PolarisServiceImpl(
            realmConfig, reservedProperties, newRootAdminService(), serviceIdentityProvider);
    var grant =
        new SemanticModelGrant(
            List.of(NS1.levels()), MODEL.name(), privilege, GrantResource.TypeEnum.SEMANTIC_MODEL);
    ObjectMapper mapper = new ObjectMapper();
    AddGrantRequest request =
        mapper.readValue(
            mapper.writeValueAsString(new AddGrantRequest(grant)), AddGrantRequest.class);
    assertThat(request.getGrant()).isInstanceOf(SemanticModelGrant.class);
    try (Response response =
        api.addGrantToCatalogRole(CATALOG_NAME, CATALOG_ROLE2, request, null, null)) {
      assertThat(response.getStatus()).isEqualTo(201);
    }
    try (Response response =
        api.listGrantsForCatalogRole(CATALOG_NAME, CATALOG_ROLE2, null, null)) {
      assertThat(((GrantResources) response.getEntity()).getGrants()).contains(grant);
    }
    try (Response response =
        api.revokeGrantFromCatalogRole(
            CATALOG_NAME, CATALOG_ROLE2, false, new RevokeGrantRequest(grant), null, null)) {
      assertThat(response.getStatus()).isEqualTo(201);
    }
    assertThat(newRootAdminService().listGrantsForCatalogRole(CATALOG_NAME, CATALOG_ROLE2))
        .isEmpty();
  }
}
