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
import java.util.stream.Stream;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.GrantResources;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.Profiles;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
@TestProfile(Profiles.PolarisAuthzBaseProfile.class)
class SemanticModelGrantAuthzTest extends PolarisAuthzTestBase {
  static Stream<GrantResource> scopedGrants() {
    return Stream.of(PolarisPrivilege.values())
        .filter(privilege -> privilege.name().startsWith("SEMANTIC_MODEL_"))
        .flatMap(
            privilege ->
                Stream.of(
                    new NamespaceGrant(
                        List.of(NS1.levels()),
                        NamespacePrivilege.valueOf(privilege.name()),
                        GrantResource.TypeEnum.NAMESPACE),
                    new CatalogGrant(
                        CatalogPrivilege.valueOf(privilege.name()),
                        GrantResource.TypeEnum.CATALOG)));
  }

  @ParameterizedTest
  @MethodSource("scopedGrants")
  void grantsRoundTripThroughManagementApi(GrantResource grant) throws Exception {
    var api =
        new PolarisServiceImpl(
            realmConfig, reservedProperties, newRootAdminService(), serviceIdentityProvider);
    ObjectMapper mapper = new ObjectMapper();
    AddGrantRequest request =
        mapper.readValue(
            mapper.writeValueAsString(new AddGrantRequest(grant)), AddGrantRequest.class);
    assertThat(request.getGrant()).isEqualTo(grant);
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
