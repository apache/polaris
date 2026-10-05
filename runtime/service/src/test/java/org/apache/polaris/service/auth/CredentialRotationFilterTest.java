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

import static org.apache.polaris.service.auth.CredentialRotationFilter.ROTATE_CREDENTIALS;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.SecurityContext;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.Arrays;
import java.util.Set;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.service.admin.api.PolarisPrincipalsApi;
import org.junit.jupiter.api.Test;

class CredentialRotationFilterTest {

  private static final PolarisPrincipal MUST_ROTATE =
      PolarisPrincipal.of(
          "alice",
          ImmutableAttributeMap.builder()
              .put(
                  PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY,
                  new PrincipalEntity.Builder()
                      .setName("alice")
                      .setCredentialRotationRequiredState()
                      .build())
              .build(),
          Set.of("role"));

  private static final PolarisPrincipal REGULAR =
      PolarisPrincipal.of("bob", AttributeMap.EMPTY, Set.of("role"));

  @Test
  void rejectsOtherEndpoints() {
    assertThatThrownBy(() -> filter(true, MUST_ROTATE, listPrincipals()))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("must rotate credentials first");
  }

  @Test
  void allowsRotateCredentialsEndpoint() {
    assertThatCode(() -> filter(true, MUST_ROTATE, ROTATE_CREDENTIALS)).doesNotThrowAnyException();
  }

  @Test
  void allowsWhenEnforcementDisabled() {
    assertThatCode(() -> filter(false, MUST_ROTATE, listPrincipals())).doesNotThrowAnyException();
  }

  @Test
  void allowsPrincipalThatDoesNotNeedToRotate() {
    assertThatCode(() -> filter(true, REGULAR, listPrincipals())).doesNotThrowAnyException();
  }

  @Test
  void ignoresNonPolarisPrincipal() {
    ContainerRequestContext requestContext = requestContext(mock(Principal.class));
    assertThatCode(() -> newFilter(true).checkCredentialRotationRequired(requestContext, null))
        .doesNotThrowAnyException();
  }

  @Test
  void ignoresUnauthenticatedRequests() {
    ContainerRequestContext requestContext = requestContext(null);
    assertThatCode(() -> newFilter(true).checkCredentialRotationRequired(requestContext, null))
        .doesNotThrowAnyException();
  }

  @Test
  void doesNotInspectResourceWhenEnforcementDisabled() {
    ContainerRequestContext requestContext = requestContext(MUST_ROTATE);
    assertThatCode(() -> newFilter(false).checkCredentialRotationRequired(requestContext, null))
        .doesNotThrowAnyException();
  }

  private static CredentialRotationFilter newFilter(boolean enforce) {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(
            FeatureConfiguration.ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING))
        .thenReturn(enforce);
    CredentialRotationFilter filter = new CredentialRotationFilter();
    filter.realmConfig = realmConfig;
    return filter;
  }

  private static Method listPrincipals() {
    return Arrays.stream(PolarisPrincipalsApi.class.getMethods())
        .filter(m -> m.getName().equals("listPrincipals"))
        .findFirst()
        .orElseThrow();
  }

  private static void filter(boolean enforce, PolarisPrincipal principal, Method method) {
    ResourceInfo resourceInfo = mock(ResourceInfo.class);
    when(resourceInfo.getResourceClass()).thenAnswer(i -> PolarisPrincipalsApi.class);
    when(resourceInfo.getResourceMethod()).thenReturn(method);
    newFilter(enforce).checkCredentialRotationRequired(requestContext(principal), resourceInfo);
  }

  private static ContainerRequestContext requestContext(Principal principal) {
    SecurityContext securityContext = mock(SecurityContext.class);
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    when(requestContext.getSecurityContext()).thenReturn(securityContext);
    return requestContext;
  }
}
