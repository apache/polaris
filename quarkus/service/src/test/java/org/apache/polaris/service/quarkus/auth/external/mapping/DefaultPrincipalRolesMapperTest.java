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
package org.apache.polaris.service.quarkus.auth.external.mapping;

import static org.apache.polaris.service.quarkus.auth.external.OidcTenantResolvingAugmentor.TENANT_CONFIG_ATTRIBUTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.security.identity.SecurityIdentity;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.service.quarkus.auth.external.OidcTenantConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DefaultPrincipalRolesMapperTest {

  private DefaultPrincipalRolesMapper mapper;
  private SecurityIdentity identity;

  @BeforeEach
  public void setup() {
    identity = mock(SecurityIdentity.class);
    OidcTenantConfiguration.PrincipalRolesMapper rolesMapper =
        mock(OidcTenantConfiguration.PrincipalRolesMapper.class);
    when(rolesMapper.filter()).thenReturn(Optional.of("POLARIS_ROLE:.*"));
    OidcTenantConfiguration.PrincipalRolesMapper.RegexMapping mapping =
        mock(OidcTenantConfiguration.PrincipalRolesMapper.RegexMapping.class);
    when(mapping.regex()).thenReturn("POLARIS_ROLE:(.*)");
    when(mapping.replacement()).thenReturn("PRINCIPAL_ROLE:$1");
    when(mapping.replace(any())).thenCallRealMethod();
    when(rolesMapper.mappings()).thenReturn(List.of(mapping));
    when(rolesMapper.filterPredicate()).thenCallRealMethod();
    when(rolesMapper.mapperFunction()).thenCallRealMethod();
    OidcTenantConfiguration config = mock(OidcTenantConfiguration.class);
    when(config.principalRolesMapper()).thenReturn(rolesMapper);
    when(identity.getAttribute(TENANT_CONFIG_ATTRIBUTE)).thenReturn(config);
    mapper = new DefaultPrincipalRolesMapper();
  }

  @ParameterizedTest
  @MethodSource
  public void mapPrincipalRoles(Set<String> input, Set<String> expected) {
    when(identity.getRoles()).thenReturn(input);
    Set<String> actual = mapper.mapPrincipalRoles(identity);
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> mapPrincipalRoles() {
    return Stream.of(
        Arguments.of(Set.of(), Set.of()),
        Arguments.of(Set.of("POLARIS_ROLE:role1"), Set.of("PRINCIPAL_ROLE:role1")),
        Arguments.of(
            Set.of("POLARIS_ROLE:role1", "POLARIS_ROLE:role2"),
            Set.of("PRINCIPAL_ROLE:role1", "PRINCIPAL_ROLE:role2")),
        Arguments.of(
            Set.of("POLARIS_ROLE:role1", "OTHER_ROLE:role2"), Set.of("PRINCIPAL_ROLE:role1")));
  }
}
