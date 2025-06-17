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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Instance;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.service.quarkus.auth.external.OidcTenantConfiguration;
import org.apache.polaris.service.quarkus.auth.external.OidcTenantConfiguration.PrincipalMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DefaultPrincipalMapperTest {

  private DefaultPrincipalMapper mapper;
  private ClaimsLocator claimsLocator;
  private SecurityIdentity identity;

  @BeforeEach
  public void setup() {
    claimsLocator = mock(ClaimsLocator.class);
    identity = mock(SecurityIdentity.class);
    PrincipalMapper principalMapping = mock(PrincipalMapper.class);
    when(principalMapping.idClaimPath()).thenReturn(Optional.of("id_path"));
    when(principalMapping.nameClaimPath()).thenReturn(Optional.of("name_path"));
    OidcTenantConfiguration config = mock(OidcTenantConfiguration.class);
    when(config.principalMapper()).thenReturn(principalMapping);
    when(identity.getAttribute(TENANT_CONFIG_ATTRIBUTE)).thenReturn(config);
    @SuppressWarnings("unchecked")
    Instance<PrincipalRolesMapper> strategies = mock(Instance.class);
    when(strategies.select(Identifier.Literal.of("default"))).thenReturn(strategies);
    when(strategies.get()).thenReturn(new DefaultPrincipalRolesMapper());
    mapper = new DefaultPrincipalMapper(claimsLocator);
  }

  @ParameterizedTest
  @MethodSource
  public void mapPrincipalId(Object claim, long expected) {
    when(claimsLocator.locateClaim(eq("id_path"), any())).thenReturn(claim);
    long result = mapper.mapPrincipalId(identity).orElse(-1);
    assertThat(result).isEqualTo(expected);
  }

  static Stream<Arguments> mapPrincipalId() {
    return Stream.of(Arguments.of(123L, 123L), Arguments.of("123", 123L), Arguments.of(null, -1));
  }

  @ParameterizedTest
  @MethodSource
  public void mapPrincipalName(Object claim, String expected) {
    when(claimsLocator.locateClaim(eq("name_path"), any())).thenReturn(claim);
    String result = mapper.mapPrincipalName(identity).orElse(null);
    assertThat(result).isEqualTo(expected);
  }

  static Stream<Arguments> mapPrincipalName() {
    return Stream.of(
        Arguments.of("testUser", "testUser"), Arguments.of(123, "123"), Arguments.of(null, null));
  }
}
