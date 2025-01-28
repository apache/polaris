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
package org.apache.polaris.service.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.context.RealmId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DefaultRealmIdResolverTest {

  private RealmContextConfiguration config;

  @BeforeEach
  void setUp() {
    config = Mockito.mock(RealmContextConfiguration.class);
    when(config.headerName()).thenReturn("Polaris-Header");
    when(config.realms()).thenReturn(List.of("realm1", "realm2"));
    when(config.defaultRealm()).thenCallRealMethod();
  }

  @Test
  void headerPresentSuccess() {
    DefaultRealmIdResolver resolver = new DefaultRealmIdResolver(config);
    RealmId realmId1 =
        resolver.resolveRealmId("requestURL", "method", "path", Map.of("Polaris-Header", "realm1"));
    assertThat(realmId1.id()).isEqualTo("realm1");
    RealmId realmId2 =
        resolver.resolveRealmId("requestURL", "method", "path", Map.of("Polaris-Header", "realm2"));
    assertThat(realmId2.id()).isEqualTo("realm2");
  }

  @Test
  void headerPresentFailure() {
    DefaultRealmIdResolver resolver = new DefaultRealmIdResolver(config);
    assertThatThrownBy(
            () ->
                resolver.resolveRealmId(
                    "requestURL", "method", "path", Map.of("Polaris-Header", "realm3")))
        .isInstanceOf(UnresolvableRealmException.class)
        .hasMessage("Unknown realm: realm3");
  }

  @Test
  void headerNotPresentSuccess() {
    when(config.requireHeader()).thenReturn(false);
    DefaultRealmIdResolver resolver = new DefaultRealmIdResolver(config);
    RealmId realmId1 = resolver.resolveRealmId("requestURL", "method", "path", Map.of());
    assertThat(realmId1.id()).isEqualTo("realm1");
  }

  @Test
  void headerNotPresentFailure() {
    when(config.requireHeader()).thenReturn(true);
    DefaultRealmIdResolver resolver = new DefaultRealmIdResolver(config);
    assertThatThrownBy(() -> resolver.resolveRealmId("requestURL", "method", "path", Map.of()))
        .isInstanceOf(UnresolvableRealmException.class)
        .hasMessage("Missing required realm header: Polaris-Header");
  }
}
