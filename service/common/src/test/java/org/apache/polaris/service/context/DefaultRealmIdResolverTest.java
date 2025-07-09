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
import org.apache.polaris.core.context.RealmContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DefaultRealmContextResolverTest {

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
    DefaultRealmContextResolver resolver = new DefaultRealmContextResolver(config);
    RealmContext RealmContext1 =
        resolver
            .resolveRealmContext("requestURL", "method", "path", Map.of("Polaris-Header", "realm1"))
            .toCompletableFuture()
            .join();
    assertThat(RealmContext1.getRealmIdentifier()).isEqualTo("realm1");
    RealmContext RealmContext2 =
        resolver
            .resolveRealmContext("requestURL", "method", "path", Map.of("Polaris-Header", "realm2"))
            .toCompletableFuture()
            .join();
    assertThat(RealmContext2.getRealmIdentifier()).isEqualTo("realm2");
  }

  @Test
  void headerPresentFailure() {
    DefaultRealmContextResolver resolver = new DefaultRealmContextResolver(config);
    assertThatThrownBy(
            () ->
                resolver
                    .resolveRealmContext(
                        "requestURL", "method", "path", Map.of("Polaris-Header", "realm3"))
                    .toCompletableFuture()
                    .join())
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown realm: realm3");
  }

  @Test
  void headerNotPresentSuccess() {
    when(config.requireHeader()).thenReturn(false);
    DefaultRealmContextResolver resolver = new DefaultRealmContextResolver(config);
    RealmContext RealmContext1 =
        resolver
            .resolveRealmContext("requestURL", "method", "path", Map.of())
            .toCompletableFuture()
            .join();
    assertThat(RealmContext1.getRealmIdentifier()).isEqualTo("realm1");
  }

  @Test
  void headerNotPresentFailure() {
    when(config.requireHeader()).thenReturn(true);
    DefaultRealmContextResolver resolver = new DefaultRealmContextResolver(config);
    assertThatThrownBy(
            () ->
                resolver
                    .resolveRealmContext("requestURL", "method", "path", Map.of())
                    .toCompletableFuture()
                    .join())
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required realm header: Polaris-Header");
  }

  @Test
  void headerCaseInsensitive() {
    DefaultRealmContextResolver resolver = new DefaultRealmContextResolver(config);
    RealmContext RealmContext1 =
        resolver
            .resolveRealmContext("requestURL", "method", "path", Map.of("POLARIS-HEADER", "realm1"))
            .toCompletableFuture()
            .join();
    assertThat(RealmContext1.getRealmIdentifier()).isEqualTo("realm1");
    RealmContext RealmContext2 =
        resolver
            .resolveRealmContext("requestURL", "method", "path", Map.of("polaris-header", "realm2"))
            .toCompletableFuture()
            .join();
    assertThat(RealmContext2.getRealmIdentifier()).isEqualTo("realm2");
  }
}
