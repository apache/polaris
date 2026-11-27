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
package org.apache.polaris.service.config;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.service.context.RealmContextConfiguration;
import org.apache.polaris.service.persistence.PersistenceConfiguration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ServiceProducersBootstrapTest {

  private static final String REALM = "realm1";

  /**
   * A result that bootstrapped nothing carries no credentials, which happens for a realm that is
   * already set up and for one that could not be bootstrapped at all. With no credentials
   * configured for the realm, auto-bootstrap would otherwise print the ones it believes were
   * generated, so it must neither dereference them nor print anything to stdout.
   */
  @ParameterizedTest
  @CsvSource({"ENTITY_ALREADY_EXISTS,realm is already bootstrapped", "UNEXPECTED_ERROR_SIGNALED,"})
  public void testAutoBootstrapWithoutCredentialsInResult(
      BaseResult.ReturnStatus status, String reason) {
    var bootstrapper = mock(Bootstrapper.class);
    when(bootstrapper.bootstrapRealms(any(), any()))
        .thenReturn(Map.of(REALM, new PrincipalSecretsResult(status, reason)));

    var stdout = new ByteArrayOutputStream();
    var original = System.out;
    System.setOut(new PrintStream(stdout, true, UTF_8));
    try {
      assertThatCode(() ->
              new ServiceProducers()
                  .maybeBootstrap(
                      null,
                      bootstrapper,
                      persistenceConfiguration(),
                      realmContextConfiguration(),
                      RootCredentialsSet.EMPTY))
          .doesNotThrowAnyException();
    } finally {
      System.setOut(original);
    }

    assertThat(stdout.toString(UTF_8)).isEmpty();
  }

  private static PersistenceConfiguration persistenceConfiguration() {
    return new PersistenceConfiguration() {
      @Override
      public String type() {
        return "in-memory";
      }

      @Override
      public Set<String> autoBootstrapTypes() {
        return Set.of("in-memory");
      }
    };
  }

  private static RealmContextConfiguration realmContextConfiguration() {
    var config = mock(RealmContextConfiguration.class);
    when(config.realms()).thenReturn(List.of(REALM));
    return config;
  }
}
