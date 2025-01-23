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

import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.persistence.BaseResult;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class BasePolarisAuthenticatorTest {

  private BasePolarisAuthenticator authenticator;
  private PolarisMetaStoreManager metaStoreManager;
  private PolarisMetaStoreSession metaStoreSession;

  @BeforeEach
  public void setUp() {
    metaStoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    metaStoreSession = Mockito.mock(PolarisMetaStoreSession.class);
    authenticator =
        new BasePolarisAuthenticator(metaStoreManager, metaStoreSession) {
          @Override
          public Optional<AuthenticatedPolarisPrincipal> authenticate(String credentials) {
            return Optional.empty();
          }
        };
  }

  @Test
  public void testFetchPrincipalThrowsServiceExceptionOnMetastoreException() {
    DecodedToken token = Mockito.mock(DecodedToken.class);
    long principalId = 100L;
    when(token.getPrincipalId()).thenReturn(principalId);
    when(metaStoreManager.loadEntity(metaStoreSession, 0L, principalId))
        .thenThrow(new RuntimeException("Metastore exception"));

    Assertions.assertThatThrownBy(() -> authenticator.getPrincipal(token))
        .isInstanceOf(ServiceFailureException.class)
        .hasMessage("Unable to fetch principal entity");
  }

  @Test
  public void testFetchPrincipalThrowsNotAuthorizedWhenNotFound() {
    DecodedToken token = Mockito.mock(DecodedToken.class);
    long principalId = 100L;
    when(token.getPrincipalId()).thenReturn(principalId);
    when(token.getClientId()).thenReturn("abc");
    when(metaStoreManager.loadEntity(metaStoreSession, 0L, principalId))
        .thenReturn(
            new PolarisMetaStoreManager.EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, ""));

    Assertions.assertThatThrownBy(() -> authenticator.getPrincipal(token))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("Unable to authenticate");
  }
}
