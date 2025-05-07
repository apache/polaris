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

import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DefaultAuthenticatorTest {

  private DefaultAuthenticator authenticator;
  private PolarisMetaStoreManager metaStoreManager;
  private PolarisCallContext polarisCallContext;

  @BeforeEach
  public void setUp() {
    RealmContext realmContext = () -> "test";
    polarisCallContext = Mockito.mock(PolarisCallContext.class);
    CallContext callContext = CallContext.of(realmContext, polarisCallContext);
    metaStoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    MetaStoreManagerFactory metaStoreManagerFactory = Mockito.mock(MetaStoreManagerFactory.class);
    when(metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext))
        .thenReturn(metaStoreManager);
    authenticator = new DefaultAuthenticator();
    authenticator.metaStoreManagerFactory = metaStoreManagerFactory;
    authenticator.callContext = callContext;
  }

  @Test
  public void testFetchPrincipalThrowsServiceExceptionOnMetastoreException() {
    DecodedToken token = Mockito.mock(DecodedToken.class);
    long principalId = 100L;
    when(token.getPrincipalId()).thenReturn(principalId);
    when(metaStoreManager.loadEntity(
            polarisCallContext, 0L, principalId, PolarisEntityType.PRINCIPAL))
        .thenThrow(new RuntimeException("Metastore exception"));

    Assertions.assertThatThrownBy(() -> authenticator.authenticate(token))
        .isInstanceOf(ServiceFailureException.class)
        .hasMessage("Unable to fetch principal entity");
  }

  @Test
  public void testFetchPrincipalThrowsNotAuthorizedWhenNotFound() {
    DecodedToken token = Mockito.mock(DecodedToken.class);
    long principalId = 100L;
    when(token.getPrincipalId()).thenReturn(principalId);
    when(token.getClientId()).thenReturn("abc");
    when(metaStoreManager.loadEntity(
            polarisCallContext, 0L, principalId, PolarisEntityType.PRINCIPAL))
        .thenReturn(new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, ""));

    Assertions.assertThatThrownBy(() -> authenticator.authenticate(token))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("Unable to authenticate");
  }
}
