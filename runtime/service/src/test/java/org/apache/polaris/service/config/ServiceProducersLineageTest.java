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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.extensions.lineage.LineageConfiguration;
import org.apache.polaris.extensions.lineage.LineageStoreManager;
import org.apache.polaris.extensions.lineage.NoopLineageStoreManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ServiceProducersLineageTest {
  private final ServiceProducers serviceProducers = new ServiceProducers();
  private final RealmContext realmContext = mock(RealmContext.class);
  private final MetaStoreManagerFactory metaStoreManagerFactory =
      mock(MetaStoreManagerFactory.class);
  private final LineageConfiguration lineageConfiguration = mock(LineageConfiguration.class);
  private final LineageConfiguration.PersistenceConfiguration persistenceConfiguration =
      mock(LineageConfiguration.PersistenceConfiguration.class);

  @BeforeEach
  void setUp() {
    when(lineageConfiguration.persistence()).thenReturn(persistenceConfiguration);
  }

  @Test
  void returnsNoopStoreManagerWhenLineageStoreDisabled() {
    when(persistenceConfiguration.enabled()).thenReturn(false);

    LineageStoreManager storeManager =
        serviceProducers.lineageStoreManager(
            realmContext, metaStoreManagerFactory, lineageConfiguration);

    assertThat(storeManager).isInstanceOf(NoopLineageStoreManager.class);
    verifyNoInteractions(metaStoreManagerFactory);
  }

  @Test
  void reusesMetastoreSessionWhenItImplementsLineageStoreManager() {
    LineageStoreSession session = mock(LineageStoreSession.class);
    when(persistenceConfiguration.enabled()).thenReturn(true);
    when(persistenceConfiguration.type()).thenReturn("relational-jdbc");
    when(metaStoreManagerFactory.getOrCreateSession(realmContext)).thenReturn(session);

    LineageStoreManager storeManager =
        serviceProducers.lineageStoreManager(
            realmContext, metaStoreManagerFactory, lineageConfiguration);

    assertThat(storeManager).isSameAs(session);
    verify(metaStoreManagerFactory).getOrCreateSession(realmContext);
  }

  @Test
  void throwsWhenLineageStoreTypeUnsupported() {
    when(persistenceConfiguration.enabled()).thenReturn(true);
    when(persistenceConfiguration.type()).thenReturn("custom");

    assertThatThrownBy(
            () ->
                serviceProducers.lineageStoreManager(
                    realmContext, metaStoreManagerFactory, lineageConfiguration))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Unsupported lineage persistence type: custom");

    verifyNoInteractions(metaStoreManagerFactory);
  }

  @Test
  void throwsWhenConfiguredSessionDoesNotImplementLineageStoreManager() {
    BasePersistence session = mock(BasePersistence.class);
    when(persistenceConfiguration.enabled()).thenReturn(true);
    when(persistenceConfiguration.type()).thenReturn("relational-jdbc");
    when(metaStoreManagerFactory.getOrCreateSession(realmContext)).thenReturn(session);

    assertThatThrownBy(
            () ->
                serviceProducers.lineageStoreManager(
                    realmContext, metaStoreManagerFactory, lineageConfiguration))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(LineageStoreManager.class.getSimpleName());
  }

  private interface LineageStoreSession extends BasePersistence, LineageStoreManager {}
}
