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
package org.apache.polaris.core.persistence;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import java.time.Clock;
import org.apache.polaris.core.persistence.transactional.TransactionalMetaStoreManagerImpl;
import org.apache.polaris.core.persistence.transactional.TreeMapMetaStore;
import org.apache.polaris.core.persistence.transactional.TreeMapTransactionalPersistenceImpl;
import org.mockito.Mockito;

public class ResolverTest extends BaseResolverTest {

  private final Clock clock = Clock.systemUTC();
  private PolarisTestMetaStoreManager tm;
  private TransactionalMetaStoreManagerImpl metaStoreManager;

  @Override
  protected PolarisMetaStoreManager metaStoreManager() {
    if (metaStoreManager == null) {
      TreeMapMetaStore store = new TreeMapMetaStore(diagServices);
      TreeMapTransactionalPersistenceImpl metaStore =
          new TreeMapTransactionalPersistenceImpl(
              diagServices, store, Mockito.mock(), RANDOM_SECRETS);
      metaStoreManager =
          new TransactionalMetaStoreManagerImpl(
              clock, diagServices, realmContext, realmConfig, () -> metaStore);
    }
    return metaStoreManager;
  }

  @Override
  protected PolarisTestMetaStoreManager tm() {
    if (tm == null) {
      // bootstrap the meta store with our test schema
      tm = new PolarisTestMetaStoreManager(metaStoreManager());
    }
    return tm;
  }
}
