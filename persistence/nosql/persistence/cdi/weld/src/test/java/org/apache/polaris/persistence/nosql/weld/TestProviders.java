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
package org.apache.polaris.persistence.nosql.weld;

import static org.apache.polaris.persistence.nosql.api.Realms.SYSTEM_REALM_ID;

import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.RealmPersistenceFactory;
import org.apache.polaris.persistence.nosql.api.SystemPersistence;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.realms.api.RealmManagement;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({SoftAssertionsExtension.class})
@EnableWeld
public class TestProviders {
  @InjectSoftAssertions SoftAssertions soft;

  @WeldSetup WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  @Test
  public void checkProviders() {
    var backend = weld.select(Backend.class).get();
    soft.assertThat(backend.type()).isEqualTo("InMemory");

    var realmManagement = weld.select(RealmManagement.class).get();
    soft.assertThat(realmManagement.get("fooBar")).isEmpty();

    var systemPersistence = weld.select(Persistence.class, new SystemPersistence.Literal()).get();
    soft.assertThat(systemPersistence.realmId()).isEqualTo(SYSTEM_REALM_ID);

    var requestScopedRunner = weld.select(RequestScopedRunner.class).get();
    requestScopedRunner.runWithRequestContext(
        () -> {
          var builder1 = weld.select(RealmPersistenceFactory.class).get();
          var persistence1 = builder1.newBuilder().realmId("my-realm").build();
          soft.assertThat(persistence1.realmId()).isEqualTo("my-realm");

          var builder2 = weld.select(RealmPersistenceFactory.class).get();
          var persistence2 = builder2.newBuilder().realmId("other-realm").build();
          soft.assertThat(persistence2.realmId()).isEqualTo("other-realm");

          // Trigger IdGenerator "activation"
          persistence1.generateId();

          // Trigger IdGenerator "activation"
          persistence2.generateId();
        });
  }
}
