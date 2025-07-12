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
package org.apache.polaris.service.quarkus.catalog;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;

@QuarkusTest
@TestProfile(IcebergViewCatalogPersistenceInMemTest.Profile.class)
public class IcebergViewCatalogPersistenceInMemTest extends AbstractIcebergCatalogViewTest {
  @Override
  protected PrincipalSecretsResult bootstrapRealm(String realmName) {
    return metaStoreManagerFactory
        .bootstrapRealms(
            List.of(realmName),
            RootCredentialsSet.fromList(List.of(realmName + ",aClientId,aSecret")))
        .get(realmName);
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(DEFAULT_QUARKUS_CONFIGS)
          .put("polaris.persistence.type", "nosql")
          .put("polaris.persistence.backend.type", "InMemory")
          .build();
    }
  }
}
