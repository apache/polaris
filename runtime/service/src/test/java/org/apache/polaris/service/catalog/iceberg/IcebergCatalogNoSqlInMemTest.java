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
package org.apache.polaris.service.catalog.iceberg;

import static org.apache.polaris.service.catalog.Profiles.NOSQL_IN_MEM;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;

@QuarkusTest
@TestProfile(IcebergCatalogNoSqlInMemTest.Profile.class)
public class IcebergCatalogNoSqlInMemTest extends AbstractIcebergCatalogTest {

  public static class Profile extends AbstractIcebergCatalogTest.Profile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .putAll(NOSQL_IN_MEM)
          .build();
    }
  }

  @Override
  protected void bootstrapRealm(String realmName) {
    metaStoreManagerFactory
        .bootstrapRealms(
            List.of(realmName),
            RootCredentialsSet.fromList(List.of(realmName + ",aClientId,aSecret")))
        .get(realmName);
  }

  @Override
  protected EntityCache createEntityCache(
      PolarisDiagnostics diagnostics,
      RealmConfig realmConfig,
      PolarisMetaStoreManager metaStoreManager) {
    return null;
  }
}
