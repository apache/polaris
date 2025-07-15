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
package org.apache.polaris.service.it;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;

@QuarkusIntegrationTest
@TestProfile(value = RestCatalogPersistenceIT.Profile.class)
public class RestCatalogPersistenceIT extends PolarisRestCatalogMinIOIT {
  public static class Profile extends PersistenceTesting.PersistenceInMemoryProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.storage.aws.access-key", MINIO_ACCESS_KEY)
          .put("polaris.storage.aws.secret-key", MINIO_SECRET_KEY)
          .put("polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "false")
          .build();
    }
  }
}
