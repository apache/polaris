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
package org.apache.polaris.service.spark.it;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import org.apache.polaris.service.it.test.CatalogFederationIntegrationTest;

@TestProfile(CatalogFederationIT.CatalogFederationProfile.class)
@QuarkusIntegrationTest
public class CatalogFederationIT extends CatalogFederationIntegrationTest {

  public static class CatalogFederationProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.features.\"ENABLE_CATALOG_FEDERATION\"", "true")
          .put("polaris.features.\"SUPPORTED_CATALOG_CONNECTION_TYPES\"", "[\"ICEBERG_REST\"]")
          .put("polaris.features.\"ALLOW_OVERLAPPING_CATALOG_URLS\"", "true")
          .put("polaris.features.\"ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS\"", "true")
          .put("polaris.features.\"ALLOW_DROPPING_NON_EMPTY_PASSTHROUGH_FACADE_CATALOG\"", "true")
          .put("polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "false")
          .put("polaris.features.\"ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING\"", "true")
          .put("polaris.storage.aws.access-key", CatalogFederationIntegrationTest.MINIO_ACCESS_KEY)
          .put("polaris.storage.aws.secret-key", CatalogFederationIntegrationTest.MINIO_SECRET_KEY)
          .build();
    }
  }
}
