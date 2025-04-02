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

import static jakarta.ws.rs.core.Response.Status.CREATED;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.polaris.core.admin.model.*;
import org.apache.polaris.core.rest.PolarisEndpoint;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@QuarkusTest
@TestProfile(GetConfigTest.Profile.class)
public class GetConfigTest {
  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.features.defaults.\"ALLOW_SPECIFYING_FILE_IO_IMPL\"",
          "true",
          "polaris.features.defaults.\"INITIALIZE_DEFAULT_CATALOG_FILEIO_FOR_TEST\"",
          "true",
          "polaris.features.defaults.\"SUPPORTED_CATALOG_STORAGE_TYPES\"",
          "[\"FILE\"]");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetConfig(boolean enableGenericTable) {
    TestServices services =
        TestServices.builder().config(Map.of("ENABLE_GENERIC_TABLES", enableGenericTable)).build();

    FileStorageConfigInfo fileStorage =
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://"))
            .build();
    String catalogName = "test-catalog";
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("file:///tmp/path/to/data"))
            .setStorageConfigInfo(fileStorage)
            .build();

    Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext());
    assertThat(response.getStatus()).isEqualTo(CREATED.getStatusCode());

    response =
        services
            .restConfigurationApi()
            .getConfig(catalogName, services.realmContext(), services.securityContext());
    ConfigResponse configResponse = response.readEntity(ConfigResponse.class);
    assertThat(configResponse.overrides()).contains(Map.entry("prefix", catalogName));
    if (enableGenericTable) {
      assertThat(configResponse.endpoints()).contains(PolarisEndpoint.V1_CREATE_GENERIC_TABLE);
      assertThat(configResponse.endpoints()).contains(PolarisEndpoint.V1_DELETE_GENERIC_TABLE);
      assertThat(configResponse.endpoints()).contains(PolarisEndpoint.V1_LIST_GENERIC_TABLES);
      assertThat(configResponse.endpoints()).contains(PolarisEndpoint.V1_LOAD_GENERIC_TABLE);
    } else {
      assertThat(configResponse.endpoints())
          .doesNotContain(PolarisEndpoint.V1_CREATE_GENERIC_TABLE);
      assertThat(configResponse.endpoints())
          .doesNotContain(PolarisEndpoint.V1_DELETE_GENERIC_TABLE);
      assertThat(configResponse.endpoints()).doesNotContain(PolarisEndpoint.V1_LIST_GENERIC_TABLES);
      assertThat(configResponse.endpoints()).doesNotContain(PolarisEndpoint.V1_LOAD_GENERIC_TABLE);
    }
  }
}
