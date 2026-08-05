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

import static jakarta.ws.rs.core.Response.Status.CREATED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;

import jakarta.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.rest.GenericTableEndpoints;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.policy.PolicyEndpoints;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class GetConfigTest {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetConfig(boolean enableGenericTable) {
    ConfigResponse configResponse = getConfig(Map.of("ENABLE_GENERIC_TABLES", enableGenericTable));

    assertThat(configResponse.endpoints()).contains(PolicyEndpoints.V1_CREATE_POLICY);
    assertEndpointOrder(configResponse, enableGenericTable);
    assertGenericTableEndpoints(configResponse, enableGenericTable);
  }

  @Test
  public void testIdempotencyKeyLifetimeAdvertisedWhenEnabled() {
    ConfigResponse configResponse =
        getConfig(Map.of("polaris.idempotency.enabled", true, "polaris.idempotency.ttl", "PT30M"));
    assertThat(configResponse.idempotencyKeyLifetime()).isEqualTo("PT30M");
  }

  @Test
  public void testIdempotencyKeyLifetimeAbsentWhenDisabled() {
    ConfigResponse configResponse = getConfig(Map.of("polaris.idempotency.enabled", false));
    assertThat(configResponse.idempotencyKeyLifetime()).isNull();
  }

  /**
   * A mock authorizer that resolves inputs but denies the given operations with a {@link
   * ForbiddenException} on the legacy {@code authorizeOrThrow} path used by the handler.
   */
  private static PolarisAuthorizer authorizerDenying(PolarisAuthorizableOperation... deniedOps) {
    PolarisAuthorizer authorizer = Mockito.mock(PolarisAuthorizer.class);
    Mockito.doAnswer(
            invocation -> {
              AuthorizationState authzState = invocation.getArgument(0);
              authzState.getResolutionManifest().resolveAll();
              return null;
            })
        .when(authorizer)
        .resolveAuthorizationInputs(any(), any());
    Mockito.doAnswer(
            invocation -> {
              PolarisAuthorizableOperation op = invocation.getArgument(2);
              for (PolarisAuthorizableOperation denied : deniedOps) {
                if (op == denied) {
                  throw new ForbiddenException("test: operation %s denied", op);
                }
              }
              return null;
            })
        .when(authorizer)
        .authorizeOrThrow(
            any(),
            any(),
            any(PolarisAuthorizableOperation.class),
            any(PolarisResolvedPathWrapper.class),
            isNull());
    return authorizer;
  }

  @Test
  public void testCatalogPropertiesReturnedWhenAuthorized() {
    TestServices services = TestServices.builder().config(baseConfig(Map.of())).build();
    String catalogName = createCatalog(services);

    Response response =
        services
            .restConfigurationApi()
            .getConfig(catalogName, services.realmContext(), services.securityContext());
    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    ConfigResponse configResponse = response.readEntity(ConfigResponse.class);

    assertThat(configResponse.defaults()).containsEntry("custom-property", "custom-value");
  }

  @Test
  public void testCatalogPropertiesHiddenWithoutPropertiesPrivilege() {
    TestServices services =
        TestServices.builder()
            .config(baseConfig(Map.of()))
            .authorizer(
                authorizerDenying(PolarisAuthorizableOperation.GET_CATALOG_CONFIG_PROPERTIES))
            .build();
    String catalogName = createCatalog(services);

    Response response =
        services
            .restConfigurationApi()
            .getConfig(catalogName, services.realmContext(), services.securityContext());
    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    ConfigResponse configResponse = response.readEntity(ConfigResponse.class);

    // Catalog properties must not be disclosed, but the response still carries the prefix and
    // endpoints so that REST clients can initialize against the catalog.
    assertThat(configResponse.defaults()).isEmpty();
    assertThat(configResponse.overrides()).contains(Map.entry("prefix", catalogName));
    assertThat(configResponse.endpoints()).isNotEmpty();
  }

  @Test
  public void testConfigEndpointForbiddenWithoutConfigPrivilege() {
    TestServices services =
        TestServices.builder()
            .config(baseConfig(Map.of()))
            .authorizer(authorizerDenying(PolarisAuthorizableOperation.GET_CATALOG_CONFIG))
            .build();
    String catalogName = createCatalog(services);

    assertThatThrownBy(
            () ->
                services
                    .restConfigurationApi()
                    .getConfig(catalogName, services.realmContext(), services.securityContext()))
        .isInstanceOf(ForbiddenException.class);
  }

  private static ConfigResponse getConfig(Map<String, Object> extraConfig) {
    TestServices services = TestServices.builder().config(baseConfig(extraConfig)).build();
    String catalogName = createCatalog(services);

    Response response =
        services
            .restConfigurationApi()
            .getConfig(catalogName, services.realmContext(), services.securityContext());
    ConfigResponse configResponse = response.readEntity(ConfigResponse.class);

    assertThat(configResponse.overrides()).contains(Map.entry("prefix", catalogName));
    return configResponse;
  }

  private static Map<String, Object> baseConfig(Map<String, Object> extraConfig) {
    Map<String, Object> config = new HashMap<>();
    config.put("ALLOW_INSECURE_STORAGE_TYPES", true);
    config.put("SUPPORTED_CATALOG_STORAGE_TYPES", List.of("FILE", "S3"));
    config.putAll(extraConfig);
    return config;
  }

  private static String createCatalog(TestServices services) {
    FileStorageConfigInfo fileStorage =
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://"))
            .build();
    String catalogName = "test-catalog-" + UUID.randomUUID();
    CatalogProperties catalogProperties = new CatalogProperties("file:///tmp/path/to/data");
    catalogProperties.put("custom-property", "custom-value");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(catalogProperties)
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
    return catalogName;
  }

  private static void assertEndpointOrder(
      ConfigResponse configResponse, boolean enableGenericTable) {
    if (enableGenericTable) {
      assertThat(configResponse.endpoints())
          .containsSubsequence(
              Endpoint.V1_LIST_NAMESPACES,
              Endpoint.V1_REGISTER_VIEW,
              GenericTableEndpoints.V1_CREATE_GENERIC_TABLE,
              PolicyEndpoints.V1_CREATE_POLICY);
    } else {
      assertThat(configResponse.endpoints())
          .containsSubsequence(
              Endpoint.V1_LIST_NAMESPACES,
              Endpoint.V1_REGISTER_VIEW,
              PolicyEndpoints.V1_CREATE_POLICY);
    }
  }

  private static void assertGenericTableEndpoints(
      ConfigResponse configResponse, boolean enableGenericTable) {
    if (enableGenericTable) {
      assertThat(configResponse.endpoints())
          .contains(GenericTableEndpoints.V1_CREATE_GENERIC_TABLE);
      assertThat(configResponse.endpoints())
          .contains(GenericTableEndpoints.V1_DELETE_GENERIC_TABLE);
      assertThat(configResponse.endpoints()).contains(GenericTableEndpoints.V1_LIST_GENERIC_TABLES);
      assertThat(configResponse.endpoints()).contains(GenericTableEndpoints.V1_LOAD_GENERIC_TABLE);
    } else {
      assertThat(configResponse.endpoints())
          .doesNotContain(GenericTableEndpoints.V1_CREATE_GENERIC_TABLE);
      assertThat(configResponse.endpoints())
          .doesNotContain(GenericTableEndpoints.V1_DELETE_GENERIC_TABLE);
      assertThat(configResponse.endpoints())
          .doesNotContain(GenericTableEndpoints.V1_LIST_GENERIC_TABLES);
      assertThat(configResponse.endpoints())
          .doesNotContain(GenericTableEndpoints.V1_LOAD_GENERIC_TABLE);
    }
  }
}
