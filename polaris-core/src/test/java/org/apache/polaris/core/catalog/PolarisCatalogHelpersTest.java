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
package org.apache.polaris.core.catalog;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.BearerAuthenticationParameters;
import org.apache.polaris.core.admin.model.BigQueryMetastoreConnectionConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo.ConnectionTypeEnum;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.HadoopConnectionConfigInfo;
import org.apache.polaris.core.admin.model.HiveConnectionConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ImplicitAuthenticationParameters;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class PolarisCatalogHelpersTest {

  @ParameterizedTest
  @EnumSource(
      value = ConnectionTypeEnum.class,
      names = {"HADOOP", "HIVE", "BIGQUERY"})
  void requireImplicitAuthenticationAcceptsImplicit(ConnectionTypeEnum connectionType) {
    Catalog catalog = externalCatalog(connection(connectionType, implicit()));

    assertThatCode(
            () -> PolarisCatalogHelpers.requireImplicitAuthentication(catalog, connectionType))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @EnumSource(
      value = ConnectionTypeEnum.class,
      names = {"HADOOP", "HIVE", "BIGQUERY"})
  void requireImplicitAuthenticationRejectsOtherAuthentication(ConnectionTypeEnum connectionType) {
    Catalog catalog = externalCatalog(connection(connectionType, bearer()));

    assertThatThrownBy(
            () -> PolarisCatalogHelpers.requireImplicitAuthentication(catalog, connectionType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            connectionType.name()
                + " connections only support IMPLICIT authentication, but BEARER was requested");
  }

  @Test
  void requireImplicitAuthenticationLeavesMissingAuthenticationToRequestValidation() {
    Catalog catalog = externalCatalog(connection(ConnectionTypeEnum.HADOOP, null));

    assertThatCode(() ->
            PolarisCatalogHelpers.requireImplicitAuthentication(catalog, ConnectionTypeEnum.HADOOP))
        .doesNotThrowAnyException();
  }

  @Test
  void requireImplicitAuthenticationIgnoresOtherConnectionTypes() {
    Catalog hive = externalCatalog(connection(ConnectionTypeEnum.HIVE, bearer()));
    Catalog icebergRest =
        externalCatalog(
            IcebergRestConnectionConfigInfo.builder(ConnectionTypeEnum.ICEBERG_REST)
                .setUri("https://remote.example.com/api/catalog")
                .setRemoteCatalogName("remote")
                .setAuthenticationParameters(bearer())
                .build());

    assertThatCode(() ->
            PolarisCatalogHelpers.requireImplicitAuthentication(hive, ConnectionTypeEnum.HADOOP))
        .doesNotThrowAnyException();
    assertThatCode(() ->
            PolarisCatalogHelpers.requireImplicitAuthentication(
                icebergRest, ConnectionTypeEnum.HADOOP))
        .doesNotThrowAnyException();
  }

  @Test
  void requireImplicitAuthenticationIgnoresCatalogsWithoutConnection() {
    Catalog internal =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("internal")
            .setProperties(new CatalogProperties("file:///tmp/internal"))
            .setStorageConfigInfo(fileStorage())
            .build();

    assertThatCode(() ->
            PolarisCatalogHelpers.requireImplicitAuthentication(
                internal, ConnectionTypeEnum.HADOOP))
        .doesNotThrowAnyException();
    assertThatCode(() ->
            PolarisCatalogHelpers.requireImplicitAuthentication(
                externalCatalog(null), ConnectionTypeEnum.HADOOP))
        .doesNotThrowAnyException();
  }

  private static ConnectionConfigInfo connection(
      ConnectionTypeEnum connectionType, AuthenticationParameters authenticationParameters) {
    return switch (connectionType) {
      case HADOOP ->
          HadoopConnectionConfigInfo.builder(connectionType)
              .setWarehouse("file:///tmp/warehouse")
              .setAuthenticationParameters(authenticationParameters)
              .build();
      case HIVE ->
          HiveConnectionConfigInfo.builder(connectionType)
              .setUri("thrift://hms:9083")
              .setWarehouse("file:///tmp/warehouse")
              .setAuthenticationParameters(authenticationParameters)
              .build();
      case BIGQUERY ->
          BigQueryMetastoreConnectionConfigInfo.builder(connectionType)
              .setProperties(Map.of("gcp.bigquery.project-id", "p", "warehouse", "w"))
              .setAuthenticationParameters(authenticationParameters)
              .build();
      default -> throw new IllegalArgumentException(connectionType.name());
    };
  }

  private static Catalog externalCatalog(ConnectionConfigInfo connection) {
    return ExternalCatalog.builder()
        .setType(Catalog.TypeEnum.EXTERNAL)
        .setName("external")
        .setProperties(new CatalogProperties("file:///tmp/external"))
        .setStorageConfigInfo(fileStorage())
        .setConnectionConfigInfo(connection)
        .build();
  }

  private static StorageConfigInfo fileStorage() {
    return FileStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
        .setAllowedLocations(List.of("file:///tmp/"))
        .build();
  }

  private static AuthenticationParameters implicit() {
    return ImplicitAuthenticationParameters.builder()
        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.IMPLICIT)
        .build();
  }

  private static AuthenticationParameters bearer() {
    return BearerAuthenticationParameters.builder()
        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.BEARER)
        .setBearerToken("some-token")
        .build();
  }
}
