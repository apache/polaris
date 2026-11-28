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
package org.apache.polaris.extensions.federation.hadoop;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.BearerAuthenticationParameters;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.HadoopConnectionConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ImplicitAuthenticationParameters;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.junit.jupiter.api.Test;

class HadoopCatalogValidatorTest {
  private final HadoopCatalogValidator validator = new HadoopCatalogValidator();

  @Test
  void implicitAuthenticationIsAccepted() {
    assertThatCode(() -> validator.validate(catalog(connection(implicit()))))
        .doesNotThrowAnyException();
  }

  @Test
  void otherAuthenticationIsRejected() {
    assertThatThrownBy(() -> validator.validate(catalog(connection(bearer()))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "HADOOP connections only support IMPLICIT authentication, but BEARER was requested");
  }

  @Test
  void otherConnectionTypesAreNotChecked() {
    ConnectionConfigInfo icebergRest =
        IcebergRestConnectionConfigInfo.builder(
                ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setUri("https://remote.example.com/api/catalog")
            .setRemoteCatalogName("remote")
            .setAuthenticationParameters(bearer())
            .build();

    assertThatCode(() -> validator.validate(catalog(icebergRest))).doesNotThrowAnyException();
  }

  private static ConnectionConfigInfo connection(AuthenticationParameters authentication) {
    return HadoopConnectionConfigInfo.builder(ConnectionConfigInfo.ConnectionTypeEnum.HADOOP)
        .setWarehouse("file:///tmp/warehouse")
        .setAuthenticationParameters(authentication)
        .build();
  }

  private static Catalog catalog(ConnectionConfigInfo connection) {
    return ExternalCatalog.builder()
        .setType(Catalog.TypeEnum.EXTERNAL)
        .setName("external")
        .setProperties(new CatalogProperties("file:///tmp/external"))
        .setStorageConfigInfo(
            FileStorageConfigInfo.builder()
                .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
                .setAllowedLocations(List.of("file:///tmp/"))
                .build())
        .setConnectionConfigInfo(connection)
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
