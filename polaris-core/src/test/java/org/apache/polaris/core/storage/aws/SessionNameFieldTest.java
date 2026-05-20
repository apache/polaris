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
package org.apache.polaris.core.storage.aws;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class SessionNameFieldTest {

  private static final CredentialVendingContext FULL_CONTEXT =
      CredentialVendingContext.builder()
          .realm(Optional.of("my-realm"))
          .catalogName(Optional.of("my-catalog"))
          .namespace(Optional.of("db.schema"))
          .tableName(Optional.of("my_table"))
          .build();

  @ParameterizedTest
  @CsvSource({
    "realm,   my-realm",
    "catalog, my-catalog",
    "namespace, db.schema",
    "table,   my_table",
    "principal, test-principal"
  })
  void fromConfigNameResolvesCorrectField(String configName, String expectedValue) {
    Optional<SessionNameField> field = SessionNameField.fromConfigName(configName.trim());
    assertThat(field).isPresent();
    assertThat(field.get().getValue("test-principal", FULL_CONTEXT))
        .isEqualTo(expectedValue.trim());
  }

  @ParameterizedTest
  @ValueSource(strings = {"roles", "trace_id", "unknown", ""})
  void fromConfigNameReturnsEmptyForUnsupportedFields(String name) {
    assertThat(SessionNameField.fromConfigName(name)).isEmpty();
  }

  @Test
  void realmReturnsEmptyStringWhenAbsent() {
    assertThat(SessionNameField.REALM.getValue("p", CredentialVendingContext.empty())).isEmpty();
  }

  @Test
  void catalogReturnsEmptyStringWhenAbsent() {
    assertThat(SessionNameField.CATALOG.getValue("p", CredentialVendingContext.empty())).isEmpty();
  }

  @Test
  void namespaceReturnsEmptyStringWhenAbsent() {
    assertThat(SessionNameField.NAMESPACE.getValue("p", CredentialVendingContext.empty()))
        .isEmpty();
  }

  @Test
  void tableReturnsEmptyStringWhenAbsent() {
    assertThat(SessionNameField.TABLE.getValue("p", CredentialVendingContext.empty())).isEmpty();
  }

  @Test
  void principalAlwaysReturnsPrincipalName() {
    assertThat(SessionNameField.PRINCIPAL.getValue("alice", CredentialVendingContext.empty()))
        .isEqualTo("alice");
  }
}
