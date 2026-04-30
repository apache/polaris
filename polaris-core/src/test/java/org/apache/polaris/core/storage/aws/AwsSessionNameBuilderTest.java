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

import java.util.List;
import java.util.Optional;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.junit.jupiter.api.Test;

class AwsSessionNameBuilderTest {

  private static final String PRINCIPAL = "etl_writer";

  private static CredentialVendingContext ctx(
      String realm, String catalog, String namespace, String table) {
    return CredentialVendingContext.builder()
        .realm(Optional.ofNullable(realm))
        .catalogName(Optional.ofNullable(catalog))
        .namespace(Optional.ofNullable(namespace))
        .tableName(Optional.ofNullable(table))
        .build();
  }

  @Test
  void emptyFieldListReturnsLegacyDefault() {
    String result =
        AwsSessionNameBuilder.buildSessionName(
            PRINCIPAL, CredentialVendingContext.empty(), List.of());
    assertThat(result).isEqualTo("PolarisAwsCredentialsStorageIntegration");
  }

  @Test
  void singleFieldPrincipal() {
    String result =
        AwsSessionNameBuilder.buildSessionName(
            PRINCIPAL, CredentialVendingContext.empty(), List.of(SessionNameField.PRINCIPAL));
    assertThat(result).isEqualTo("p-" + PRINCIPAL);
    assertThat(result).hasSizeLessThanOrEqualTo(64);
  }

  @Test
  void realmCatalogTablePrincipalProducesStructuredName() {
    CredentialVendingContext context = ctx("acme", "hr_catalog", null, "employee");
    String result =
        AwsSessionNameBuilder.buildSessionName(
            "etl_writer",
            context,
            List.of(
                SessionNameField.REALM,
                SessionNameField.CATALOG,
                SessionNameField.TABLE,
                SessionNameField.PRINCIPAL));
    assertThat(result).startsWith("p-");
    assertThat(result).contains("acme");
    assertThat(result).contains("hr_catalog");
    assertThat(result).contains("employee");
    assertThat(result).contains("etl_writer");
    assertThat(result).hasSizeLessThanOrEqualTo(64);
  }

  @Test
  void resultAlwaysWithinAwsLimit() {
    String longRealm = "a".repeat(100);
    String longCatalog = "b".repeat(100);
    String longTable = "c".repeat(100);
    String longPrincipal = "d".repeat(100);
    CredentialVendingContext context = ctx(longRealm, longCatalog, null, longTable);

    String result =
        AwsSessionNameBuilder.buildSessionName(
            longPrincipal,
            context,
            List.of(
                SessionNameField.REALM,
                SessionNameField.CATALOG,
                SessionNameField.TABLE,
                SessionNameField.PRINCIPAL));

    assertThat(result).hasSizeLessThanOrEqualTo(64);
    assertThat(result).startsWith("p-");
  }

  @Test
  void proportionalAllocationGivesRemainderToLastField() {
    // 4 fields, budget = 64 - 2 (prefix) - 3 (separators) = 59
    // base = 59/4 = 14, remainder = 59%4 = 3 → last field gets 14+3=17 chars
    String long1 = "a".repeat(50);
    String long2 = "b".repeat(50);
    String long3 = "c".repeat(50);
    String long4 = "d".repeat(50);
    CredentialVendingContext context = ctx(long1, long2, long3, long4);

    String result =
        AwsSessionNameBuilder.buildSessionName(
            long4,
            context,
            List.of(
                SessionNameField.REALM,
                SessionNameField.CATALOG,
                SessionNameField.NAMESPACE,
                SessionNameField.PRINCIPAL));

    // "p-" + 14 a's + "-" + 14 b's + "-" + 14 c's + "-" + 17 d's = 2+14+1+14+1+14+1+17 = 64
    assertThat(result).hasSize(64);
    assertThat(result).startsWith("p-");
  }

  @Test
  void invalidCharactersAreSanitized() {
    CredentialVendingContext context = ctx("my realm", "my/catalog", null, null);
    String result =
        AwsSessionNameBuilder.buildSessionName(
            "user@domain",
            context,
            List.of(SessionNameField.REALM, SessionNameField.CATALOG, SessionNameField.PRINCIPAL));

    // spaces and slashes → underscores; @ is valid in session names
    assertThat(result).matches("[p][\\w+=,.@-]+");
    assertThat(result).hasSizeLessThanOrEqualTo(64);
    assertThat(result).doesNotContain(" ").doesNotContain("/");
  }

  @Test
  void missingContextFieldsProduceEmptyComponent() {
    // When context fields are absent, getValue returns ""
    // The sanitized empty string stays empty so the separator appears doubled
    CredentialVendingContext context = CredentialVendingContext.empty();
    String result =
        AwsSessionNameBuilder.buildSessionName(
            "user", context, List.of(SessionNameField.CATALOG, SessionNameField.PRINCIPAL));

    // catalog is empty → "p--user" (empty catalog segment)
    assertThat(result).startsWith("p-");
    assertThat(result).endsWith("user");
    assertThat(result).hasSizeLessThanOrEqualTo(64);
  }

  @Test
  void allFiveSupportedFieldsWithinLimit() {
    CredentialVendingContext context = ctx("realm1", "catalog1", "ns1", "tbl1");
    String result =
        AwsSessionNameBuilder.buildSessionName(
            "user1",
            context,
            List.of(
                SessionNameField.REALM,
                SessionNameField.CATALOG,
                SessionNameField.NAMESPACE,
                SessionNameField.TABLE,
                SessionNameField.PRINCIPAL));

    assertThat(result).startsWith("p-");
    assertThat(result).hasSizeLessThanOrEqualTo(64);
  }

  @Test
  void twoFieldsProportionalBudget() {
    // 2 fields: budget = 64 - 2 - 1 = 61, base = 30, remainder = 1 → last gets 31
    String long1 = "a".repeat(50);
    String long2 = "b".repeat(50);
    CredentialVendingContext context = ctx(long1, long2, null, null);

    String result =
        AwsSessionNameBuilder.buildSessionName(
            long2, context, List.of(SessionNameField.REALM, SessionNameField.PRINCIPAL));

    // "p-" + 30 a's + "-" + 31 b's = 2+30+1+31 = 64
    assertThat(result).hasSize(64);
  }
}
