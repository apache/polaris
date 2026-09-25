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
package org.apache.polaris.service.catalog.tag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.types.Types;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TargetType;
import org.junit.jupiter.api.Test;

class TagCatalogUtilsTest {

  @Test
  void resolveTopLevelFieldIdRejectsFieldIdZero() {
    Schema schema = new Schema(Types.NestedField.required(0, "id", Types.IntegerType.get()));

    assertThatThrownBy(() -> TagCatalogUtils.resolveTopLevelFieldId(schema, "id"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("id");
  }

  @Test
  void resolveTopLevelFieldIdResolvesPositiveFieldId() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    assertThat(TagCatalogUtils.resolveTopLevelFieldId(schema, "id")).isEqualTo(1);
  }

  // ----- targetFromQuery: the §2 combination table -----

  @Test
  void targetFromQueryCatalogTakesNoOtherParameter() {
    TagAttachmentTarget target =
        TagCatalogUtils.targetFromQuery(TargetType.CATALOG, null, null, null);

    assertThat(target.getType()).isEqualTo(TargetType.CATALOG);
    assertThat(target.getPath()).isNullOrEmpty();
  }

  @Test
  void targetFromQueryCatalogRejectsAnyOtherParameter() {
    assertThatThrownBy(
            () -> TagCatalogUtils.targetFromQuery(TargetType.CATALOG, "sales", null, null))
        .isInstanceOf(BadRequestException.class);
    assertThatThrownBy(() -> TagCatalogUtils.targetFromQuery(TargetType.CATALOG, null, "t1", null))
        .isInstanceOf(BadRequestException.class);
    assertThatThrownBy(() -> TagCatalogUtils.targetFromQuery(TargetType.CATALOG, null, null, "c1"))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void targetFromQueryNamespaceSplitsLevelsAndTakesNoTargetNameOrColumn() {
    TagAttachmentTarget target =
        TagCatalogUtils.targetFromQuery(TargetType.NAMESPACE, "sales\u001feu", null, null);

    assertThat(target.getType()).isEqualTo(TargetType.NAMESPACE);
    assertThat(target.getPath()).containsExactly("sales", "eu");
  }

  @Test
  void targetFromQueryNamespaceRequiresNamespace() {
    assertThatThrownBy(
            () -> TagCatalogUtils.targetFromQuery(TargetType.NAMESPACE, null, null, null))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void targetFromQueryNamespaceRejectsTargetNameOrColumn() {
    assertThatThrownBy(
            () -> TagCatalogUtils.targetFromQuery(TargetType.NAMESPACE, "sales", "t1", null))
        .isInstanceOf(BadRequestException.class);
    assertThatThrownBy(
            () -> TagCatalogUtils.targetFromQuery(TargetType.NAMESPACE, "sales", null, "c1"))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void targetFromQueryTableAppendsTargetNameAfterNamespaceLevels() {
    TagAttachmentTarget target =
        TagCatalogUtils.targetFromQuery(TargetType.TABLE, "sales\u001feu", "customers", null);

    assertThat(target.getType()).isEqualTo(TargetType.TABLE);
    assertThat(target.getPath()).containsExactly("sales", "eu", "customers");
  }

  @Test
  void targetFromQueryViewAppendsTargetNameAfterNamespaceLevelsTooAndRejectsColumn() {
    TagAttachmentTarget target =
        TagCatalogUtils.targetFromQuery(TargetType.VIEW, "sales", "top_customers", null);

    assertThat(target.getType()).isEqualTo(TargetType.VIEW);
    assertThat(target.getPath()).containsExactly("sales", "top_customers");
    assertThatThrownBy(
            () -> TagCatalogUtils.targetFromQuery(TargetType.VIEW, "sales", "top_customers", "c1"))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void targetFromQueryTableRequiresNamespaceAndTargetName() {
    assertThatThrownBy(() -> TagCatalogUtils.targetFromQuery(TargetType.TABLE, null, "t1", null))
        .isInstanceOf(BadRequestException.class);
    assertThatThrownBy(() -> TagCatalogUtils.targetFromQuery(TargetType.TABLE, "sales", null, null))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void targetFromQueryColumnCarriesTheColumnNameSeparatelyFromThePath() {
    TagAttachmentTarget target =
        TagCatalogUtils.targetFromQuery(TargetType.COLUMN, "sales\u001feu", "customers", "email");

    assertThat(target.getType()).isEqualTo(TargetType.COLUMN);
    assertThat(target.getPath()).containsExactly("sales", "eu", "customers");
    assertThat(target.getColumn()).isEqualTo(List.of("email"));
  }

  @Test
  void targetFromQueryColumnRequiresNamespaceTargetNameAndColumn() {
    assertThatThrownBy(() -> TagCatalogUtils.targetFromQuery(TargetType.COLUMN, null, "t1", "c1"))
        .isInstanceOf(BadRequestException.class);
    assertThatThrownBy(
            () -> TagCatalogUtils.targetFromQuery(TargetType.COLUMN, "sales", null, "c1"))
        .isInstanceOf(BadRequestException.class);
    assertThatThrownBy(
            () -> TagCatalogUtils.targetFromQuery(TargetType.COLUMN, "sales", "t1", null))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void targetFromQueryDoesNotTreatAPercentEncodedSeparatorAsALevelBoundary() {
    // The framework's own URI decode already turned %251F into the literal text "%1F" (a percent
    // sign followed by ordinary characters), not U+001F, before this method ever sees the value:
    // a double-encoded separator is one level, not two.
    TagAttachmentTarget target =
        TagCatalogUtils.targetFromQuery(TargetType.NAMESPACE, "sales%1Feu", null, null);

    assertThat(target.getPath()).containsExactly("sales%1Feu");
  }
}
