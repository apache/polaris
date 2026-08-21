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
package org.apache.polaris.core.tag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.junit.jupiter.api.Test;

public class TagEntityTest {

  private static TagEntity.Builder builder(String name) {
    return new TagEntity.Builder(name)
        .setAllowedValues(List.of("public", "internal"))
        .setTargetTypes(List.of("catalog", "table-like"));
  }

  @Test
  public void testBuildRoundTrip() {
    TagEntity tag = builder("classification").setComment("data classification").build();

    assertThat(tag.getType()).isEqualTo(PolarisEntityType.TAG);
    assertThat(tag.getSubType()).isEqualTo(PolarisEntitySubType.NULL_SUBTYPE);
    assertThat(tag.getName()).isEqualTo("classification");
    assertThat(tag.getComment()).isEqualTo("data classification");
    assertThat(tag.getAllowedValues()).containsExactly("public", "internal");
    assertThat(tag.getTargetTypes()).containsExactly("catalog", "table-like");
  }

  @Test
  public void testVersionStartsAtZero() {
    assertThat(builder("t").build().getTagVersion()).isZero();
  }

  @Test
  public void testAllowedValuesOrderIsPreserved() {
    TagEntity tag =
        new TagEntity.Builder("t")
            .setAllowedValues(List.of("c", "a", "b"))
            .setTargetTypes(List.of("catalog"))
            .build();

    assertThat(tag.getAllowedValues()).containsExactly("c", "a", "b");
  }

  @Test
  public void testCopyBuilderBumpsVersionAndKeepsUntouchedFields() {
    TagEntity original = builder("classification").setComment("first").build();

    TagEntity updated =
        new TagEntity.Builder(original)
            .setAllowedValues(List.of("public"))
            .setTagVersion(original.getTagVersion() + 1)
            .build();

    assertThat(updated.getTagVersion()).isEqualTo(1);
    assertThat(updated.getAllowedValues()).containsExactly("public");
    assertThat(updated.getComment()).isEqualTo("first");
    assertThat(updated.getTargetTypes()).containsExactly("catalog", "table-like");
  }

  @Test
  public void testNullCommentLeavesExistingCommentUntouched() {
    TagEntity original = builder("t").setComment("kept").build();

    TagEntity updated = new TagEntity.Builder(original).setComment(null).build();

    assertThat(updated.getComment()).isEqualTo("kept");
  }

  @Test
  public void testAllowedValuesAreRequired() {
    assertThatThrownBy(() -> new TagEntity.Builder("t").setTargetTypes(List.of("catalog")).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Allowed values must be specified");
  }

  @Test
  public void testTargetTypesAreRequired() {
    assertThatThrownBy(() -> new TagEntity.Builder("t").setAllowedValues(List.of("public")).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Target types must be specified");
  }

  @Test
  public void testValidateAllowedValuesRejectsEmptyList() {
    assertThatThrownBy(() -> TagValidation.validateAllowedValues(List.of()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("at least one value");
  }

  @Test
  public void testValidateAllowedValuesRejectsEmptyMember() {
    assertThatThrownBy(() -> TagValidation.validateAllowedValues(List.of("public", "")))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("empty value");
  }

  @Test
  public void testValidateAllowedValuesRejectsDuplicates() {
    assertThatThrownBy(() -> TagValidation.validateAllowedValues(List.of("public", "public")))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("duplicates");
  }

  @Test
  public void testValidateAllowedValuesIsCaseSensitive() {
    TagValidation.validateAllowedValues(List.of("Public", "public"));
  }

  @Test
  public void testValidateTargetTypesRejectsEmptyListAndDuplicates() {
    assertThatThrownBy(() -> TagValidation.validateTargetTypes(List.of()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("at least one type");
    assertThatThrownBy(() -> TagValidation.validateTargetTypes(List.of("catalog", "catalog")))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("duplicates");
  }
}
