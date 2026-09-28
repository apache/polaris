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
        .setValues(List.of("public", "internal"))
        .setTargetTypes(List.of("CATALOG", "TABLE"));
  }

  @Test
  public void testBuildRoundTrip() {
    TagEntity tag = builder("classification").setDescription("data classification").build();

    assertThat(tag.getType()).isEqualTo(PolarisEntityType.TAG);
    assertThat(tag.getSubType()).isEqualTo(PolarisEntitySubType.NULL_SUBTYPE);
    assertThat(tag.getName()).isEqualTo("classification");
    assertThat(tag.getDescription()).isEqualTo("data classification");
    assertThat(tag.getValues()).containsExactly("public", "internal");
    assertThat(tag.getTargetTypes()).containsExactly("CATALOG", "TABLE");
  }

  @Test
  public void testValuesOrderIsPreserved() {
    TagEntity tag =
        new TagEntity.Builder("t")
            .setValues(List.of("c", "a", "b"))
            .setTargetTypes(List.of("CATALOG"))
            .build();

    assertThat(tag.getValues()).containsExactly("c", "a", "b");
  }

  @Test
  public void testCopyBuilderKeepsUntouchedFields() {
    TagEntity original = builder("classification").setDescription("first").build();

    TagEntity updated = new TagEntity.Builder(original).setValues(List.of("public")).build();

    assertThat(updated.getValues()).containsExactly("public");
    assertThat(updated.getDescription()).isEqualTo("first");
    assertThat(updated.getTargetTypes()).containsExactly("CATALOG", "TABLE");
  }

  @Test
  public void testNullDescriptionClearsIt() {
    TagEntity original = builder("t").setDescription("kept").build();

    TagEntity updated = new TagEntity.Builder(original).setDescription(null).build();

    // An update states the whole editable definition, so a null description asks for no description
    // rather than for the one already stored.
    assertThat(updated.getDescription()).isNull();
  }

  @Test
  public void testTheEntityStoresWhateverDescriptionItIsGiven() {
    TagEntity original = builder("t").setDescription("kept").build();

    TagEntity updated = new TagEntity.Builder(original).setDescription("").build();

    // The entity holds what it is handed and draws no conclusions: an empty string stays an empty
    // string here. Which strings mean "no description", and what gets stored for them, is
    // TagCatalog's decision, not this builder's.
    assertThat(updated.getDescription()).isEmpty();
  }

  @Test
  public void testValuesAreRequired() {
    assertThatThrownBy(() -> new TagEntity.Builder("t").setTargetTypes(List.of("CATALOG")).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Values must be specified");
  }

  @Test
  public void testTargetTypesAreRequired() {
    assertThatThrownBy(() -> new TagEntity.Builder("t").setValues(List.of("public")).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Target types must be specified");
  }

  @Test
  public void testValidateValuesRejectsEmptyList() {
    assertThatThrownBy(() -> TagValidation.validateValues(List.of()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("at least one value");
  }

  @Test
  public void testValidateValuesRejectsEmptyMember() {
    assertThatThrownBy(() -> TagValidation.validateValues(List.of("public", "")))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("empty value");
  }

  @Test
  public void testValidateValuesRejectsDuplicates() {
    assertThatThrownBy(() -> TagValidation.validateValues(List.of("public", "public")))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("duplicates");
  }

  @Test
  public void testValidateValuesIsCaseSensitive() {
    TagValidation.validateValues(List.of("Public", "public"));
  }

  @Test
  public void testValidateTargetTypesRejectsEmptyListAndDuplicates() {
    assertThatThrownBy(() -> TagValidation.validateTargetTypes(List.of()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("at least one type");
    assertThatThrownBy(() -> TagValidation.validateTargetTypes(List.of("CATALOG", "CATALOG")))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("duplicates");
  }

  @Test
  public void testValidateValuesRejectsAValueOverTwoThousandBytes() {
    TagValidation.validateValues(List.of("a".repeat(2000)));
    assertThatThrownBy(() -> TagValidation.validateValues(List.of("a".repeat(2001))))
        .hasMessageContaining("2000 bytes");
  }

  @Test
  public void testValidateValuesCountsBytesNotCharacters() {
    // Three bytes per character in UTF-8, so 667 characters is 2001 bytes and is refused even
    // though the same count of ASCII characters would be well inside the limit.
    String threeByteChar = "\u4e2d";
    TagValidation.validateValues(List.of(threeByteChar.repeat(666)));
    assertThatThrownBy(() -> TagValidation.validateValues(List.of(threeByteChar.repeat(667))))
        .hasMessageContaining("2000 bytes");
  }

  @Test
  public void testValidateValuesCountsBytesOfFourByteCharacters() {
    // Four bytes per character in UTF-8 and two UTF-16 chars each, so 500 is exactly 2000 bytes at
    // a String length of 1000, and 501 is over.
    String fourByteChar = "\uD83D\uDE00";
    TagValidation.validateValues(List.of(fourByteChar.repeat(500)));
    assertThatThrownBy(() -> TagValidation.validateValues(List.of(fourByteChar.repeat(501))))
        .isInstanceOf(BadRequestException.class)
        .hasMessage("Values must not exceed 2000 bytes, measured on the decoded value in UTF-8");
  }
}
