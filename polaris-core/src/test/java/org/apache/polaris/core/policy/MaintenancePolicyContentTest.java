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
package org.apache.polaris.core.policy;

import static org.apache.polaris.core.policy.content.maintenance.OrphanFileRemovalPolicyContent.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.polaris.core.policy.content.maintenance.BaseMaintenancePolicyContent;
import org.apache.polaris.core.policy.content.maintenance.DataCompactionPolicyContent;
import org.apache.polaris.core.policy.content.maintenance.MetadataCompactionPolicyContent;
import org.apache.polaris.core.policy.content.maintenance.OrphanFileRemovalPolicyContent;
import org.apache.polaris.core.policy.content.maintenance.SnapshotExpiryPolicyContent;
import org.apache.polaris.core.policy.validator.InvalidPolicyException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class MaintenancePolicyContentTest {
  static Stream<Arguments> policyTypes() {
    return Stream.of(
        Arguments.of(PredefinedPolicyTypes.DATA_COMPACTION),
        Arguments.of(PredefinedPolicyTypes.METADATA_COMPACTION),
        Arguments.of(PredefinedPolicyTypes.ORPHAN_FILE_REMOVAL),
        Arguments.of(PredefinedPolicyTypes.METADATA_COMPACTION));
  }

  Function<String, BaseMaintenancePolicyContent> getParser(PredefinedPolicyTypes policyType) {
    switch (policyType) {
      case DATA_COMPACTION:
        return DataCompactionPolicyContent::fromString;
      case METADATA_COMPACTION:
        return MetadataCompactionPolicyContent::fromString;
      case ORPHAN_FILE_REMOVAL:
        return OrphanFileRemovalPolicyContent::fromString;
      case SNAPSHOT_EXPIRY:
        return SnapshotExpiryPolicyContent::fromString;
      default:
        throw new IllegalArgumentException("Unknown policy type: " + policyType);
    }
  }

  @ParameterizedTest
  @MethodSource("policyTypes")
  public void testValidPolicyContent(PredefinedPolicyTypes policyType) {
    var parser = getParser(policyType);

    assertThat(parser.apply("{\"enable\": false}").enabled()).isFalse();
    assertThat(parser.apply("{\"enable\": true}").enabled()).isTrue();

    var validJson = "{\"version\":\"2025-02-03\", \"enable\": true}";
    assertThat(parser.apply(validJson).getVersion()).isEqualTo("2025-02-03");

    validJson = "{\"enable\": true, \"config\": {\"key1\": \"value1\", \"key2\": true}}";
    assertThat(parser.apply(validJson).getConfig().get("key1")).isEqualTo("value1");
  }

  @ParameterizedTest
  @MethodSource("policyTypes")
  void testIsValidEmptyString(PredefinedPolicyTypes policyTypes) {
    var parser = getParser(policyTypes);
    assertThatThrownBy(() -> parser.apply(""))
        .as("Validating empty string should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Policy is empty");
  }

  @ParameterizedTest
  @MethodSource("policyTypes")
  void testIsValidJSONLiteralNull(PredefinedPolicyTypes policyTypes) {
    var parser = getParser(policyTypes);
    assertThatThrownBy(() -> parser.apply("null"))
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy: null");
  }

  @ParameterizedTest
  @MethodSource("policyTypes")
  void testIsValidEmptyJson(PredefinedPolicyTypes policyTypes) {
    var parser = getParser(policyTypes);
    assertThatThrownBy(() -> parser.apply("{}"))
        .as("Validating empty JSON '{}' should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @ParameterizedTest
  @MethodSource("policyTypes")
  void testIsValidInvalidVersionFormat(PredefinedPolicyTypes policyTypes) {
    var parser = getParser(policyTypes);
    String invalidPolicy = "{\"enable\": true, \"version\": \"fdafds\"}";
    assertThatThrownBy(() -> parser.apply(invalidPolicy))
        .as("Validating policy with invalid version format should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class);
  }

  @ParameterizedTest
  @MethodSource("policyTypes")
  void testIsValidInvalidKeyInPolicy(PredefinedPolicyTypes policyTypes) {
    var parser = getParser(policyTypes);
    String invalidPolicy = "{\"version\":\"2025-02-03\", \"enable\": true, \"invalid_key\": 12342}";
    assertThatThrownBy(() -> parser.apply(invalidPolicy))
        .as("Validating policy with an unknown key should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @ParameterizedTest
  @MethodSource("policyTypes")
  void testIsValidUnrecognizedToken(PredefinedPolicyTypes policyTypes) {
    var parser = getParser(policyTypes);
    var invalidPolicy = "{\"enable\": invalidToken}";
    assertThatThrownBy(() -> parser.apply(invalidPolicy))
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @ParameterizedTest
  @MethodSource("policyTypes")
  void testIsValidNullValue(PredefinedPolicyTypes policyTypes) {
    var parser = getParser(policyTypes);
    var invalidPolicy = "{\"enable\": null}";
    assertThatThrownBy(() -> parser.apply(invalidPolicy))
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @ParameterizedTest
  @MethodSource("policyTypes")
  void testIsValidWrongString(PredefinedPolicyTypes policyTypes) {
    var parser = getParser(policyTypes);
    var invalidPolicy = "{\"enable\": \"invalid\"}";
    assertThatThrownBy(() -> parser.apply(invalidPolicy))
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @Test
  public void testValidOrphanFileRemovalPolicyContent() {
    assertThat(
            fromString("{\"enable\": true, \"max_orphan_file_age_in_days\": 3}")
                .getMaxOrphanFileAgeInDays())
        .isEqualTo(3);
    assertThat(
            fromString(
                    "{\"enable\": true, \"max_orphan_file_age_in_days\": 3, \"locations\": ["
                        + "    \"s3://my-bucket/ns/my_table/\","
                        + "    \"s3://my-bucket/ns/my_table/my-data/\","
                        + "    \"s3://my-bucket/ns/my_table/my-metadata\""
                        + "  ]}")
                .getLocations()
                .get(0))
        .isEqualTo("s3://my-bucket/ns/my_table/");
  }

  @Test
  public void testInvalidOrphanFileRemovalPolicyContent() {
    assertThatThrownBy(() -> fromString("{\"enable\": true, \"max_orphan_file_age_in_days\": -3}"))
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid max_orphan_file_age_in_days");
  }
}
