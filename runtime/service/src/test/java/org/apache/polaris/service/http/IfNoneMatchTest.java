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
package org.apache.polaris.service.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for If-None-Match header processing and ETag interaction scenarios. This includes both HTTP
 * header parsing and tests that verify how ETag generation works together with If-None-Match
 * processing for metadata location changes.
 */
public class IfNoneMatchTest {

  @Test
  public void validSingleETag() {
    String header = "W/\"value\"";
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader(header);

    String parsedETag = ifNoneMatch.eTags().getFirst();

    Assertions.assertEquals(header, parsedETag);
  }

  @Test
  public void validMultipleETags() {
    String etagValue1 = "W/\"etag1\"";
    String etagValue2 = "W/\"etag2,with,comma\"";
    String etagValue3 = "W/\"etag3\"";

    String header = etagValue1 + ", " + etagValue2 + ", " + etagValue3;
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader(header);

    Assertions.assertEquals(3, ifNoneMatch.eTags().size());

    String etag1 = ifNoneMatch.eTags().get(0);
    String etag2 = ifNoneMatch.eTags().get(1);
    String etag3 = ifNoneMatch.eTags().get(2);

    Assertions.assertEquals(etagValue1, etag1);
    Assertions.assertEquals(etagValue2, etag2);
    Assertions.assertEquals(etagValue3, etag3);
  }

  @Test
  public void validWildcardIfNoneMatch() {
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("*");
    Assertions.assertTrue(ifNoneMatch.isWildcard());
    Assertions.assertTrue(ifNoneMatch.eTags().isEmpty());
  }

  @Test
  public void nullIfNoneMatchIsValidAndMapsToEmptyETags() {
    // this test is important because we may not know what the representation of
    // the header will be if it is not provided. If it is null, then we should default
    // to an empty list of etags, as that has no footprint for logical decisions to be made
    IfNoneMatch nullIfNoneMatch = IfNoneMatch.fromHeader(null);
    Assertions.assertTrue(nullIfNoneMatch.eTags().isEmpty());
  }

  @Test
  public void invalidETagThrowsException() {
    String header = "wrong_value";
    Assertions.assertThrows(IllegalArgumentException.class, () -> IfNoneMatch.fromHeader(header));
  }

  @Test
  public void etagsMatch() {
    String weakETag = "W/\"weak\"";
    String strongETag = "\"strong\"";
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("W/\"weak\", \"strong\"");
    Assertions.assertTrue(ifNoneMatch.anyMatch(weakETag));
    Assertions.assertTrue(ifNoneMatch.anyMatch(strongETag));
  }

  @Test
  public void weakETagOnlyMatchesWeak() {
    String weakETag = "W/\"etag\"";
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("\"etag\"");
    Assertions.assertFalse(ifNoneMatch.anyMatch(weakETag));
  }

  @Test
  public void strongETagOnlyMatchesStrong() {
    String strongETag = "\"etag\"";
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("W/\"etag\"");
    Assertions.assertFalse(ifNoneMatch.anyMatch(strongETag));
  }

  @Test
  public void wildCardMatchesEverything() {
    String strongETag = "\"etag\"";
    String weakETag = "W/\"etag\"";
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader("*");
    Assertions.assertTrue(ifNoneMatch.anyMatch(strongETag));
    Assertions.assertTrue(ifNoneMatch.anyMatch(weakETag));

    IfNoneMatch canonicallyBuiltWildcard = IfNoneMatch.WILDCARD;
    Assertions.assertTrue(canonicallyBuiltWildcard.anyMatch(strongETag));
    Assertions.assertTrue(canonicallyBuiltWildcard.anyMatch(weakETag));
  }

  @Test
  public void cantConstructHeaderWithWildcardAndNonEmptyETag() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new IfNoneMatch(true, List.of("\"etag\"")));
  }

  @Test
  public void cantConstructHeaderWithOneValidAndOneInvalidPart() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> IfNoneMatch.fromHeader("W/\"etag\", W/invalid-etag"));
  }

  @Test
  public void invalidHeaderWithOnlyWhitespacesBetween() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> IfNoneMatch.fromHeader("W/\"etag\" \"valid-etag\""));
  }

  @Test
  public void testETagGenerationConsistency() {
    // Test that ETag generation is consistent for the same metadata location
    String metadataLocation = "s3://bucket/path/metadata.json";

    String etag1 = IcebergHttpUtil.generateETagForMetadataFileLocation(metadataLocation);
    String etag2 = IcebergHttpUtil.generateETagForMetadataFileLocation(metadataLocation);

    assertThat(etag1).isEqualTo(etag2);
    assertThat(etag1).startsWith("W/\"");
    assertThat(etag1).endsWith("\"");
  }

  @Test
  public void testETagChangeAfterMetadataLocationChange() {
    // Test that ETags change when metadata location changes (simulating schema updates)
    String originalMetadataLocation = "s3://bucket/path/metadata/v1.metadata.json";
    String updatedMetadataLocation = "s3://bucket/path/metadata/v2.metadata.json";

    String originalETag =
        IcebergHttpUtil.generateETagForMetadataFileLocation(originalMetadataLocation);
    String updatedETag =
        IcebergHttpUtil.generateETagForMetadataFileLocation(updatedMetadataLocation);

    // ETags should be different for different metadata locations
    assertThat(originalETag).isNotEqualTo(updatedETag);

    // Both should be valid weak ETags
    assertThat(originalETag).startsWith("W/\"").endsWith("\"");
    assertThat(updatedETag).startsWith("W/\"").endsWith("\"");

    // Test If-None-Match behavior with changed metadata
    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader(originalETag);

    // Original ETag should match itself
    assertThat(ifNoneMatch.anyMatch(originalETag)).isTrue();

    // Original ETag should NOT match the updated ETag (indicating table has changed)
    assertThat(ifNoneMatch.anyMatch(updatedETag)).isFalse();
  }

  @Test
  public void testETagBehaviorForTableSchemaChanges() {
    // Simulate a table schema change scenario
    String baseLocation = "s3://warehouse/db/table/metadata/";

    // Original table metadata
    String v1MetadataLocation = baseLocation + "v1.metadata.json";
    String v1ETag = IcebergHttpUtil.generateETagForMetadataFileLocation(v1MetadataLocation);

    // After adding a column (new metadata version)
    String v2MetadataLocation = baseLocation + "v2.metadata.json";
    String v2ETag = IcebergHttpUtil.generateETagForMetadataFileLocation(v2MetadataLocation);

    // After adding another column (another metadata version)
    String v3MetadataLocation = baseLocation + "v3.metadata.json";
    String v3ETag = IcebergHttpUtil.generateETagForMetadataFileLocation(v3MetadataLocation);

    // All ETags should be different
    Set<String> etagSet = Set.of(v1ETag, v2ETag, v3ETag);
    assertThat(etagSet)
        .as("Schema evolution should generate unique ETags for each version (v1, v2, v3)")
        .hasSize(3);

    // Test If-None-Match with original ETag after schema changes
    IfNoneMatch originalIfNoneMatch = IfNoneMatch.fromHeader(v1ETag);

    // Should match the original version
    assertThat(originalIfNoneMatch.anyMatch(v1ETag)).isTrue();

    // Should NOT match newer versions (indicating table has changed)
    assertThat(originalIfNoneMatch.anyMatch(v2ETag)).isFalse();
    assertThat(originalIfNoneMatch.anyMatch(v3ETag)).isFalse();

    // Test with multiple ETags including the current one
    String multipleETags = "W/\"some-old-etag\", " + v1ETag + ", W/\"another-old-etag\"";
    IfNoneMatch multipleIfNoneMatch = IfNoneMatch.fromHeader(multipleETags);

    // Should match v1 (one of the ETags in the list)
    assertThat(multipleIfNoneMatch.anyMatch(v1ETag)).isTrue();

    // Should NOT match v2 or v3 (not in the list)
    assertThat(multipleIfNoneMatch.anyMatch(v2ETag)).isFalse();
    assertThat(multipleIfNoneMatch.anyMatch(v3ETag)).isFalse();
  }

  @Test
  public void testETagUniquenessAcrossTableLifecycle() {
    // Test ETag uniqueness across the complete table lifecycle
    String baseLocation = "s3://warehouse/db/users/metadata/";

    // Original table creation
    String v1MetadataLocation = baseLocation + "v1.metadata.json";
    String v1ETag = IcebergHttpUtil.generateETagForMetadataFileLocation(v1MetadataLocation);

    // Schema evolution
    String v2MetadataLocation = baseLocation + "v2.metadata.json";
    String v2ETag = IcebergHttpUtil.generateETagForMetadataFileLocation(v2MetadataLocation);

    // More schema changes
    String v3MetadataLocation = baseLocation + "v3.metadata.json";
    String v3ETag = IcebergHttpUtil.generateETagForMetadataFileLocation(v3MetadataLocation);

    // Table dropped and recreated with different schema (new metadata path)
    String recreatedV1MetadataLocation = baseLocation + "recreated-v1.metadata.json";
    String recreatedV1ETag =
        IcebergHttpUtil.generateETagForMetadataFileLocation(recreatedV1MetadataLocation);

    // Further evolution of recreated table
    String recreatedV2MetadataLocation = baseLocation + "recreated-v2.metadata.json";
    String recreatedV2ETag =
        IcebergHttpUtil.generateETagForMetadataFileLocation(recreatedV2MetadataLocation);

    // All ETags should be unique
    List<String> allETags = List.of(v1ETag, v2ETag, v3ETag, recreatedV1ETag, recreatedV2ETag);

    // Verify all ETags are different from each other
    for (int i = 0; i < allETags.size(); i++) {
      for (int j = i + 1; j < allETags.size(); j++) {
        assertThat(allETags.get(i)).isNotEqualTo(allETags.get(j));
      }
    }

    // Test If-None-Match behavior across lifecycle
    IfNoneMatch originalV1IfNoneMatch = IfNoneMatch.fromHeader(v1ETag);

    // Should match original v1
    assertThat(originalV1IfNoneMatch.anyMatch(v1ETag)).isTrue();

    // Should NOT match any other version (evolution or recreation)
    assertThat(originalV1IfNoneMatch.anyMatch(v2ETag)).isFalse();
    assertThat(originalV1IfNoneMatch.anyMatch(v3ETag)).isFalse();
    assertThat(originalV1IfNoneMatch.anyMatch(recreatedV1ETag)).isFalse();
    assertThat(originalV1IfNoneMatch.anyMatch(recreatedV2ETag)).isFalse();

    // Test with multiple ETags from original table lifecycle
    String multipleOriginalETags = v1ETag + ", " + v2ETag + ", " + v3ETag;
    IfNoneMatch multipleOriginalIfNoneMatch = IfNoneMatch.fromHeader(multipleOriginalETags);

    // Should match any of the original table versions
    assertThat(multipleOriginalIfNoneMatch.anyMatch(v1ETag)).isTrue();
    assertThat(multipleOriginalIfNoneMatch.anyMatch(v2ETag)).isTrue();
    assertThat(multipleOriginalIfNoneMatch.anyMatch(v3ETag)).isTrue();

    // Should NOT match recreated table versions
    assertThat(multipleOriginalIfNoneMatch.anyMatch(recreatedV1ETag)).isFalse();
    assertThat(multipleOriginalIfNoneMatch.anyMatch(recreatedV2ETag)).isFalse();
  }
}
