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

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * When a schema already read for a table may be reused to render another of its rows. A reverse
 * lookup can read the rows of one table across several persistence reads inside one request, and a
 * later read may bring a newer metadata pointer, so reuse is only sound within one state of the
 * table.
 */
public class TagCatalogSchemaCacheTest {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()));

  @Test
  public void testTheSamePointerIsAHit() {
    assertThat(
            TagCatalog.stillDescribes(
                new TagCatalog.SchemaAt("file:///m/00000-a.json", SCHEMA),
                "file:///m/00000-a.json"))
        .isTrue();
  }

  @Test
  public void testAMovedPointerIsNotAHit() {
    assertThat(
            TagCatalog.stillDescribes(
                new TagCatalog.SchemaAt("file:///m/00000-a.json", SCHEMA),
                "file:///m/00001-b.json"))
        .isFalse();
  }

  /** Nothing cached yet is not a hit, which is the ordinary first read of a table. */
  @Test
  public void testAnAbsentEntryIsNotAHit() {
    assertThat(TagCatalog.stillDescribes(null, "file:///m/00000-a.json")).isFalse();
  }

  /**
   * A pointer that is missing on either side cannot be shown to describe one state, so it is not a
   * hit. The read that follows decides what an unreadable table means; this rule does not answer it
   * by reusing a schema.
   */
  @Test
  public void testAnUnknownPointerIsNotAHit() {
    assertThat(TagCatalog.stillDescribes(new TagCatalog.SchemaAt(null, SCHEMA), "file:///m/a.json"))
        .isFalse();
    assertThat(TagCatalog.stillDescribes(new TagCatalog.SchemaAt("file:///m/a.json", SCHEMA), null))
        .isFalse();
  }
}
