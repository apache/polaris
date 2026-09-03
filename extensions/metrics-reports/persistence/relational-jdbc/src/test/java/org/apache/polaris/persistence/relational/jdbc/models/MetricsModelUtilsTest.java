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
package org.apache.polaris.persistence.relational.jdbc.models;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

public class MetricsModelUtilsTest {

  @Test
  public void parseJsonArrayReadsJsonEncodedValues() {
    assertThat(MetricsModelUtils.parseJsonArray("[\"id\",\"name\",\"value\"]"))
        .containsExactly("id", "name", "value");
  }

  @Test
  public void parseJsonArrayReadsPreUpgradeCommaDelimitedRows() {
    // Rows written by Polaris 1.7.0, before this field switched to JSON encoding.
    assertThat(MetricsModelUtils.parseJsonArray("id,name,value"))
        .containsExactly("id", "name", "value");
  }

  @Test
  public void parseJsonArrayReadsSingleLegacyFieldName() {
    assertThat(MetricsModelUtils.parseJsonArray("id")).containsExactly("id");
  }

  @Test
  public void parseJsonArrayReturnsEmptyForNullOrBlank() {
    assertThat(MetricsModelUtils.parseJsonArray(null)).isEmpty();
    assertThat(MetricsModelUtils.parseJsonArray("")).isEmpty();
  }

  @Test
  public void toJsonArrayRoundTripsThroughParseJsonArray() {
    List<String> names = List.of("id", "name,with,commas", "value");
    String encoded = MetricsModelUtils.toJsonArray(names);
    assertThat(MetricsModelUtils.parseJsonArray(encoded)).containsExactlyElementsOf(names);
  }
}
