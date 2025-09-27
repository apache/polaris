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

package org.apache.polaris.service.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RequestIdGeneratorTest {

  private RequestIdGenerator requestIdGenerator;

  @BeforeEach
  void setUp() {
    requestIdGenerator = new RequestIdGenerator();
  }

  @Test
  void testGenerateRequestId_ReturnsValidFormat() {
    String requestId = requestIdGenerator.generateRequestId();

    assertThat(requestId).isNotNull();
    assertThat(requestId).matches(this::isValidRequestIdFormat);
    assertThat(requestId).contains("_1"); // First call should increment counter to 1
  }

  @Test
  void testGenerateRequestId_ReturnsUniqueIds() {
    Set<String> generatedIds = new HashSet<>();

    // Generate multiple request IDs and verify they're all unique
    for (int i = 0; i < 1000; i++) {
      String requestId = requestIdGenerator.generateRequestId();
      assertThat(generatedIds).doesNotContain(requestId);
      generatedIds.add(requestId);
    }

    assertThat(generatedIds).hasSize(1000);
  }

  @Test
  void testCounterIncrementsSequentially() {
    requestIdGenerator.setCounter(0);

    String firstId = requestIdGenerator.generateRequestId();
    String secondId = requestIdGenerator.generateRequestId();
    String thirdId = requestIdGenerator.generateRequestId();

    assertThat(firstId).endsWith("_1");
    assertThat(secondId).endsWith("_2");
    assertThat(thirdId).endsWith("_3");
  }

  @Test
  void testCounterRotationAtSoftMax() {
    // Set counter close to soft max
    long softMax = RequestIdGenerator.COUNTER_SOFT_MAX;
    requestIdGenerator.setCounter(softMax - 1);

    String beforeRotation = requestIdGenerator.generateRequestId();
    String afterRotation = requestIdGenerator.generateRequestId();

    // The UUID part should be different after rotation
    String beforeUuidPart = beforeRotation.substring(0, beforeRotation.lastIndexOf('_'));
    String afterUuidPart = afterRotation.substring(0, afterRotation.lastIndexOf('_'));
    assertNotEquals(beforeUuidPart, afterUuidPart);

    // After rotation, counter should be reset and UUID should be different
    assertThat(beforeRotation).endsWith("_" + softMax);
    assertThat(afterRotation).endsWith("_1"); // Counter reset to 1 (after increment from 0)
  }

  @Test
  void testSetCounterChangesNextGeneratedId() {
    requestIdGenerator.setCounter(100);

    String requestId = requestIdGenerator.generateRequestId();

    assertThat(requestId).endsWith("_101"); // Should increment from set value
  }

  private boolean isValidRequestIdFormat(String str) {
    try {
      String[] requestIdParts = str.split("_");
      String uuid = requestIdParts[0];
      String counter = requestIdParts[1];
      UUID.fromString(uuid);
      Long.parseLong(counter);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
