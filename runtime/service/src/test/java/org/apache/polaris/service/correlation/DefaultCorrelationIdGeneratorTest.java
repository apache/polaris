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

package org.apache.polaris.service.correlation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.polaris.service.correlation.DefaultCorrelationIdGenerator.RequestId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultCorrelationIdGeneratorTest {

  private DefaultCorrelationIdGenerator requestIdGenerator;

  @BeforeEach
  void setUp() {
    requestIdGenerator = new DefaultCorrelationIdGenerator();
  }

  @Test
  void testGeneratesUniqueIds() {
    Set<String> generatedIds = new ConcurrentSkipListSet<>();
    try (ExecutorService executor = Executors.newFixedThreadPool(10)) {
      for (int i = 0; i < 1000; i++) {
        executor.execute(() -> generatedIds.add(requestIdGenerator.nextRequestId().toString()));
      }
    }
    assertThat(generatedIds).hasSize(1000);
  }

  @Test
  void testCounterIncrementsSequentially() {
    assertThat(requestIdGenerator.nextRequestId().counter()).isEqualTo(1L);
    assertThat(requestIdGenerator.nextRequestId().counter()).isEqualTo(2L);
    assertThat(requestIdGenerator.nextRequestId().counter()).isEqualTo(3L);
  }

  @Test
  void testCounterRotationAtMax() {
    requestIdGenerator.state.set(new RequestId(UUID.randomUUID(), Long.MAX_VALUE));

    var beforeRotation = requestIdGenerator.nextRequestId();
    var afterRotation = requestIdGenerator.nextRequestId();

    // The UUID should be different after rotation
    assertThat(beforeRotation.uuid()).isNotEqualTo(afterRotation.uuid());

    // The counter should be reset to 1 after rotation
    assertThat(beforeRotation.counter()).isEqualTo(Long.MAX_VALUE);
    assertThat(afterRotation.counter()).isEqualTo(1L);
  }

  @Test
  void testRequestIdToString() {
    var uuid = UUID.randomUUID();
    assertThat(new RequestId(uuid, 1L).toString()).isEqualTo(uuid + "_0000000000000000001");
    assertThat(new RequestId(uuid, 12345L).toString()).isEqualTo(uuid + "_0000000000000012345");
    assertThat(new RequestId(uuid, Long.MAX_VALUE).toString())
        .isEqualTo(uuid + "_9223372036854775807");
  }
}
