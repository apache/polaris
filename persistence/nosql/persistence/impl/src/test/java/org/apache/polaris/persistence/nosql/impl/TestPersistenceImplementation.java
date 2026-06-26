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
package org.apache.polaris.persistence.nosql.impl;

import static org.mockito.Mockito.mock;

import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestPersistenceImplementation {
  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  public void referencePreviousHeadCountMustBePositive(int count) {
    var params =
        PersistenceParams.BuildablePersistenceParams.builder()
            .referencePreviousHeadCount(count)
            .build();

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new PersistenceImplementation(
                    mock(Backend.class),
                    params,
                    "realm",
                    mock(MonotonicClock.class),
                    IdGenerator.NONE))
        .withMessage("referencePreviousHeadCount must be positive");
  }
}
