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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BasePolicyValidatorTest {
  private BasePolicyValidator validator;

  @BeforeEach
  public void setUp() {
    validator = new BasePolicyValidator();
  }

  @Test
  public void testValidateValidPolicy() {
    var validJson = "{\"enable\": False}";
    var result = validator.validate(new Policy(validJson));
    assertThat(result).isTrue();

    validJson = "{\"enable\": true, \"target_file_size_bytes\": 12342}";
    result = validator.validate(new Policy(validJson));
    assertThat(result).isTrue();

    validJson = "{\"version\":\"2025-02-03\", \"enable\": true, \"target_file_size_bytes\": 12342}";
    result = validator.validate(new Policy(validJson));
    assertThat(result).isTrue();

    validJson = "{\"enable\": true, \"config\": {\"key1\": \"value1\", \"key2\": true}}";
    result = validator.validate(new Policy(validJson));
    assertThat(result).isTrue();
  }

  @Test
  public void testInValidateValidPolicy() {
    // missing required key
    var inValidJson = "{}";
    var result = validator.validate(new Policy(inValidJson));
    assertThat(result).isFalse();

    // invalid keys
    inValidJson = "{\"enable\": true, \"invalid_key\": 12342}";
    result = validator.validate(new Policy(inValidJson));
    assertThat(result).isFalse();
  }
}
