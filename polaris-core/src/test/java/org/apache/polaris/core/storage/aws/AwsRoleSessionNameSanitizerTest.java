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
package org.apache.polaris.core.storage.aws;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class AwsRoleSessionNameSanitizerTest {

  /** AWS STS role session name validation pattern. */
  private static final Pattern AWS_ROLE_SESSION_NAME_PATTERN = Pattern.compile("[\\w+=,.@-]*");

  @ParameterizedTest
  @CsvSource({
    "polaris-Invalid (local),polaris-Invalid__local_",
    "service/account:readonly,service_account_readonly",
    "user name,user_name",
    "polaris-test-principal,polaris-test-principal",
    "user@domain.com,user@domain.com",
    "key=value,key=value"
  })
  void testSanitize(String input, String expected) {
    assertThat(AwsRoleSessionNameSanitizer.sanitize(input)).isEqualTo(expected);
  }

  @Test
  void testSanitizeTruncatesToMaxLength() {
    String longInput = "a".repeat(100);
    String result = AwsRoleSessionNameSanitizer.sanitize(longInput);
    assertThat(result).hasSize(AwsRoleSessionNameSanitizer.MAX_ROLE_SESSION_NAME_LENGTH);
  }

  @Test
  void testSanitizeOutputMatchesAwsPattern() {
    String[] inputs = {
      "polaris-Invalid (local)",
      "special!@#$%chars",
      "path/to/resource",
      "very-long-name-" + "x".repeat(100)
    };

    for (String input : inputs) {
      String sanitized = AwsRoleSessionNameSanitizer.sanitize(input);
      assertThat(AWS_ROLE_SESSION_NAME_PATTERN.matcher(sanitized).matches())
          .as("Sanitized '%s' should match AWS pattern", sanitized)
          .isTrue();
      assertThat(sanitized.length()).isLessThanOrEqualTo(64);
    }
  }
}
