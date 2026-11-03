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
package org.apache.polaris.extension.storage.cloudflare.r2;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CloudflareR2ParentTokenTest {

  private static final String ACCESS_KEY = "test-r2-access-key";
  private static final String SECRET_KEY = "test-r2-secret-key";

  @Test
  void toStringShowsTheKeyIdAndHidesTheSecret() {
    String text = new CloudflareR2ParentToken(ACCESS_KEY, SECRET_KEY).toString();
    assertThat(text)
        .isEqualTo("CloudflareR2ParentToken{accessKeyId=test-r2-access-key, secret=<redacted>}");
    assertThat(text).doesNotContain(SECRET_KEY);
  }

  @Test
  void fingerprintIsTheFirstFourBytesOfSha256AsLowercaseHex() {
    // sha256("test-r2-secret-key") starts with af6d386e.
    assertThat(CloudflareR2ParentToken.fingerprint(SECRET_KEY)).isEqualTo("af6d386e");
    assertThat(CloudflareR2ParentToken.fingerprint(SECRET_KEY)).hasSize(8).matches("[0-9a-f]{8}");
  }

  @Test
  void fingerprintIsStableAndDiffersBetweenSecrets() {
    assertThat(CloudflareR2ParentToken.fingerprint(SECRET_KEY))
        .isEqualTo(CloudflareR2ParentToken.fingerprint(SECRET_KEY))
        .isNotEqualTo(CloudflareR2ParentToken.fingerprint("other-secret"));
  }
}
