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

package org.apache.polaris.core.storage.aws.r2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class CloudflareR2EndpointTest {

  private static final String ACCOUNT = "0123456789abcdef0123456789abcdef";

  @Test
  void parsesTheDefaultHost() {
    CloudflareR2Endpoint endpoint =
        CloudflareR2Endpoint.parse("https://" + ACCOUNT + ".r2.cloudflarestorage.com");
    assertThat(endpoint.accountId()).isEqualTo(ACCOUNT);
    assertThat(endpoint.jurisdiction()).isNull();
    assertThat(endpoint.host()).isEqualTo(ACCOUNT + ".r2.cloudflarestorage.com");
  }

  @ParameterizedTest
  @ValueSource(strings = {"eu", "fedramp", "us"})
  void parsesEveryKnownJurisdiction(String jurisdiction) {
    CloudflareR2Endpoint endpoint =
        CloudflareR2Endpoint.parse(
            "https://" + ACCOUNT + "." + jurisdiction + ".r2.cloudflarestorage.com");
    assertThat(endpoint.accountId()).isEqualTo(ACCOUNT);
    assertThat(endpoint.jurisdiction()).isEqualTo(jurisdiction);
    assertThat(endpoint.host())
        .isEqualTo(ACCOUNT + "." + jurisdiction + ".r2.cloudflarestorage.com");
    assertThat(CloudflareR2Endpoint.KNOWN_JURISDICTIONS).contains(jurisdiction);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(
      strings = {
        "https://0123456789ABCDEF0123456789ABCDEF.r2.cloudflarestorage.com", // uppercase hex
        "https://0123456789abcdef0123456789abcde.r2.cloudflarestorage.com", // 31 hex chars
        "https://0123456789abcdef0123456789abcdef0.r2.cloudflarestorage.com", // 33 hex chars
        "https://0123456789abcdef0123456789abcdef.mars.r2.cloudflarestorage.com", // unknown label
        "https://x.0123456789abcdef0123456789abcdef.eu.r2.cloudflarestorage.com", // extra label
        "http://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com", // http
        "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com:443", // port
        "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com/", // path
        "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com?x=1", // query
        "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com#f", // fragment
        "https://user@0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com", // userinfo
        "https://s3.us-east-1.amazonaws.com", // not R2 at all
        ""
      })
  void rejectsEverythingElse(String endpoint) {
    assertThatThrownBy(() -> CloudflareR2Endpoint.parse(endpoint))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("endpoint")
        .hasMessageContaining("r2.cloudflarestorage.com");
  }
}
