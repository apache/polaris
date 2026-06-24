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
package org.apache.polaris.core.storage.gcp;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.config.RealmConfig;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class GcpStorageCredentialCacheKeyTest {

  private static final GcpStorageConfigurationInfo CONFIG =
      GcpStorageConfigurationInfo.builder()
          .addAllAllowedLocations(java.util.List.of("gs://bucket/path"))
          .build();
  private static final GoogleCredentials CREDS = Mockito.mock(GoogleCredentials.class);
  private static final HttpTransportFactory TRANSPORT = NetHttpTransport::new;
  private static final RealmConfig REALM_CONFIG = Mockito.mock(RealmConfig.class);

  private static GcpStorageCredentialCacheKey key(Optional<String> principalName) {
    return GcpStorageCredentialCacheKey.of(
        "tenant1",
        CONFIG,
        Set.of("gs://bucket/path"),
        Set.of(),
        Set.of(),
        Optional.empty(),
        principalName,
        CREDS,
        TRANSPORT,
        REALM_CONFIG,
        GcpCredentialOps.DEFAULT,
        Optional.empty());
  }

  @Test
  void principalNameIsPartOfCacheIdentity() {
    // When attribution is on, the vended token is per-principal: two principals must not collide
    // on one cache entry.
    assertThat(key(Optional.of("alice"))).isNotEqualTo(key(Optional.of("bob")));
    assertThat(key(Optional.of("alice"))).hasSameHashCodeAs(key(Optional.of("alice")));
    assertThat(key(Optional.of("alice"))).isEqualTo(key(Optional.of("alice")));
  }

  @Test
  void emptyPrincipalSharesOneEntry() {
    // When attribution is off the principal is absent, preserving cross-principal cache reuse.
    assertThat(key(Optional.empty())).isEqualTo(key(Optional.empty()));
  }
}
