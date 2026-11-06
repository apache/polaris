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

import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.junit.jupiter.api.Test;

class CloudflareR2ProductionReadinessChecksTest {

  private static final String ACCESS_KEY_PROPERTY = "polaris.storage.cloudflare-r2.access-key";
  private static final String SECRET_KEY_PROPERTY = "polaris.storage.cloudflare-r2.secret-key";

  // Distinct dummy values, so an absence assertion on one cannot pass by accident for the other.
  private static final String ACCESS_KEY_VALUE = "dummy-access-value-1f3a";
  private static final String SECRET_KEY_VALUE = "dummy-secret-value-9c2e";

  private final CloudflareR2ProductionReadinessChecks checks =
      new CloudflareR2ProductionReadinessChecks();

  private static CloudflareR2Configuration config(
      Optional<String> accessKey, Optional<String> secretKey) {
    return new CloudflareR2Configuration() {
      @Override
      public Optional<String> accessKey() {
        return accessKey;
      }

      @Override
      public Optional<String> secretKey() {
        return secretKey;
      }

      @Override
      public Map<String, NamedToken> storages() {
        return Map.of();
      }
    };
  }

  @Test
  void bothHalvesSetReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkCloudflareR2ParentToken(
            config(Optional.of(ACCESS_KEY_VALUE), Optional.of(SECRET_KEY_VALUE)));

    assertThat(result.ready()).isTrue();
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  void neitherHalfSetReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkCloudflareR2ParentToken(config(Optional.empty(), Optional.empty()));

    assertThat(result.ready()).isTrue();
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  void onlyAccessKeySetNamesSecretKeyProperty() {
    ProductionReadinessCheck result =
        checks.checkCloudflareR2ParentToken(
            config(Optional.of(ACCESS_KEY_VALUE), Optional.empty()));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty()).isEqualTo(SECRET_KEY_PROPERTY);
              assertThat(error.severe()).isFalse();
              assertThat(error.message())
                  .isEqualTo(
                      "The default Cloudflare R2 parent token is ignored because "
                          + SECRET_KEY_PROPERTY
                          + " is not set. R2_LOCAL_SIGNING catalogs that use the default token"
                          + " cannot vend credentials until both "
                          + ACCESS_KEY_PROPERTY
                          + " and "
                          + SECRET_KEY_PROPERTY
                          + " are set.");
              assertThat(error.message()).doesNotContain(ACCESS_KEY_VALUE);
              assertThat(error.offendingProperty()).doesNotContain(ACCESS_KEY_VALUE);
            });
  }

  @Test
  void onlySecretKeySetNamesAccessKeyProperty() {
    ProductionReadinessCheck result =
        checks.checkCloudflareR2ParentToken(
            config(Optional.empty(), Optional.of(SECRET_KEY_VALUE)));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty()).isEqualTo(ACCESS_KEY_PROPERTY);
              assertThat(error.severe()).isFalse();
              assertThat(error.message())
                  .isEqualTo(
                      "The default Cloudflare R2 parent token is ignored because "
                          + ACCESS_KEY_PROPERTY
                          + " is not set. R2_LOCAL_SIGNING catalogs that use the default token"
                          + " cannot vend credentials until both "
                          + ACCESS_KEY_PROPERTY
                          + " and "
                          + SECRET_KEY_PROPERTY
                          + " are set.");
              assertThat(error.message()).doesNotContain(SECRET_KEY_VALUE);
              assertThat(error.offendingProperty()).doesNotContain(SECRET_KEY_VALUE);
            });
  }
}
