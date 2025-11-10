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

package org.apache.polaris.core.storage;

import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_ENDPOINT;
import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_SECRET_KEY;
import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS;
import static org.apache.polaris.core.storage.StorageAccessProperty.EXPIRATION_TIME;
import static org.apache.polaris.core.storage.StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class StorageAccessConfigTest {

  @Test
  public void testPutGet() {
    StorageAccessConfig.Builder b = StorageAccessConfig.builder();
    b.put(AWS_ENDPOINT, "ep1");
    b.put(AWS_SECRET_KEY, "sk2");
    StorageAccessConfig c = b.build();
    assertThat(c.credentials()).isEqualTo(Map.of(AWS_SECRET_KEY.getPropertyName(), "sk2"));
    assertThat(c.extraProperties()).isEqualTo(Map.of(AWS_ENDPOINT.getPropertyName(), "ep1"));
    assertThat(c.get(AWS_SECRET_KEY)).isEqualTo("sk2");
    assertThat(c.get(AWS_ENDPOINT)).isEqualTo("ep1");
  }

  @Test
  public void testGetExtraProperty() {
    StorageAccessConfig.Builder b = StorageAccessConfig.builder();
    b.putExtraProperty(AWS_ENDPOINT.getPropertyName(), "extra");
    StorageAccessConfig c = b.build();
    assertThat(c.extraProperties()).isEqualTo(Map.of(AWS_ENDPOINT.getPropertyName(), "extra"));
    assertThat(c.get(AWS_ENDPOINT)).isEqualTo("extra");
  }

  @Test
  public void testGetInternalProperty() {
    StorageAccessConfig.Builder b = StorageAccessConfig.builder();
    b.putExtraProperty(AWS_ENDPOINT.getPropertyName(), "extra");
    b.putInternalProperty(AWS_ENDPOINT.getPropertyName(), "ep1");
    StorageAccessConfig c = b.build();
    assertThat(c.extraProperties()).isEqualTo(Map.of(AWS_ENDPOINT.getPropertyName(), "extra"));
    assertThat(c.internalProperties()).isEqualTo(Map.of(AWS_ENDPOINT.getPropertyName(), "ep1"));
    assertThat(c.get(AWS_ENDPOINT)).isEqualTo("ep1");
  }

  @Test
  public void testNoCredentialOverride() {
    StorageAccessConfig.Builder b = StorageAccessConfig.builder();
    b.put(AWS_SECRET_KEY, "sk-test");
    b.putExtraProperty(AWS_SECRET_KEY.getPropertyName(), "sk-extra");
    b.putInternalProperty(AWS_SECRET_KEY.getPropertyName(), "sk-internal");
    StorageAccessConfig c = b.build();
    assertThat(c.get(AWS_SECRET_KEY)).isEqualTo("sk-test");
    assertThat(c.extraProperties()).isEqualTo(Map.of(AWS_SECRET_KEY.getPropertyName(), "sk-extra"));
    assertThat(c.internalProperties())
        .isEqualTo(Map.of(AWS_SECRET_KEY.getPropertyName(), "sk-internal"));
  }

  @Test
  public void testExpiresAt() {
    StorageAccessConfig.Builder b = StorageAccessConfig.builder();
    assertThat(b.build().expiresAt()).isEmpty();
    b.put(GCS_ACCESS_TOKEN_EXPIRES_AT, "111");
    assertThat(b.build().expiresAt()).hasValue(Instant.ofEpochMilli(111));
    b.put(AWS_SESSION_TOKEN_EXPIRES_AT_MS, "222");
    assertThat(b.build().expiresAt()).hasValue(Instant.ofEpochMilli(222));
    b.put(EXPIRATION_TIME, "333");
    assertThat(b.build().expiresAt()).hasValue(Instant.ofEpochMilli(333));
    b.expiresAt(Instant.ofEpochMilli(444));
    assertThat(b.build().expiresAt()).hasValue(Instant.ofEpochMilli(444));
  }
}
