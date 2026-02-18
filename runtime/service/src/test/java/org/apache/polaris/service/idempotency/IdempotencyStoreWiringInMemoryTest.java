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
package org.apache.polaris.service.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.core.persistence.IdempotencyStore.ReserveResult;
import org.apache.polaris.core.persistence.IdempotencyStore.ReserveResultType;
import org.apache.polaris.core.persistence.InMemoryIdempotencyStore;
import org.apache.polaris.service.catalog.Profiles;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(IdempotencyStoreWiringInMemoryTest.InMemoryProfile.class)
class IdempotencyStoreWiringInMemoryTest {

  public static class InMemoryProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> cfg = new HashMap<>(new Profiles.DefaultProfile().getConfigOverrides());
      cfg.put("polaris.realm-context.realms", "test");
      cfg.put("polaris.persistence.type", "in-memory");
      return cfg;
    }
  }

  @Inject IdempotencyStore store;

  @Test
  void injectsInMemoryIdempotencyStore() {
    Object delegate = unwrapArcProxy(store);
    assertThat(delegate).isInstanceOf(InMemoryIdempotencyStore.class);
  }

  @Test
  void reserveSmokeTest() {
    Instant now = Instant.now();
    Instant exp = now.plus(Duration.ofMinutes(5));

    ReserveResult r1 = store.reserve("test", "K1", "op", "catalogs/1/tables/ns.tbl", exp, "A", now);
    assertThat(r1.getType()).isEqualTo(ReserveResultType.OWNED);

    ReserveResult r2 = store.reserve("test", "K1", "op", "catalogs/1/tables/ns.tbl", exp, "B", now);
    assertThat(r2.getType()).isEqualTo(ReserveResultType.DUPLICATE);
    assertThat(r2.getExisting()).isPresent();
  }

  private static Object unwrapArcProxy(Object bean) {
    try {
      // Arc client proxies implement this method to expose the contextual instance.
      return bean.getClass().getMethod("arc_contextualInstance").invoke(bean);
    } catch (ReflectiveOperationException ignored) {
      return bean;
    }
  }
}
