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

package org.apache.polaris.service.events;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PolarisEventListenersMultipleListenersTest.TwoEventListenersProfile.class)
public class PolarisEventListenersMultipleListenersTest {
  static CountDownLatch latch = new CountDownLatch(2);

  @ApplicationScoped
  @Identifier("test-event-listener-1")
  public static class EventListener1 implements PolarisEventListener {
    boolean called = false;

    @Override
    public void onEvent(PolarisEvent event) {
      if (called) {
        throw new IllegalStateException("EventListener1 should only be called once");
      }
      called = true;
      latch.countDown();
    }
  }

  @ApplicationScoped
  @Identifier("test-event-listener-2")
  public static class EventListener2 implements PolarisEventListener {
    boolean called = false;

    @Override
    public void onEvent(PolarisEvent event) {
      if (called) {
        throw new IllegalStateException("EventListener1 should only be called once");
      }
      called = true;
      latch.countDown();
    }
  }

  public static class TwoEventListenersProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.event-listener.types", "test-event-listener-1,test-event-listener-2")
          .build();
    }
  }

  @Inject PolarisEventDispatcher eventDispatcher;

  @Test
  public void testAllListenersGetNotified() throws InterruptedException {
    eventDispatcher.dispatch(new PolarisEvent(PolarisEventType.AFTER_SEND_NOTIFICATION, null));
    assertTrue(latch.await(5, TimeUnit.SECONDS));
  }
}
