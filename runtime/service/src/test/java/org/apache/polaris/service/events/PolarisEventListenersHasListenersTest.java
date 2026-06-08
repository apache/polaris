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

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PolarisEventListenersHasListenersTest.Profile.class)
public class PolarisEventListenersHasListenersTest {
  @Singleton
  @Identifier("has-listeners-after-send-listener")
  public static class AfterSendEventListener implements PolarisEventListener {
    @Override
    public void onEvent(PolarisEvent event) {}
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.event-listener.types",
          "has-listeners-after-send-listener",
          "polaris.event-listener.has-listeners-after-send-listener.enabled-event-types",
          "AFTER_SEND_NOTIFICATION");
    }
  }

  @Inject PolarisEventDispatcher eventDispatcher;

  @Test
  public void testHasListenersReturnsTrueOnlyForConfiguredEventTypes() {
    assertThat(eventDispatcher.hasListeners(PolarisEventType.AFTER_SEND_NOTIFICATION)).isTrue();
    assertThat(eventDispatcher.hasListeners(PolarisEventType.BEFORE_SEND_NOTIFICATION)).isFalse();
  }
}
