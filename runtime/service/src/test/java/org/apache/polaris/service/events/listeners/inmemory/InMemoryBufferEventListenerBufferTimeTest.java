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

package org.apache.polaris.service.events.listeners.inmemory;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestProfile(InMemoryBufferEventListenerBufferTimeTest.Profile.class)
class InMemoryBufferEventListenerBufferTimeTest extends InMemoryBufferEventListenerTestBase {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(BASE_CONFIG)
          .put("polaris.event-listener.persistence-in-memory-buffer.buffer-time", "100ms")
          .put("polaris.event-listener.persistence-in-memory-buffer.max-buffer-size", "1000")
          .build();
    }
  }

  @Test
  void testFlushOnTimeout() {
    sendAsync("test1", 5);
    sendAsync("test2", 1);
    assertRows("test1", 5);
    assertRows("test2", 1);
  }
}
