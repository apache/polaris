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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestProfile(InMemoryEventListenerBufferSizeTest.Profile.class)
class InMemoryEventListenerBufferSizeTest extends InMemoryEventListenerTestBase {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(BASE_CONFIG)
          .put("polaris.event-listener.persistence-in-memory-buffer.buffer-time", "60s")
          .put("polaris.event-listener.persistence-in-memory-buffer.max-buffer-size", "10")
          .build();
    }
  }

  @Test
  void testFlushOnSize() {
    sendAsync("test1", 10);
    sendAsync("test2", 10);
    assertRows("test1", 10);
    assertRows("test2", 10);
  }

  @Test
  void testFlushOnShutdown() {
    producer.processEvent("test1", event());
    producer.processEvent("test2", event());
    producer.shutdown();
    assertRows("test1", 1);
    assertRows("test2", 1);
  }

  @Test
  void testFailureRecovery() {
    var manager = Mockito.mock(PolarisMetaStoreManager.class);
    doReturn(manager).when(metaStoreManagerFactory).getOrCreateMetaStoreManager(any());
    RuntimeException error = new RuntimeException("error");
    doThrow(error)
        .doThrow(error) // first batch will give up after 2 attempts
        .doThrow(error)
        .doCallRealMethod() // second batch will succeed on the 2nd attempt
        .when(manager)
        .writeEvents(any(), any());
    sendAsync("test1", 20);
    assertRows("test1", 10);
  }
}
