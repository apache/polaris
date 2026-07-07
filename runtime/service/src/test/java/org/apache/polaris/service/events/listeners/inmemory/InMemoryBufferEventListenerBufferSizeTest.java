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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.subscription.BackPressureFailure;
import java.time.Duration;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.Profiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestProfile(Profiles.InMemoryBufferEventListenerBufferSizeProfile.class)
class InMemoryBufferEventListenerBufferSizeTest extends InMemoryBufferEventListenerTestBase {

  @Test
  void testFlushOnSize() {
    sendAsync("test1", 10);
    sendAsync("test2", 10);
    assertRows("test1", 10);
    assertRows("test2", 10);
    verify(metaStoreManagerFactory, never()).getOrCreateMetricsPersistence(any());
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
  void testFlushFailureRecovery() {
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

  @Test
  void testProcessorFailureRecovery() {
    producer.processEvent("test1", event());
    var test1 = producer.processor("test1");
    assertThat(test1).isNotNull();
    // emulate backpressure error; will drop the event and invalidate the processor
    test1.processor.onError(new BackPressureFailure("error"));
    // wait for the processor to be evicted from the cache
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(producer.hasProcessor("test1")).isFalse());
    // will create a new processor and recover
    sendAsync("test1", 10);
    assertRows("test1", 10);
  }
}
