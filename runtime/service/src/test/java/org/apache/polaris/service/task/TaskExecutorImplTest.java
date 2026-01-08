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
package org.apache.polaris.service.task;

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.TestPolarisEventListener;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for TaskExecutorImpl */
public class TaskExecutorImplTest {
  @Test
  void testEventsAreEmitted() {
    String realm = "myrealm";
    RealmContext realmContext = () -> realm;

    TestServices testServices = TestServices.builder().realmContext(realmContext).build();

    TestPolarisEventListener testPolarisEventListener =
        (TestPolarisEventListener) testServices.polarisEventListener();

    PolarisMetaStoreManager metaStoreManager = testServices.metaStoreManager();

    PolarisCallContext polarisCallCtx = testServices.newCallContext();

    // This task doesn't have a type so it won't be handle-able by a real handler. We register a
    // test TaskHandler below that can handle any task.
    TaskEntity taskEntity =
        new TaskEntity.Builder()
            .setName("mytask")
            .setId(metaStoreManager.generateNewEntityId(polarisCallCtx).getId())
            .setCreateTimestamp(testServices.clock().millis())
            .build();
    metaStoreManager.createEntityIfNotExists(polarisCallCtx, null, taskEntity);

    int attempt = 1;

    TaskExecutorImpl executor =
        new TaskExecutorImpl(
            Runnable::run,
            null,
            testServices.clock(),
            testServices.metaStoreManagerFactory(),
            new TaskFileIOSupplier(
                testServices.fileIOFactory(), testServices.storageAccessConfigProvider()),
            new RealmContextHolder(),
            testServices.polarisEventListener(),
            testServices.eventMetadataFactory(),
            null,
            new PolarisPrincipalHolder(),
            testServices.principal());

    executor.addTaskHandler(
        new TaskHandler() {
          @Override
          public boolean canHandleTask(TaskEntity task) {
            return true;
          }

          @Override
          public boolean handleTask(TaskEntity task, CallContext callContext) {
            PolarisEvent beforeTaskAttemptedEvent =
                testPolarisEventListener.getLatest(PolarisEventType.BEFORE_ATTEMPT_TASK);
            Assertions.assertEquals(
                taskEntity.getId(),
                beforeTaskAttemptedEvent
                    .attributes()
                    .get(EventAttributes.TASK_ENTITY_ID)
                    .orElse(null));
            Assertions.assertEquals(
                attempt,
                beforeTaskAttemptedEvent
                    .attributes()
                    .get(EventAttributes.TASK_ATTEMPT)
                    .orElse(null));
            return true;
          }
        });

    executor.handleTask(
        taskEntity.getId(),
        polarisCallCtx,
        PolarisEventMetadata.builder().realmId(realm).build(),
        attempt);

    PolarisEvent afterAttemptTaskEvent =
        testPolarisEventListener.getLatest(PolarisEventType.AFTER_ATTEMPT_TASK);
    Assertions.assertEquals(
        taskEntity.getId(),
        afterAttemptTaskEvent.attributes().get(EventAttributes.TASK_ENTITY_ID).orElse(null));
    Assertions.assertEquals(
        attempt, afterAttemptTaskEvent.attributes().get(EventAttributes.TASK_ATTEMPT).orElse(null));
    Assertions.assertEquals(
        true, afterAttemptTaskEvent.attributes().get(EventAttributes.TASK_SUCCESS).orElse(null));
  }
}
