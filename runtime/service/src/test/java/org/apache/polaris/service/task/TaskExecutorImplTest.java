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

import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.events.AfterAttemptTaskEvent;
import org.apache.polaris.service.events.BeforeAttemptTaskEvent;
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

    // This task doesn't have a type so it won't be handle-able by a real handler. We register a
    // test TaskHandler below that can handle any task.
    TaskEntity taskEntity =
        new TaskEntity.Builder()
            .setName("mytask")
            .setId(metaStoreManager.generateNewEntityId().getId())
            .setCreateTimestamp(testServices.clock().millis())
            .build();
    metaStoreManager.createEntityIfNotExists(null, taskEntity);

    int attempt = 1;

    TaskExecutorImpl executor =
        new TaskExecutorImpl(
            Runnable::run,
            testServices.clock(),
            testServices.metaStoreManagerFactory(),
            new TaskFileIOSupplier(testServices.fileIOFactory()),
            testServices.polarisEventListener(),
            null);

    executor.addTaskHandler(
        new TaskHandler() {
          @Override
          public boolean canHandleTask(TaskEntity task) {
            return true;
          }

          @Override
          public boolean handleTask(
              RealmContext realmContext, RealmConfig realmConfig, TaskEntity task) {
            var beforeTaskAttemptedEvent =
                testPolarisEventListener.getLatest(BeforeAttemptTaskEvent.class);
            Assertions.assertEquals(taskEntity.getId(), beforeTaskAttemptedEvent.taskEntityId());
            Assertions.assertEquals(attempt, beforeTaskAttemptedEvent.attempt());
            return true;
          }
        });

    executor.handleTask(
        testServices.realmContext(), testServices.realmConfig(), taskEntity.getId(), attempt);

    var afterAttemptTaskEvent = testPolarisEventListener.getLatest(AfterAttemptTaskEvent.class);
    Assertions.assertEquals(taskEntity.getId(), afterAttemptTaskEvent.taskEntityId());
    Assertions.assertEquals(attempt, afterAttemptTaskEvent.attempt());
    Assertions.assertTrue(afterAttemptTaskEvent.success());
  }
}
