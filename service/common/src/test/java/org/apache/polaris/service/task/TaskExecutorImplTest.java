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
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.events.AfterTaskAttemptedEvent;
import org.apache.polaris.service.events.BeforeTaskAttemptedEvent;
import org.apache.polaris.service.events.TestPolarisEventListener;
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

    MetaStoreManagerFactory metaStoreManagerFactory = testServices.metaStoreManagerFactory();
    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    BasePersistence bp = metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get();

    PolarisCallContext polarisCallCtx =
        new PolarisCallContext(realmContext, bp, testServices.polarisDiagnostics());

    // This task doesn't have a type so it won't be handle-able by a real handler. We register a
    // test TaskHandler below that can handle any task.
    TaskEntity taskEntity =
        new TaskEntity.Builder()
            .setName("mytask")
            .setId(metaStoreManager.generateNewEntityId(polarisCallCtx).getId())
            .build();
    metaStoreManager.createEntityIfNotExists(polarisCallCtx, null, taskEntity);

    int attempt = 1;

    TaskExecutorImpl executor =
        new TaskExecutorImpl(
            Runnable::run,
            testServices.metaStoreManagerFactory(),
            new TaskFileIOSupplier(testServices.fileIOFactory()),
            testServices.polarisEventListener());
    executor.addTaskHandler(
        new TaskHandler() {
          @Override
          public boolean canHandleTask(TaskEntity task) {
            return true;
          }

          @Override
          public boolean handleTask(TaskEntity task, CallContext callContext) {
            var beforeTaskAttemptedEvent =
                testPolarisEventListener.getLatest(BeforeTaskAttemptedEvent.class);
            Assertions.assertEquals(taskEntity.getId(), beforeTaskAttemptedEvent.taskEntityId());
            Assertions.assertEquals(callContext, beforeTaskAttemptedEvent.callContext());
            Assertions.assertEquals(attempt, beforeTaskAttemptedEvent.attempt());
            return true;
          }
        });

    executor.handleTask(taskEntity.getId(), polarisCallCtx, attempt);

    var afterAttemptTaskEvent = testPolarisEventListener.getLatest(AfterTaskAttemptedEvent.class);
    Assertions.assertEquals(taskEntity.getId(), afterAttemptTaskEvent.taskEntityId());
    Assertions.assertEquals(polarisCallCtx, afterAttemptTaskEvent.callContext());
    Assertions.assertEquals(attempt, afterAttemptTaskEvent.attempt());
    Assertions.assertTrue(afterAttemptTaskEvent.success());
  }
}
