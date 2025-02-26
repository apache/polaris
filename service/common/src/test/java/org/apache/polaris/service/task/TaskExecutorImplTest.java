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

import java.time.Clock;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.events.AfterAttemptTaskEvent;
import org.apache.polaris.service.events.BeforeAttemptTaskEvent;
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
    PolarisMetaStoreSession metaStoreSession =
        metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get();

    PolarisDiagnostics diagnostics = testServices.polarisDiagnostics();
    Clock clock = Clock.systemUTC();

    TaskEntity taskEntity =
        new TaskEntity.Builder()
            .setName("mytask")
            .setId(metaStoreManager.generateNewEntityId(metaStoreSession).getId())
            .build();
    metaStoreManager.createEntityIfNotExists(metaStoreSession, null, taskEntity);

    int attempt = 1;

    TaskExecutorImpl executor =
        new TaskExecutorImpl(
            Runnable::run,
            testServices.metaStoreManagerFactory(),
            testServices.configurationStore(),
            diagnostics,
            new TaskFileIOSupplier(testServices.fileIOFactory()),
            clock,
            testServices.polarisEventListener());
    executor.addTaskHandler(
        new TaskHandler() {
          @Override
          public boolean canHandleTask(TaskEntity task) {
            return true;
          }

          @Override
          public boolean handleTask(TaskEntity task, RealmContext realmContext) {
            BeforeAttemptTaskEvent beforeAttemptTaskEvent =
                testPolarisEventListener.getLatest(BeforeAttemptTaskEvent.class);
            Assertions.assertEquals(taskEntity.getId(), beforeAttemptTaskEvent.taskEntityId());
            Assertions.assertEquals(realmContext, beforeAttemptTaskEvent.realmContext());
            Assertions.assertEquals(attempt, beforeAttemptTaskEvent.attempt());
            return true;
          }
        });

    executor.handleTask(taskEntity.getId(), realmContext, attempt);

    AfterAttemptTaskEvent afterAttemptTaskEvent =
        testPolarisEventListener.getLatest(AfterAttemptTaskEvent.class);
    Assertions.assertEquals(taskEntity.getId(), afterAttemptTaskEvent.taskEntityId());
    Assertions.assertEquals(realmContext, afterAttemptTaskEvent.realmContext());
    Assertions.assertEquals(attempt, afterAttemptTaskEvent.attempt());
    Assertions.assertTrue(afterAttemptTaskEvent.success());
  }
}
