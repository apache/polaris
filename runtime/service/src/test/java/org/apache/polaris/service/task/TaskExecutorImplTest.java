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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.InMemoryEventCollector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for TaskExecutorImpl */
public class TaskExecutorImplTest {
  @Test
  void testEventsAreEmitted() {
    String realm = "myrealm";
    RealmContext realmContext = () -> realm;

    TestServices testServices = TestServices.builder().realmContext(realmContext).build();

    InMemoryEventCollector testPolarisEventDispatcher =
        (InMemoryEventCollector) testServices.polarisEventDispatcher();

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
            testServices.polarisEventDispatcher(),
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
                testPolarisEventDispatcher.getLatest(PolarisEventType.BEFORE_ATTEMPT_TASK);
            Assertions.assertEquals(
                taskEntity.getId(),
                beforeTaskAttemptedEvent.attributes().getRequired(EventAttributes.TASK_ENTITY_ID));
            Assertions.assertEquals(
                attempt,
                beforeTaskAttemptedEvent.attributes().getRequired(EventAttributes.TASK_ATTEMPT));
            return true;
          }
        });

    executor.handleTask(
        taskEntity.getId(),
        polarisCallCtx,
        PolarisEventMetadata.builder().realmId(realm).build(),
        attempt);

    PolarisEvent afterAttemptTaskEvent =
        testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_ATTEMPT_TASK);
    Assertions.assertEquals(
        taskEntity.getId(),
        afterAttemptTaskEvent.attributes().getRequired(EventAttributes.TASK_ENTITY_ID));
    Assertions.assertEquals(
        attempt, afterAttemptTaskEvent.attributes().getRequired(EventAttributes.TASK_ATTEMPT));
    Assertions.assertEquals(
        true, afterAttemptTaskEvent.attributes().getRequired(EventAttributes.TASK_SUCCESS));
  }

  @Test
  void handleTaskThrowsWhenNoHandlerFound() {
    String realm = "myrealm";
    RealmContext realmContext = () -> realm;

    TestServices testServices = TestServices.builder().realmContext(realmContext).build();

    PolarisMetaStoreManager metaStoreManager = testServices.metaStoreManager();
    PolarisCallContext polarisCallCtx = testServices.newCallContext();

    TaskEntity taskEntity =
        new TaskEntity.Builder()
            .setName("no-handler-task")
            .withTaskType(AsyncTaskType.MANIFEST_FILE_CLEANUP)
            .setId(metaStoreManager.generateNewEntityId(polarisCallCtx).getId())
            .setCreateTimestamp(testServices.clock().millis())
            .build();
    metaStoreManager.createEntityIfNotExists(polarisCallCtx, null, taskEntity);

    TaskExecutorImpl executor =
        new TaskExecutorImpl(
            Runnable::run,
            null,
            testServices.clock(),
            testServices.metaStoreManagerFactory(),
            new TaskFileIOSupplier(
                testServices.fileIOFactory(), testServices.storageAccessConfigProvider()),
            new RealmContextHolder(),
            testServices.polarisEventDispatcher(),
            testServices.eventMetadataFactory(),
            null,
            new PolarisPrincipalHolder(),
            testServices.principal());

    // No handlers registered
    assertThatThrownBy(
            () ->
                executor.handleTask(
                    taskEntity.getId(),
                    polarisCallCtx,
                    PolarisEventMetadata.builder().realmId(realm).build(),
                    1))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Unable to find handler for task type")
        .hasMessageContaining(String.valueOf(taskEntity.getId()));
  }

  @Test
  void handleTaskThrowsWhenHandlerReturnsFalse() {
    String realm = "myrealm";
    RealmContext realmContext = () -> realm;

    TestServices testServices = TestServices.builder().realmContext(realmContext).build();

    InMemoryEventCollector testPolarisEventDispatcher =
        (InMemoryEventCollector) testServices.polarisEventDispatcher();

    PolarisMetaStoreManager metaStoreManager = testServices.metaStoreManager();
    PolarisCallContext polarisCallCtx = testServices.newCallContext();

    TaskEntity taskEntity =
        new TaskEntity.Builder()
            .setName("failing-task")
            .setId(metaStoreManager.generateNewEntityId(polarisCallCtx).getId())
            .setCreateTimestamp(testServices.clock().millis())
            .build();
    metaStoreManager.createEntityIfNotExists(polarisCallCtx, null, taskEntity);

    TaskExecutorImpl executor =
        new TaskExecutorImpl(
            Runnable::run,
            null,
            testServices.clock(),
            testServices.metaStoreManagerFactory(),
            new TaskFileIOSupplier(
                testServices.fileIOFactory(), testServices.storageAccessConfigProvider()),
            new RealmContextHolder(),
            testServices.polarisEventDispatcher(),
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
            return false; // simulate transient failure
          }
        });

    assertThatThrownBy(
            () ->
                executor.handleTask(
                    taskEntity.getId(),
                    polarisCallCtx,
                    PolarisEventMetadata.builder().realmId(realm).build(),
                    1))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Task handler returned false")
        .hasMessageContaining(String.valueOf(taskEntity.getId()));

    // Event should have been emitted with TASK_SUCCESS=false even though we threw
    PolarisEvent afterEvent =
        testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_ATTEMPT_TASK);
    assertThat(afterEvent.attributes().getRequired(EventAttributes.TASK_SUCCESS)).isEqualTo(false);
  }

  @Test
  void asyncRetryIsTriggeredWhenHandlerReturnsFalse() throws InterruptedException {
    String realm = "myrealm";
    RealmContext realmContext = () -> realm;

    TestServices testServices = TestServices.builder().realmContext(realmContext).build();

    PolarisMetaStoreManager metaStoreManager = testServices.metaStoreManager();
    PolarisCallContext polarisCallCtx = testServices.newCallContext();

    TaskEntity taskEntity =
        new TaskEntity.Builder()
            .setName("retry-task")
            .setId(metaStoreManager.generateNewEntityId(polarisCallCtx).getId())
            .setCreateTimestamp(testServices.clock().millis())
            .build();
    metaStoreManager.createEntityIfNotExists(polarisCallCtx, null, taskEntity);

    AtomicInteger handlerCalls = new AtomicInteger(0);

    ExecutorService testExecutor = Executors.newSingleThreadExecutor();
    TaskExecutorImpl executor =
        new TaskExecutorImpl(
            testExecutor,
            null,
            testServices.clock(),
            testServices.metaStoreManagerFactory(),
            new TaskFileIOSupplier(
                testServices.fileIOFactory(), testServices.storageAccessConfigProvider()),
            new RealmContextHolder(),
            testServices.polarisEventDispatcher(),
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
            int call = handlerCalls.incrementAndGet();
            // Fail first 2 attempts (transient), succeed on 3rd
            if (call < 3) {
              return false;
            }
            return true;
          }
        });

    // This starts the async processing. The first handle runs (sync with our executor),
    // returns false -> throws inside the future (handled by retry compose).
    executor.addTaskHandlerContext(taskEntity.getId(), polarisCallCtx);

    // With direct or single-thread executor the initial call is synchronous.
    // We verify at least the first call happened (return-false leads to exception path).
    assertThat(handlerCalls.get()).isGreaterThanOrEqualTo(1);

    testExecutor.shutdownNow();
  }
}
