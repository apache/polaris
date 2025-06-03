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
package org.apache.polaris.core.persistence;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;

/**
 * Integration test for the polaris persistence layer
 *
 * <pre>@TODO
 *   - Update multiple entities in one shot
 *   - Lookup active: test non existent stuff
 *   - Failure to resolve, i.e. something has changed
 *   - better status report
 * </pre>
 *
 * @author bdagevil
 */
public abstract class BasePolarisMetaStoreManagerTest {

  protected final MutableClock timeSource = MutableClock.of(Instant.now(), ZoneOffset.UTC);

  private PolarisTestMetaStoreManager polarisTestMetaStoreManager;

  @BeforeEach
  public void setupPolariMetaStoreManager() {
    this.polarisTestMetaStoreManager = createPolarisTestMetaStoreManager();
  }

  protected abstract PolarisTestMetaStoreManager createPolarisTestMetaStoreManager();

  /** validate that the root catalog was properly constructed */
  @Test
  protected void validateBootstrap() {
    // allocate test driver
    polarisTestMetaStoreManager.validateBootstrap();
  }

  @Test
  protected void testCreateTestCatalog() {
    // allocate test driver
    polarisTestMetaStoreManager.testCreateTestCatalog();
  }

  @Test
  protected void testCreateTestCatalogWithRetry() {
    // allocate test driver
    polarisTestMetaStoreManager.forceRetry();
    polarisTestMetaStoreManager.testCreateTestCatalog();
  }

  @Test
  protected void testBrowse() {
    // allocate test driver
    polarisTestMetaStoreManager.testBrowse();
  }

  @Test
  protected void testCreateEntities() {
    PolarisMetaStoreManager metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    CallContext callCtx =
        CallContext.of(() -> "testRealm", polarisTestMetaStoreManager.polarisCallContext);
    if (CallContext.getCurrentContext() == null) {
      CallContext.setCurrentContext(callCtx);
    }
    TaskEntity task1 = createTask("task1", 100L);
    TaskEntity task2 = createTask("task2", 101L);
    List<PolarisBaseEntity> createdEntities =
        metaStoreManager
            .createEntitiesIfNotExist(
                polarisTestMetaStoreManager.polarisCallContext, null, List.of(task1, task2))
            .getEntities();

    Assertions.assertThat(createdEntities)
        .isNotNull()
        .hasSize(2)
        .extracting(PolarisEntity::toCore)
        .containsExactly(PolarisEntity.toCore(task1), PolarisEntity.toCore(task2));

    List<EntityNameLookupRecord> listedEntities =
        metaStoreManager
            .listEntities(
                polarisTestMetaStoreManager.polarisCallContext,
                null,
                PolarisEntityType.TASK,
                PolarisEntitySubType.NULL_SUBTYPE,
                PageToken.readEverything())
            .getEntities();
    Assertions.assertThat(listedEntities)
        .isNotNull()
        .hasSize(2)
        .containsExactly(
            new EntityNameLookupRecord(
                task1.getCatalogId(),
                task1.getId(),
                task1.getParentId(),
                task1.getName(),
                task1.getTypeCode(),
                task1.getSubTypeCode()),
            new EntityNameLookupRecord(
                task2.getCatalogId(),
                task2.getId(),
                task2.getParentId(),
                task2.getName(),
                task2.getTypeCode(),
                task2.getSubTypeCode()));
  }

  @Test
  protected void testCreateEntitiesAlreadyExisting() {
    PolarisMetaStoreManager metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    CallContext callCtx =
        CallContext.of(() -> "testRealm", polarisTestMetaStoreManager.polarisCallContext);
    if (CallContext.getCurrentContext() == null) {
      CallContext.setCurrentContext(callCtx);
    }
    TaskEntity task1 = createTask("task1", 100L);
    TaskEntity task2 = createTask("task2", 101L);
    List<PolarisBaseEntity> createdEntities =
        metaStoreManager
            .createEntitiesIfNotExist(
                polarisTestMetaStoreManager.polarisCallContext, null, List.of(task1, task2))
            .getEntities();

    Assertions.assertThat(createdEntities)
        .isNotNull()
        .hasSize(2)
        .extracting(PolarisEntity::toCore)
        .containsExactly(PolarisEntity.toCore(task1), PolarisEntity.toCore(task2));

    TaskEntity task3 = createTask("task3", 103L);

    // entities task1 and task2 already exist with the same identifier, so the full list is
    // returned
    createdEntities =
        metaStoreManager
            .createEntitiesIfNotExist(
                polarisTestMetaStoreManager.polarisCallContext, null, List.of(task1, task2, task3))
            .getEntities();
    Assertions.assertThat(createdEntities)
        .isNotNull()
        .hasSize(3)
        .extracting(PolarisEntity::toCore)
        .containsExactly(
            PolarisEntity.toCore(task1), PolarisEntity.toCore(task2), PolarisEntity.toCore(task3));
  }

  @Test
  protected void testCreateEntitiesWithConflict() {
    PolarisMetaStoreManager metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    CallContext callCtx =
        CallContext.of(() -> "testRealm", polarisTestMetaStoreManager.polarisCallContext);
    if (CallContext.getCurrentContext() == null) {
      CallContext.setCurrentContext(callCtx);
    }
    TaskEntity task1 = createTask("task1", 100L);
    TaskEntity task2 = createTask("task2", 101L);
    TaskEntity task3 = createTask("task3", 103L);
    List<PolarisBaseEntity> createdEntities =
        metaStoreManager
            .createEntitiesIfNotExist(
                polarisTestMetaStoreManager.polarisCallContext, null, List.of(task1, task2, task3))
            .getEntities();

    Assertions.assertThat(createdEntities)
        .isNotNull()
        .hasSize(3)
        .extracting(PolarisEntity::toCore)
        .containsExactly(
            PolarisEntity.toCore(task1), PolarisEntity.toCore(task2), PolarisEntity.toCore(task3));

    TaskEntity secondTask3 = createTask("task3", 104L);

    TaskEntity task4 = createTask("task4", 105L);
    createdEntities =
        metaStoreManager
            .createEntitiesIfNotExist(
                polarisTestMetaStoreManager.polarisCallContext, null, List.of(secondTask3, task4))
            .getEntities();
    Assertions.assertThat(createdEntities).isNull();
  }

  private static TaskEntity createTask(String taskName, long id) {
    return new TaskEntity.Builder()
        .setName(taskName)
        .withData("data")
        .setId(id)
        .withTaskType(AsyncTaskType.MANIFEST_FILE_CLEANUP)
        .setCreateTimestamp(Instant.now().toEpochMilli())
        .build();
  }

  /** Test that entity updates works well */
  @Test
  protected void testUpdateEntities() {
    // allocate test driver
    polarisTestMetaStoreManager.testUpdateEntities();
  }

  /** Test that entity drop works well */
  @Test
  protected void testDropEntities() {
    // allocate test driver
    polarisTestMetaStoreManager.testDropEntities();
  }

  /** Test that granting/revoking privileges works well */
  @Test
  protected void testPrivileges() {
    // allocate test driver
    polarisTestMetaStoreManager.testPrivileges();
  }

  /** test entity rename */
  @Test
  protected void testRename() {
    // allocate test driver
    polarisTestMetaStoreManager.testRename();
  }

  /** test entity lookup */
  @Test
  protected void testLookup() {
    polarisTestMetaStoreManager.testLookup();
  }

  /** Test the set of functions for the entity cache */
  @Test
  protected void testEntityCache() {
    // allocate test driver
    polarisTestMetaStoreManager.testEntityCache();
  }

  /** Test that attaching/detaching policies works well */
  @Test
  protected void testPolicyMapping() {
    polarisTestMetaStoreManager.testPolicyMapping();
  }

  @Test
  protected void testPolicyMappingCleanup() {
    polarisTestMetaStoreManager.testPolicyMappingCleanup();
  }

  @Test
  protected void testLoadTasks() {
    for (int i = 0; i < 20; i++) {
      polarisTestMetaStoreManager.createEntity(
          null, PolarisEntityType.TASK, PolarisEntitySubType.NULL_SUBTYPE, "task_" + i);
    }
    String executorId = "testExecutor_abc";
    PolarisMetaStoreManager metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    PolarisCallContext callCtx = polarisTestMetaStoreManager.polarisCallContext;
    List<PolarisBaseEntity> taskList =
        metaStoreManager.loadTasks(callCtx, executorId, PageToken.fromLimit(5)).getEntities();
    Assertions.assertThat(taskList)
        .isNotNull()
        .isNotEmpty()
        .hasSize(5)
        .allSatisfy(
            entry ->
                Assertions.assertThat(entry)
                    .extracting(
                        e ->
                            PolarisObjectMapperUtil.deserializeProperties(
                                callCtx, e.getProperties()))
                    .asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
                    .containsEntry("lastAttemptExecutorId", executorId)
                    .containsEntry("attemptCount", "1"));
    Set<String> firstTasks =
        taskList.stream().map(PolarisBaseEntity::getName).collect(Collectors.toSet());

    // grab a second round of tasks. Assert that none of the original 5 are in the list
    List<PolarisBaseEntity> newTaskList =
        metaStoreManager.loadTasks(callCtx, executorId, PageToken.fromLimit(5)).getEntities();
    Assertions.assertThat(newTaskList)
        .isNotNull()
        .isNotEmpty()
        .hasSize(5)
        .extracting(PolarisBaseEntity::getName)
        .noneMatch(firstTasks::contains);

    Set<String> firstTenTaskNames =
        Stream.concat(firstTasks.stream(), newTaskList.stream().map(PolarisBaseEntity::getName))
            .collect(Collectors.toSet());

    // only 10 tasks are unassigned. Requesting 20, we should only receive those 10
    List<PolarisBaseEntity> lastTen =
        metaStoreManager.loadTasks(callCtx, executorId, PageToken.fromLimit(20)).getEntities();

    Assertions.assertThat(lastTen)
        .isNotNull()
        .isNotEmpty()
        .hasSize(10)
        .extracting(PolarisBaseEntity::getName)
        .noneMatch(firstTenTaskNames::contains);

    Set<String> allTaskNames =
        Stream.concat(firstTenTaskNames.stream(), lastTen.stream().map(PolarisBaseEntity::getName))
            .collect(Collectors.toSet());

    List<PolarisBaseEntity> emtpyList =
        metaStoreManager.loadTasks(callCtx, executorId, PageToken.fromLimit(20)).getEntities();

    Assertions.assertThat(emtpyList).isNotNull().isEmpty();

    timeSource.add(Duration.ofMinutes(10));

    // all the tasks are unassigned. Fetch them all
    List<PolarisBaseEntity> allTasks =
        metaStoreManager.loadTasks(callCtx, executorId, PageToken.fromLimit(20)).getEntities();

    Assertions.assertThat(allTasks)
        .isNotNull()
        .isNotEmpty()
        .hasSize(20)
        .extracting(PolarisBaseEntity::getName)
        .allMatch(allTaskNames::contains);

    // drop all the tasks. Skip the clock forward and fetch. empty list expected
    allTasks.forEach(
        entity -> metaStoreManager.dropEntityIfExists(callCtx, null, entity, Map.of(), false));
    timeSource.add(Duration.ofMinutes(10));

    List<PolarisBaseEntity> finalList =
        metaStoreManager.loadTasks(callCtx, executorId, PageToken.fromLimit(20)).getEntities();

    Assertions.assertThat(finalList).isNotNull().isEmpty();
  }

  @Test
  protected void testLoadTasksInParallel() throws Exception {
    for (int i = 0; i < 100; i++) {
      polarisTestMetaStoreManager.createEntity(
          null, PolarisEntityType.TASK, PolarisEntitySubType.NULL_SUBTYPE, "task_" + i);
    }
    PolarisMetaStoreManager metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    PolarisCallContext callCtx = polarisTestMetaStoreManager.polarisCallContext;
    List<Future<Set<String>>> futureList = new ArrayList<>();
    ExecutorService executorService = Executors.newCachedThreadPool();
    try {
      for (int i = 0; i < 3; i++) {
        final String executorId = "taskExecutor_" + i;

        futureList.add(
            executorService.submit(
                () -> {
                  Set<String> taskNames = new HashSet<>();
                  List<PolarisBaseEntity> taskList = List.of();
                  boolean retry = false;
                  do {
                    retry = false;
                    try {
                      taskList =
                          metaStoreManager
                              .loadTasks(callCtx, executorId, PageToken.fromLimit(5))
                              .getEntities();
                      taskList.stream().map(PolarisBaseEntity::getName).forEach(taskNames::add);
                    } catch (RetryOnConcurrencyException e) {
                      retry = true;
                    }
                  } while (retry || !taskList.isEmpty());
                  return taskNames;
                }));
      }
    } finally {
      executorService.shutdown();
      Assertions.assertThat(executorService.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }
    List<Set<String>> responses =
        futureList.stream()
            .map(
                f -> {
                  try {
                    return f.get(30, TimeUnit.SECONDS);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    Assertions.assertThat(responses)
        .hasSize(3)
        .satisfies(l -> Assertions.assertThat(l.stream().flatMap(Set::stream)).hasSize(100));
    Map<String, Integer> taskCounts =
        responses.stream()
            .flatMap(Set::stream)
            .collect(Collectors.toMap(Function.identity(), (val) -> 1, Integer::sum));
    Assertions.assertThat(taskCounts)
        .hasSize(100)
        .allSatisfy((k, v) -> Assertions.assertThat(v).isEqualTo(1));
  }

  /** Test generateNewEntityId() function that generates unique ids by creating Tasks in parallel */
  @Test
  protected void testCreateTasksInParallel() throws Exception {
    List<Future<List<Long>>> futureList = new ArrayList<>();
    Random rand = new Random();
    ExecutorService executorService = Executors.newCachedThreadPool();
    try {
      for (int threadId = 0; threadId < 10; threadId++) {
        Future<List<Long>> future =
            executorService.submit(
                () -> {
                  List<Long> list = new ArrayList<>();
                  for (int i = 0; i < 10; i++) {
                    var entity =
                        polarisTestMetaStoreManager.createEntity(
                            null,
                            PolarisEntityType.TASK,
                            PolarisEntitySubType.NULL_SUBTYPE,
                            "task_" + rand.nextLong() + i);
                    list.add(entity.getId());
                  }
                  return list;
                });
        futureList.add(future);
      }

      List<List<Long>> responses =
          futureList.stream()
              .map(
                  f -> {
                    try {
                      return f.get();
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  })
              .collect(Collectors.toList());

      Assertions.assertThat(responses)
          .hasSize(10)
          .satisfies(l -> Assertions.assertThat(l.stream().flatMap(List::stream)).hasSize(100));
      Map<Long, Integer> idCounts =
          responses.stream()
              .flatMap(List::stream)
              .collect(Collectors.toMap(Function.identity(), (val) -> 1, Integer::sum));
      Assertions.assertThat(idCounts)
          .hasSize(100)
          .allSatisfy((k, v) -> Assertions.assertThat(v).isEqualTo(1));
    } finally {
      executorService.shutdown();
      Assertions.assertThat(executorService.awaitTermination(10, TimeUnit.MINUTES)).isTrue();
    }
  }
}
