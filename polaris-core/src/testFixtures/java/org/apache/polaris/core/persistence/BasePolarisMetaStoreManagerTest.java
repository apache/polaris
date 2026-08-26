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
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
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

  protected final MutableClock clock = MutableClock.of(Instant.now(), ZoneOffset.UTC);

  protected PolarisTestMetaStoreManager polarisTestMetaStoreManager;

  @BeforeEach
  public void setupPolarisMetaStoreManager() {
    this.polarisTestMetaStoreManager = createPolarisTestMetaStoreManager();
  }

  protected abstract PolarisTestMetaStoreManager createPolarisTestMetaStoreManager();

  /** validate that the root catalog was properly constructed */
  @Test
  protected void validateBootstrap() {
    // allocate test driver
    polarisTestMetaStoreManager.validateBootstrap();
  }

  /** re-bootstrapping an already-bootstrapped service must be a graceful no-op */
  @Test
  protected void testBootstrapAgainIsNoOp() {
    PolarisMetaStoreManager metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    // the test driver bootstrapped the service already; bootstrap a second time
    BaseResult secondBootstrap =
        metaStoreManager.bootstrapPolarisService(polarisTestMetaStoreManager.polarisCallContext);
    Assertions.assertThat(secondBootstrap.getReturnStatus())
        .isEqualTo(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS);
    // the existing realm must be left fully intact
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

    List<PolarisBaseEntity> listedEntities =
        metaStoreManager.listFullEntitiesAll(
            polarisTestMetaStoreManager.polarisCallContext,
            null,
            PolarisEntityType.TASK,
            PolarisEntitySubType.NULL_SUBTYPE);
    Assertions.assertThat(listedEntities)
        .isNotNull()
        .hasSize(2)
        .extracting(PolarisEntity::toCore)
        .containsExactlyInAnyOrder(PolarisEntity.toCore(task1), PolarisEntity.toCore(task2));

    Assertions.assertThat(createdEntities).containsExactlyInAnyOrderElementsOf(listedEntities);
  }

  @Test
  protected void testCreatePrincipalReturnedEntitySameAsPersisted() {
    PolarisMetaStoreManager metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    PolarisCallContext callCtx = polarisTestMetaStoreManager.polarisCallContext;
    PolarisBaseEntity principalEntity =
        metaStoreManager
            .createPrincipal(
                callCtx,
                new PrincipalEntity.Builder()
                    .setId(metaStoreManager.generateNewEntityId(callCtx).getId())
                    .setName("principal_test")
                    .setCreateTimestamp(100L)
                    .build())
            .getPrincipal();

    Assertions.assertThat(principalEntity)
        .isNotNull()
        .extracting(PolarisBaseEntity::getName)
        .isEqualTo("principal_test");
    Assertions.assertThat(principalEntity)
        .extracting(PolarisBaseEntity::getCreateTimestamp)
        .isEqualTo(100L);

    PolarisBaseEntity fetchedPrincipal =
        metaStoreManager
            .readEntityByName(
                callCtx,
                null,
                PolarisEntityType.PRINCIPAL,
                PolarisEntitySubType.NULL_SUBTYPE,
                "principal_test")
            .getEntity();

    Assertions.assertThat(principalEntity).isEqualTo(fetchedPrincipal);
  }

  @Test
  protected void testCreateEntitiesAlreadyExisting() {
    PolarisMetaStoreManager metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager;
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

  /** Test that duplicate grant writes are idempotent */
  @Test
  protected void testGrantRecordWriteIsIdempotent() {
    polarisTestMetaStoreManager.testGrantRecordWriteIsIdempotent();
  }

  @Test
  protected void testCommitTransactionBatchCreatesEntityAndGrant() {
    PolarisMetaStoreManager manager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    PolarisCallContext callCtx = polarisTestMetaStoreManager.polarisCallContext;
    TaskEntity task = createTask("changeset-create", 900L);
    PolarisGrantRecord grant = new PolarisGrantRecord(0L, task.getId(), 0L, 1L, 3);
    EntitiesResult result =
        manager.commitTransactionBatch(
            callCtx,
            new MetaStoreChangeSet.Builder()
                .addCreate(null, task)
                .addGrantRecordCreate(grant)
                .build());
    Assumptions.assumeThat(result.getReturnStatus())
        .isNotEqualTo(BaseResult.ReturnStatus.CHANGE_SET_ATOMICITY_UNSUPPORTED);
    Assertions.assertThat(result.isSuccess()).isTrue();
    Assertions.assertThat(
            manager
                .loadEntity(callCtx, task.getCatalogId(), task.getId(), task.getType())
                .getEntity())
        .isNotNull()
        .extracting(PolarisBaseEntity::getName)
        .isEqualTo("changeset-create");
    Assertions.assertThat(
            callCtx.getMetaStore().lookupGrantRecord(callCtx, 0L, task.getId(), 0L, 1L, 3))
        .isNotNull();
  }

  @Test
  protected void testCommitTransactionBatchRollsBackEarlierMutationsOnLaterFailure() {
    PolarisMetaStoreManager manager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    PolarisCallContext callCtx = polarisTestMetaStoreManager.polarisCallContext;
    TaskEntity existing = createTask("changeset-existing", 901L);
    EntitiesResult created =
        manager.commitTransactionBatch(callCtx, MetaStoreChangeSet.ofCreate(null, existing));
    Assumptions.assumeThat(created.getReturnStatus())
        .isNotEqualTo(BaseResult.ReturnStatus.CHANGE_SET_ATOMICITY_UNSUPPORTED);
    Assertions.assertThat(created.isSuccess()).isTrue();

    PolarisBaseEntity persisted =
        manager
            .loadEntity(callCtx, existing.getCatalogId(), existing.getId(), existing.getType())
            .getEntity();
    Assertions.assertThat(persisted).isNotNull();
    // Simulate a concurrent writer so the original CAS baseline is stale. entityVersion 0 is
    // coerced back to 1 by PolarisBaseEntity, so we bump the stored entity instead of subtracting.
    PolarisBaseEntity concurrent =
        new PolarisBaseEntity.Builder(persisted)
            .entityVersion(persisted.getEntityVersion() + 1)
            .build();
    callCtx.getMetaStore().writeEntity(callCtx, concurrent, false, persisted);
    PolarisBaseEntity update =
        new PolarisBaseEntity.Builder(persisted).name("changeset-renamed").build();
    TaskEntity rolledBack = createTask("changeset-rolled-back", 902L);
    PolarisGrantRecord grant = new PolarisGrantRecord(0L, rolledBack.getId(), 0L, 1L, 3);

    EntitiesResult failed =
        manager.commitTransactionBatch(
            callCtx,
            new MetaStoreChangeSet.Builder()
                .addCreate(null, rolledBack)
                .addUpdate(null, update, persisted)
                .addGrantRecordCreate(grant)
                .build());
    Assertions.assertThat(failed.getReturnStatus())
        .isEqualTo(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED);
    Assertions.assertThat(
            manager
                .loadEntity(
                    callCtx, rolledBack.getCatalogId(), rolledBack.getId(), rolledBack.getType())
                .getEntity())
        .isNull();
    Assertions.assertThat(
            callCtx.getMetaStore().lookupGrantRecord(callCtx, 0L, rolledBack.getId(), 0L, 1L, 3))
        .isNull();
    PolarisBaseEntity afterFailure =
        manager
            .loadEntity(callCtx, existing.getCatalogId(), existing.getId(), existing.getType())
            .getEntity();
    Assertions.assertThat(afterFailure.getName()).isEqualTo("changeset-existing");
  }

  @Test
  protected void testCommitTransactionBatchMapsAlreadyExistsConflict() {
    PolarisMetaStoreManager manager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    PolarisCallContext callCtx = polarisTestMetaStoreManager.polarisCallContext;
    TaskEntity task = createTask("changeset-dup", 903L);
    EntitiesResult created =
        manager.commitTransactionBatch(callCtx, MetaStoreChangeSet.ofCreate(null, task));
    Assumptions.assumeThat(created.getReturnStatus())
        .isNotEqualTo(BaseResult.ReturnStatus.CHANGE_SET_ATOMICITY_UNSUPPORTED);
    Assertions.assertThat(created.isSuccess()).isTrue();

    TaskEntity duplicate = createTask("changeset-dup", 904L);
    EntitiesResult conflict =
        manager.commitTransactionBatch(callCtx, MetaStoreChangeSet.ofCreate(null, duplicate));
    Assertions.assertThat(conflict.getReturnStatus())
        .isEqualTo(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS);
  }

  @Test
  protected void testCommitTransactionBatchDuplicateGrantDoesNotAbort() {
    PolarisMetaStoreManager manager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    PolarisCallContext callCtx = polarisTestMetaStoreManager.polarisCallContext;
    TaskEntity task = createTask("changeset-grant-dup", 905L);
    PolarisGrantRecord grant = new PolarisGrantRecord(0L, task.getId(), 0L, 1L, 3);
    EntitiesResult created =
        manager.commitTransactionBatch(
            callCtx,
            new MetaStoreChangeSet.Builder()
                .addCreate(null, task)
                .addGrantRecordCreate(grant)
                .build());
    Assumptions.assumeThat(created.getReturnStatus())
        .isNotEqualTo(BaseResult.ReturnStatus.CHANGE_SET_ATOMICITY_UNSUPPORTED);
    Assertions.assertThat(created.isSuccess()).isTrue();

    PolarisBaseEntity persisted =
        manager.loadEntity(callCtx, task.getCatalogId(), task.getId(), task.getType()).getEntity();
    PolarisBaseEntity update =
        new PolarisBaseEntity.Builder(persisted).name("changeset-grant-dup-updated").build();
    EntitiesResult retried =
        manager.commitTransactionBatch(
            callCtx,
            new MetaStoreChangeSet.Builder()
                .addUpdate(null, update, persisted)
                .addGrantRecordCreate(grant)
                .build());
    Assertions.assertThat(retried.isSuccess()).isTrue();
    PolarisBaseEntity after =
        manager.loadEntity(callCtx, task.getCatalogId(), task.getId(), task.getType()).getEntity();
    Assertions.assertThat(after.getName()).isEqualTo("changeset-grant-dup-updated");
    Assertions.assertThat(
            callCtx.getMetaStore().lookupGrantRecord(callCtx, 0L, task.getId(), 0L, 1L, 3))
        .isNotNull();
  }

  @Test
  protected void testApplyChangeSetBestEffortRejectsGrantMutations() {
    PolarisMetaStoreManager manager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    PolarisCallContext callCtx = polarisTestMetaStoreManager.polarisCallContext;
    PolarisGrantRecord grant = new PolarisGrantRecord(0L, 1L, 0L, 2L, 3);
    EntitiesResult result =
        manager.applyChangeSetBestEffort(
            callCtx, new MetaStoreChangeSet.Builder().addGrantRecordCreate(grant).build());
    Assertions.assertThat(result.getReturnStatus())
        .isEqualTo(BaseResult.ReturnStatus.CHANGE_SET_ATOMICITY_UNSUPPORTED);
  }

  @Test
  protected void testApplyChangeSetBestEffortAppliesCreatesSequentially() {
    PolarisMetaStoreManager manager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    PolarisCallContext callCtx = polarisTestMetaStoreManager.polarisCallContext;
    TaskEntity first = createTask("best-effort-create-1", 906L);
    TaskEntity second = createTask("best-effort-create-2", 907L);

    EntitiesResult result =
        manager.applyChangeSetBestEffort(
            callCtx,
            MetaStoreChangeSet.ofBatch(
                List.of(new EntityWithPath(null, first), new EntityWithPath(null, second)),
                List.of()));
    Assertions.assertThat(result.isSuccess()).isTrue();
    Assertions.assertThat(
            manager
                .loadEntity(callCtx, first.getCatalogId(), first.getId(), first.getType())
                .getEntity())
        .isNotNull();
    Assertions.assertThat(
            manager
                .loadEntity(callCtx, second.getCatalogId(), second.getId(), second.getType())
                .getEntity())
        .isNotNull();
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

  /** test batch entity load */
  @Test
  protected void testLoadResolvedEntitiesById() {
    polarisTestMetaStoreManager.testLoadResolvedEntitiesById();
  }

  /** test that grantee and securable grant records are loaded from the correct store methods */
  @Test
  protected void testLoadResolvedEntitiesGranteeVsSecurableRecords() {
    polarisTestMetaStoreManager.testLoadResolvedEntitiesGranteeVsSecurableRecords();
  }

  /** test that resolved entities do not include grant records referencing dropped grantees */
  @Test
  protected void testLoadResolvedEntitySkipsDroppedGranteeReferences() {
    polarisTestMetaStoreManager.testLoadResolvedEntitySkipsDroppedGranteeReferences();
  }

  /**
   * Test that loadGrantsToGrantee/loadGrantsOnSecurable return only grants where the entity plays
   * the expected role — regression test for entities that are both grantee and securable.
   */
  @Test
  protected void testLoadGrantsGranteeVsSecurableRecords() {
    polarisTestMetaStoreManager.testLoadGrantsGranteeVsSecurableRecords();
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
                        PolarisBaseEntity::getPropertiesAsMap,
                        InstanceOfAssertFactories.map(String.class, String.class))
                    .containsEntry(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID, executorId)
                    .containsEntry(PolarisTaskConstants.ATTEMPT_COUNT, "1"));
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

    clock.add(Duration.ofMinutes(10));

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
    clock.add(Duration.ofMinutes(10));

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

  @Test
  protected void testResetCredentialsClientIdCollision() {
    PolarisMetaStoreManager metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    PolarisCallContext callCtx = polarisTestMetaStoreManager.polarisCallContext;

    PrincipalEntity principalA = polarisTestMetaStoreManager.createPrincipal("principalA");
    PrincipalEntity principalB = polarisTestMetaStoreManager.createPrincipal("principalB");

    String principalAClientId = principalA.getClientId();

    Assertions.assertThatThrownBy(
            () ->
                metaStoreManager.resetPrincipalSecrets(
                    callCtx, principalB.getId(), principalAClientId, null))
        .isInstanceOf(AlreadyExistsException.class);
  }

  /**
   * {@link PolarisEntityConstants#ENTITY_BASE_LOCATION} is an ordinary user-settable property, so
   * entities that are not catalog content may carry it. Those entities are not location-tracked, so
   * creating, updating and dropping them must all succeed regardless.
   */
  @Test
  protected void testBaseLocationPropertyOnEntityThatIsNotCatalogContent() {
    PolarisMetaStoreManager metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    PolarisCallContext callCtx = polarisTestMetaStoreManager.polarisCallContext;
    Map<String, String> properties =
        Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://bucket/path/");

    // creating a principal role that carries a base location must succeed
    PolarisBaseEntity principalRole =
        polarisTestMetaStoreManager.createEntity(
            null,
            PolarisEntityType.PRINCIPAL_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            "principalRoleWithLocation",
            properties);
    Assertions.assertThat(principalRole.getPropertiesAsMap())
        .containsEntry(PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://bucket/path/");

    // a catalog that carries one must remain updatable afterwards
    PolarisBaseEntity catalog =
        new PolarisBaseEntity.Builder()
            .catalogId(PolarisEntityConstants.getNullId())
            .id(metaStoreManager.generateNewEntityId(callCtx).getId())
            .typeCode(PolarisEntityType.CATALOG.getCode())
            .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
            .parentId(PolarisEntityConstants.getRootEntityId())
            .name("catalogWithLocation")
            .propertiesAsMap(properties)
            .internalPropertiesAsMap(Map.of())
            .build();
    catalog = metaStoreManager.createCatalog(callCtx, catalog, List.of()).getCatalog();
    Assertions.assertThat(catalog).isNotNull();
    catalog =
        metaStoreManager
            .loadEntity(callCtx, catalog.getCatalogId(), catalog.getId(), catalog.getType())
            .getEntity();

    PolarisBaseEntity relocatedCatalog =
        new PolarisBaseEntity.Builder(catalog)
            .propertiesAsMap(
                Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://bucket/other/"))
            .build();
    EntityResult updated =
        metaStoreManager.updateEntityPropertiesIfNotChanged(callCtx, null, relocatedCatalog);
    Assertions.assertThat(updated.isSuccess()).isTrue();
    Assertions.assertThat(updated.getEntity().getPropertiesAsMap())
        .containsEntry(PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://bucket/other/");

    // and dropping a location-carrying entity must succeed too
    DropEntityResult dropped =
        metaStoreManager.dropEntityIfExists(callCtx, null, principalRole, null, false);
    Assertions.assertThat(dropped.isSuccess()).isTrue();
  }
}
