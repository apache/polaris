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
import java.util.concurrent.CyclicBarrier;
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
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ConditionalEntityWrite;
import org.apache.polaris.core.persistence.dao.entity.ConditionalEntityWriteResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
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

  /** test entity rename */
  @Test
  protected void testRename() {
    // allocate test driver
    polarisTestMetaStoreManager.testRename();
  }

  /** test that a rename cannot move an entity into a different catalog */
  @Test
  protected void testRenameAcrossCatalogsIsRejected() {
    polarisTestMetaStoreManager.testRenameAcrossCatalogsIsRejected();
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

  // ---------------------------------------------------------------------------------------------
  // Batch updates: updateEntitiesPropertiesIfNotChanged (apache/polaris#5807)
  // ---------------------------------------------------------------------------------------------

  /** A catalog, a namespace and two tables {@code a} and {@code b}, for the batch-update tests. */
  protected record TwoTables(
      List<PolarisEntityCore> path, PolarisBaseEntity a, PolarisBaseEntity b) {}

  protected TwoTables createTwoTables(String catalogName) {
    PolarisMetaStoreManager metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager;
    PolarisCallContext callCtx = polarisTestMetaStoreManager.polarisCallContext;
    PolarisBaseEntity catalog =
        metaStoreManager
            .createCatalog(
                callCtx,
                new PolarisBaseEntity(
                    PolarisEntityConstants.getNullId(),
                    metaStoreManager.generateNewEntityId(callCtx).getId(),
                    PolarisEntityType.CATALOG,
                    PolarisEntitySubType.NULL_SUBTYPE,
                    PolarisEntityConstants.getRootEntityId(),
                    catalogName),
                List.of())
            .getCatalog();
    PolarisBaseEntity namespace =
        polarisTestMetaStoreManager.createEntity(
            List.of(catalog), PolarisEntityType.NAMESPACE, PolarisEntitySubType.NULL_SUBTYPE, "ns");
    List<PolarisEntityCore> path = List.of(catalog, namespace);
    return new TwoTables(
        path,
        polarisTestMetaStoreManager.createEntity(
            path, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE, "a"),
        polarisTestMetaStoreManager.createEntity(
            path, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE, "b"));
  }

  protected PolarisBaseEntity reload(PolarisBaseEntity entity) {
    return polarisTestMetaStoreManager
        .polarisMetaStoreManager
        .loadEntity(
            polarisTestMetaStoreManager.polarisCallContext,
            entity.getCatalogId(),
            entity.getId(),
            entity.getType())
        .getEntity();
  }

  /** The entity as the caller read it, with a property changed to the given value. */
  protected static PolarisBaseEntity withProperty(PolarisBaseEntity entity, String value) {
    return new PolarisBaseEntity.Builder(entity).properties("{\"k\":\"" + value + "\"}").build();
  }

  private EntitiesResult updateBatch(List<PolarisEntityCore> path, PolarisBaseEntity... entities) {
    return polarisTestMetaStoreManager.polarisMetaStoreManager.updateEntitiesPropertiesIfNotChanged(
        polarisTestMetaStoreManager.polarisCallContext,
        Stream.of(entities).map(e -> new EntityWithPath(path, e)).toList());
  }

  /**
   * A batch is all-or-nothing: when one member was modified after it was read, no member is
   * updated, whichever position the stale member has in the batch.
   */
  @Test
  protected void testUpdateEntitiesIsAllOrNothing() {
    for (boolean staleFirst : List.of(true, false)) {
      TwoTables tables = createTwoTables("batch_atomic_" + staleFirst);
      // A concurrent writer updates b after both tables were read.
      Assertions.assertThat(
              polarisTestMetaStoreManager
                  .polarisMetaStoreManager
                  .updateEntityPropertiesIfNotChanged(
                      polarisTestMetaStoreManager.polarisCallContext,
                      tables.path(),
                      withProperty(tables.b(), "concurrent"))
                  .isSuccess())
          .isTrue();

      PolarisBaseEntity updatedA = withProperty(tables.a(), "batch");
      PolarisBaseEntity staleB = withProperty(tables.b(), "batch");
      EntitiesResult result =
          staleFirst
              ? updateBatch(tables.path(), staleB, updatedA)
              : updateBatch(tables.path(), updatedA, staleB);

      Assertions.assertThat(result.isSuccess()).isFalse();
      PolarisBaseEntity a = reload(tables.a());
      Assertions.assertThat(a.getEntityVersion()).isEqualTo(tables.a().getEntityVersion());
      Assertions.assertThat(a.getPropertiesAsMap()).isEqualTo(tables.a().getPropertiesAsMap());
    }
  }

  /**
   * Two batches that update the same two entities in opposite order, at the same time. They
   * conflict on the same versions, so exactly one succeeds and the other reports a concurrent
   * modification. Neither may fail with an exception, for example a database deadlock.
   */
  @Test
  protected void testCrossedBatchUpdatesAreConflictsNotErrors() throws Exception {
    TwoTables tables = createTwoTables("batch_crossed");
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 50; round++) {
        PolarisBaseEntity a = reload(tables.a());
        PolarisBaseEntity b = reload(tables.b());
        CyclicBarrier start = new CyclicBarrier(2);
        String r = Integer.toString(round);
        Future<EntitiesResult> ab =
            executor.submit(
                () -> {
                  start.await();
                  return updateBatch(
                      tables.path(), withProperty(a, r + "ab"), withProperty(b, r + "ab"));
                });
        Future<EntitiesResult> ba =
            executor.submit(
                () -> {
                  start.await();
                  return updateBatch(
                      tables.path(), withProperty(b, r + "ba"), withProperty(a, r + "ba"));
                });

        int successes = 0;
        for (Future<EntitiesResult> future : List.of(ab, ba)) {
          // An exception from the batch (such as a deadlock) fails the test here.
          EntitiesResult result = future.get(30, TimeUnit.SECONDS);
          if (result.isSuccess()) {
            successes++;
          } else {
            Assertions.assertThat(result.getReturnStatus())
                .isEqualTo(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED);
          }
        }
        Assertions.assertThat(successes).as("successful batches in round %s", round).isEqualTo(1);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Atomic conditional batches: writeEntitiesIfConditionsHold (apache/polaris#5807)
  // ---------------------------------------------------------------------------------------------

  private ConditionalEntityWriteResult writeBatch(ConditionalEntityWrite batch) {
    return polarisTestMetaStoreManager.polarisMetaStoreManager.writeEntitiesIfConditionsHold(
        polarisTestMetaStoreManager.polarisCallContext, batch);
  }

  private void assertConcurrentlyModified(
      ConditionalEntityWriteResult result, PolarisBaseEntity entity) {
    Assertions.assertThat(result.isSuccess()).isFalse();
    Assertions.assertThat(result.getReturnStatus())
        .isEqualTo(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED);
    Assertions.assertThat(result.getFailedEntity()).isNotNull();
    Assertions.assertThat(result.getFailedEntity().getId()).isEqualTo(entity.getId());
  }

  /** A checked entity is not written: its version, timestamp and properties stay as they were. */
  @Test
  protected void testBatchDoesNotWriteUnchangedEntity() {
    TwoTables tables = createTwoTables("batch_unchanged_ok");

    ConditionalEntityWriteResult result =
        writeBatch(
            ConditionalEntityWrite.builder()
                .update(tables.path(), withProperty(tables.a(), "batch"))
                .unchanged(tables.path(), tables.b())
                .build());

    Assertions.assertThat(result.isSuccess()).isTrue();
    Assertions.assertThat(result.getEntities())
        .extracting(PolarisBaseEntity::getId)
        .containsExactly(tables.a().getId());
    PolarisBaseEntity a = reload(tables.a());
    Assertions.assertThat(a.getEntityVersion()).isEqualTo(tables.a().getEntityVersion() + 1);
    Assertions.assertThat(a.getPropertiesAsMap()).isEqualTo(Map.of("k", "batch"));
    PolarisBaseEntity b = reload(tables.b());
    Assertions.assertThat(b.getEntityVersion()).isEqualTo(tables.b().getEntityVersion());
    Assertions.assertThat(b.getLastUpdateTimestamp())
        .isEqualTo(tables.b().getLastUpdateTimestamp());
    Assertions.assertThat(b.getPropertiesAsMap()).isEqualTo(tables.b().getPropertiesAsMap());
  }

  /** A batch with only conditions succeeds when they hold, and writes nothing. */
  @Test
  protected void testBatchOfOnlyConditionsWritesNothing() {
    TwoTables tables = createTwoTables("batch_only_conditions");

    ConditionalEntityWriteResult result =
        writeBatch(
            ConditionalEntityWrite.builder()
                .unchanged(tables.path(), tables.a())
                .unchanged(tables.path(), tables.b())
                .build());

    Assertions.assertThat(result.isSuccess()).isTrue();
    Assertions.assertThat(result.getEntities()).isEmpty();
    Assertions.assertThat(reload(tables.a()).getEntityVersion())
        .isEqualTo(tables.a().getEntityVersion());
    Assertions.assertThat(reload(tables.b()).getEntityVersion())
        .isEqualTo(tables.b().getEntityVersion());
  }

  /** A checked entity that was modified after it was read fails the batch; nothing is written. */
  @Test
  protected void testBatchFailsWhenCheckedEntityWasModified() {
    TwoTables tables = createTwoTables("batch_unchanged_moved");
    // A concurrent writer updates b after it was read.
    Assertions.assertThat(
            polarisTestMetaStoreManager
                .polarisMetaStoreManager
                .updateEntityPropertiesIfNotChanged(
                    polarisTestMetaStoreManager.polarisCallContext,
                    tables.path(),
                    withProperty(tables.b(), "concurrent"))
                .isSuccess())
        .isTrue();

    ConditionalEntityWriteResult result =
        writeBatch(
            ConditionalEntityWrite.builder()
                .update(tables.path(), withProperty(tables.a(), "batch"))
                .unchanged(tables.path(), tables.b())
                .build());

    assertConcurrentlyModified(result, tables.b());
    PolarisBaseEntity a = reload(tables.a());
    Assertions.assertThat(a.getEntityVersion()).isEqualTo(tables.a().getEntityVersion());
    Assertions.assertThat(a.getPropertiesAsMap()).isEqualTo(tables.a().getPropertiesAsMap());
  }

  /** A checked entity that was dropped fails the batch with the same status. */
  @Test
  protected void testBatchFailsWhenCheckedEntityWasDropped() {
    TwoTables tables = createTwoTables("batch_unchanged_dropped");
    Assertions.assertThat(
            polarisTestMetaStoreManager
                .polarisMetaStoreManager
                .dropEntityIfExists(
                    polarisTestMetaStoreManager.polarisCallContext,
                    tables.path(),
                    tables.b(),
                    null,
                    false)
                .isSuccess())
        .isTrue();

    ConditionalEntityWriteResult result =
        writeBatch(
            ConditionalEntityWrite.builder()
                .update(tables.path(), withProperty(tables.a(), "batch"))
                .unchanged(tables.path(), tables.b())
                .build());

    assertConcurrentlyModified(result, tables.b());
    Assertions.assertThat(reload(tables.a()).getEntityVersion())
        .isEqualTo(tables.a().getEntityVersion());
  }

  /** An updated entity that was dropped after it was read fails the write; nothing is written. */
  @Test
  protected void testWriteFailsWhenUpdatedEntityWasDropped() {
    TwoTables tables = createTwoTables("write_updated_dropped");
    Assertions.assertThat(
            polarisTestMetaStoreManager
                .polarisMetaStoreManager
                .dropEntityIfExists(
                    polarisTestMetaStoreManager.polarisCallContext,
                    tables.path(),
                    tables.b(),
                    null,
                    false)
                .isSuccess())
        .isTrue();

    ConditionalEntityWriteResult result =
        writeBatch(
            ConditionalEntityWrite.builder()
                .update(tables.path(), withProperty(tables.a(), "batch"))
                .update(tables.path(), withProperty(tables.b(), "batch"))
                .build());

    Assertions.assertThat(result.isSuccess()).isFalse();
    Assertions.assertThat(result.getReturnStatus())
        .isEqualTo(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED);
    PolarisBaseEntity a = reload(tables.a());
    Assertions.assertThat(a.getEntityVersion()).isEqualTo(tables.a().getEntityVersion());
    Assertions.assertThat(a.getPropertiesAsMap()).isEqualTo(tables.a().getPropertiesAsMap());
  }

  /** A write without conditions and without writes succeeds and does nothing. */
  @Test
  protected void testEmptyWriteSucceedsAndWritesNothing() {
    ConditionalEntityWriteResult result = writeBatch(ConditionalEntityWrite.builder().build());

    Assertions.assertThat(result.isSuccess()).isTrue();
    Assertions.assertThat(result.getEntities()).isEmpty();
  }

  /**
   * A change of the grant records of a checked entity does not fail the batch: grants do not change
   * the entity itself.
   */
  @Test
  protected void testBatchIgnoresGrantChangesOfCheckedEntity() {
    TwoTables tables = createTwoTables("batch_unchanged_grants");
    PolarisBaseEntity role =
        polarisTestMetaStoreManager.createEntity(
            List.of(tables.path().get(0)),
            PolarisEntityType.CATALOG_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            "role");
    polarisTestMetaStoreManager.polarisMetaStoreManager.grantPrivilegeOnSecurableToRole(
        polarisTestMetaStoreManager.polarisCallContext,
        role,
        tables.path(),
        tables.b(),
        PolarisPrivilege.TABLE_READ_DATA);

    ConditionalEntityWriteResult result =
        writeBatch(
            ConditionalEntityWrite.builder()
                .update(tables.path(), withProperty(tables.a(), "batch"))
                .unchanged(tables.path(), tables.b())
                .build());

    Assertions.assertThat(result.isSuccess()).isTrue();
  }

  /**
   * Write skew: two batches each update one entity and require the other to be unchanged. They
   * invalidate each other's condition, so exactly one of them can be applied.
   */
  @Test
  protected void testBatchesThatInvalidateEachOthersConditionsHaveOneWinner() throws Exception {
    TwoTables tables = createTwoTables("batch_write_skew");
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 50; round++) {
        PolarisBaseEntity a = reload(tables.a());
        PolarisBaseEntity b = reload(tables.b());
        CyclicBarrier start = new CyclicBarrier(2);
        String r = Integer.toString(round);
        Future<ConditionalEntityWriteResult> updatesA =
            executor.submit(
                () -> {
                  start.await();
                  return writeBatch(
                      ConditionalEntityWrite.builder()
                          .update(tables.path(), withProperty(a, r + "a"))
                          .unchanged(tables.path(), b)
                          .build());
                });
        Future<ConditionalEntityWriteResult> updatesB =
            executor.submit(
                () -> {
                  start.await();
                  return writeBatch(
                      ConditionalEntityWrite.builder()
                          .update(tables.path(), withProperty(b, r + "b"))
                          .unchanged(tables.path(), a)
                          .build());
                });

        int successes = 0;
        for (Future<ConditionalEntityWriteResult> future : List.of(updatesA, updatesB)) {
          ConditionalEntityWriteResult result = future.get(30, TimeUnit.SECONDS);
          if (result.isSuccess()) {
            successes++;
          } else {
            Assertions.assertThat(result.getReturnStatus())
                .isEqualTo(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED);
          }
        }
        Assertions.assertThat(successes).as("applied batches in round %s", round).isEqualTo(1);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  /** Batches that only check the same entity do not conflict with each other. */
  @Test
  protected void testBatchesThatOnlyCheckTheSameEntityDoNotConflict() throws Exception {
    TwoTables tables = createTwoTables("batch_shared_check");
    PolarisBaseEntity c =
        polarisTestMetaStoreManager.createEntity(
            tables.path(), PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE, "c");
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 50; round++) {
        PolarisBaseEntity a = reload(tables.a());
        PolarisBaseEntity b = reload(tables.b());
        CyclicBarrier start = new CyclicBarrier(2);
        String r = Integer.toString(round);
        Future<ConditionalEntityWriteResult> updatesA =
            executor.submit(
                () -> {
                  start.await();
                  return writeBatch(
                      ConditionalEntityWrite.builder()
                          .update(tables.path(), withProperty(a, r))
                          .unchanged(tables.path(), c)
                          .build());
                });
        Future<ConditionalEntityWriteResult> updatesB =
            executor.submit(
                () -> {
                  start.await();
                  return writeBatch(
                      ConditionalEntityWrite.builder()
                          .update(tables.path(), withProperty(b, r))
                          .unchanged(tables.path(), c)
                          .build());
                });

        Assertions.assertThat(updatesA.get(30, TimeUnit.SECONDS).isSuccess())
            .as("batch updating a in round %s", round)
            .isTrue();
        Assertions.assertThat(updatesB.get(30, TimeUnit.SECONDS).isSuccess())
            .as("batch updating b in round %s", round)
            .isTrue();
      }
      Assertions.assertThat(reload(c).getEntityVersion()).isEqualTo(c.getEntityVersion());
    } finally {
      executor.shutdownNow();
    }
  }
}
