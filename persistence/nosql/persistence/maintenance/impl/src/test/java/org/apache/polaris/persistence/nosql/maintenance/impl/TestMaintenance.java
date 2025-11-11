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
package org.apache.polaris.persistence.nosql.maintenance.impl;

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.maintenance.impl.MutableMaintenanceConfig.GRACE_TIME;

import jakarta.inject.Inject;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.ids.api.SnowflakeIdGenerator;
import org.apache.polaris.ids.mocks.MutableMonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.RealmPersistenceFactory;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceConfig;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunInformation.MaintenanceStats;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunSpec;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceService;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@SuppressWarnings("CdiInjectionPointsInspection")
@ExtendWith(SoftAssertionsExtension.class)
@EnableWeld
public class TestMaintenance {
  @InjectSoftAssertions protected SoftAssertions soft;

  @WeldSetup WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  String realmOne;
  String realmTwo;
  Persistence persOne;
  Persistence persTwo;

  @Inject MaintenanceService maintenance;
  @Inject RealmPersistenceFactory realmPersistenceFactory;
  @Inject MutableMonotonicClock mutableMonotonicClock;

  @BeforeEach
  protected void setup() {
    RealmIdentOne.testCallback = c -> true;
    RealmIdentTwo.testCallback = c -> true;
    ObjTypeIdentOne.testCallback = (c, id) -> {};
    ObjTypeIdentTwo.testCallback = (c, id) -> {};

    // Set the "grace time" to 0 so tests can write refs+objs and get those purged
    MutableMaintenanceConfig.setCurrent(
        MaintenanceConfig.builder().createdAtGraceTime(GRACE_TIME).build());

    realmOne = UUID.randomUUID().toString();
    realmTwo = UUID.randomUUID().toString();

    // 'skipDecorators' is used to bypass the cache, which cannot be consistent after maintenance
    // purged some references/objects
    persOne = realmPersistenceFactory.newBuilder().realmId(realmOne).skipDecorators().build();
    persTwo = realmPersistenceFactory.newBuilder().realmId(realmTwo).skipDecorators().build();
  }

  @AfterEach
  protected void cleanup() {
    mutableMonotonicClock.advanceBoth(GRACE_TIME);

    // Use maintenance to clean the backend for the next test
    maintenance.performMaintenance(
        MaintenanceRunSpec.builder()
            .includeSystemRealm(false)
            .realmsToPurge(Set.of(realmOne, realmTwo))
            .build());
  }

  @Test
  public void noRealmsSpecified() {
    persOne.write(ObjOne.builder().text("foo").id(persOne.generateId()).build(), ObjOne.class);
    persTwo.write(ObjTwo.builder().text("bar").id(persTwo.generateId()).build(), ObjTwo.class);

    persOne.createReference("ref1", Optional.empty());
    persTwo.createReference("ref1", Optional.empty());

    mutableMonotonicClock.advanceBoth(GRACE_TIME);

    // Run maintenance, no realm given to retain or purge, must not purge anything
    var runInfo =
        maintenance.performMaintenance(
            MaintenanceRunSpec.builder().includeSystemRealm(false).build());

    soft.assertThat(runInfo.referenceStats())
        .contains(MaintenanceStats.builder().scanned(2L).purged(0L).retained(2L).newer(0L).build());
    soft.assertThat(runInfo.objStats())
        .contains(MaintenanceStats.builder().scanned(2L).purged(0L).retained(2L).newer(0L).build());
    soft.assertThat(runInfo.identifiedReferences().orElse(-1)).isEqualTo(0L);
    soft.assertThat(runInfo.identifiedObjs().orElse(-1)).isEqualTo(0L);
  }

  @Test
  public void simple() {
    var rOneObj1 =
        persOne.write(ObjOne.builder().text("foo").id(persOne.generateId()).build(), ObjOne.class);
    var rTwoObj2 =
        persTwo.write(ObjTwo.builder().text("bar").id(persTwo.generateId()).build(), ObjTwo.class);

    var systemRealmCalled = new AtomicInteger();
    RealmIdentOne.testCallback =
        collector -> {
          if (collector.isSystemRealm()) {
            systemRealmCalled.incrementAndGet();
          }
          return true;
        };

    persOne.createReference("ref1", Optional.empty());
    persOne.createReference("ref2", Optional.empty());
    persTwo.createReference("ref1", Optional.empty());
    persTwo.createReference("ref2", Optional.empty());

    mutableMonotonicClock.advanceBoth(GRACE_TIME);

    // Run maintenance, provide realms, must purge unidentified
    var runInfo =
        maintenance.performMaintenance(
            MaintenanceRunSpec.builder()
                .includeSystemRealm(false)
                .realmsToProcess(Set.of(realmOne, realmTwo))
                .build());

    soft.assertThat(runInfo.referenceStats())
        .contains(MaintenanceStats.builder().scanned(4L).purged(4L).retained(0L).newer(0L).build());
    soft.assertThat(runInfo.objStats())
        .contains(MaintenanceStats.builder().scanned(2L).purged(2L).retained(0L).newer(0L).build());
    soft.assertThat(runInfo.identifiedReferences().orElse(-1)).isEqualTo(0L);
    soft.assertThat(runInfo.identifiedObjs().orElse(-1)).isEqualTo(0L);

    soft.assertThatExceptionOfType(ReferenceNotFoundException.class)
        .isThrownBy(() -> persOne.fetchReference("ref1"));
    soft.assertThatExceptionOfType(ReferenceNotFoundException.class)
        .isThrownBy(() -> persOne.fetchReference("ref2"));
    soft.assertThatExceptionOfType(ReferenceNotFoundException.class)
        .isThrownBy(() -> persTwo.fetchReference("ref1"));
    soft.assertThatExceptionOfType(ReferenceNotFoundException.class)
        .isThrownBy(() -> persTwo.fetchReference("ref2"));

    soft.assertThat(persOne.fetch(objRef(rOneObj1), ObjOne.class)).isNull();
    soft.assertThat(persTwo.fetch(objRef(rTwoObj2), ObjTwo.class)).isNull();
  }

  @Test
  public void systemRealm() {
    var systemRealmCalled = new AtomicInteger();
    RealmIdentOne.testCallback =
        collector -> {
          if (collector.isSystemRealm()) {
            systemRealmCalled.incrementAndGet();
          }
          return true;
        };

    mutableMonotonicClock.advanceBoth(GRACE_TIME);

    // Run maintenance, provide realms, must purge unidentified
    var runInfo =
        maintenance.performMaintenance(
            MaintenanceRunSpec.builder()
                .includeSystemRealm(true) // default
                .build());

    soft.assertThat(systemRealmCalled).hasValue(1);

    soft.assertThat(runInfo.referenceStats())
        .contains(MaintenanceStats.builder().scanned(1L).purged(0L).retained(0L).newer(1L).build());
    soft.assertThat(runInfo.objStats())
        .contains(MaintenanceStats.builder().scanned(4L).purged(0L).retained(0L).newer(4L).build());
    soft.assertThat(runInfo.identifiedReferences().orElse(-1)).isEqualTo(2L);
    soft.assertThat(runInfo.identifiedObjs().orElse(-1))
        .isEqualTo((1 << SnowflakeIdGenerator.DEFAULT_NODE_ID_BITS) + 4L);
  }

  @Test
  public void simpleRetainViaRealmIdentifier() {
    var rOneObj1 =
        persOne.write(ObjOne.builder().text("foo").id(persOne.generateId()).build(), ObjOne.class);
    var rTwoObj2 =
        persTwo.write(ObjTwo.builder().text("bar").id(persTwo.generateId()).build(), ObjTwo.class);

    persOne.createReference("ref1", Optional.empty());
    persOne.createReference("ref2", Optional.empty());
    persTwo.createReference("ref1", Optional.empty());
    persTwo.createReference("ref2", Optional.empty());

    // identify rOneObj1 as "live"
    RealmIdentOne.testCallback =
        c -> {
          if (c.realm().equals(realmOne)) {
            c.retainObject(objRef(rOneObj1));
            c.retainReference("ref1");
          }
          return true;
        };

    mutableMonotonicClock.advanceBoth(GRACE_TIME);

    // Run maintenance, provide realms, must purge unidentified
    var runInfo =
        maintenance.performMaintenance(
            MaintenanceRunSpec.builder()
                .includeSystemRealm(false)
                .realmsToProcess(Set.of(realmOne, realmTwo))
                .build());

    soft.assertThat(runInfo.referenceStats())
        .contains(MaintenanceStats.builder().scanned(4L).purged(3L).retained(1L).newer(0L).build());
    soft.assertThat(runInfo.objStats())
        .contains(MaintenanceStats.builder().scanned(2L).purged(1L).retained(1L).newer(0L).build());
    soft.assertThat(runInfo.identifiedReferences().orElse(-1)).isEqualTo(1L);
    soft.assertThat(runInfo.identifiedObjs().orElse(-1)).isEqualTo(1L);

    soft.assertThatCode(() -> persOne.fetchReference("ref1")).doesNotThrowAnyException();
    soft.assertThatExceptionOfType(ReferenceNotFoundException.class)
        .isThrownBy(() -> persOne.fetchReference("ref2"));
    soft.assertThatExceptionOfType(ReferenceNotFoundException.class)
        .isThrownBy(() -> persTwo.fetchReference("ref1"));
    soft.assertThatExceptionOfType(ReferenceNotFoundException.class)
        .isThrownBy(() -> persTwo.fetchReference("ref2"));

    soft.assertThat(persOne.fetch(objRef(rOneObj1), ObjOne.class)).isEqualTo(rOneObj1);
    soft.assertThat(persTwo.fetch(objRef(rTwoObj2), ObjTwo.class)).isNull();
  }

  @Test
  public void simpleRetainViaRealmIdentifierPersistence() {
    var rOneObj1 =
        persOne.write(ObjOne.builder().text("foo").id(persOne.generateId()).build(), ObjOne.class);
    var rTwoObj2 =
        persTwo.write(ObjTwo.builder().text("bar").id(persTwo.generateId()).build(), ObjTwo.class);

    persOne.createReference("ref1", Optional.empty());
    persOne.createReference("ref2", Optional.empty());
    persTwo.createReference("ref1", Optional.empty());
    persTwo.createReference("ref2", Optional.empty());

    // identify rOneObj1 as "live"
    RealmIdentOne.testCallback =
        c -> {
          if (c.realm().equals(realmOne)) {
            c.realmPersistence().fetch(objRef(rOneObj1), ObjOne.class);
            c.realmPersistence().fetchReference("ref1");
          }
          return true;
        };

    mutableMonotonicClock.advanceBoth(GRACE_TIME);

    // Run maintenance, provide realms, must purge unidentified
    var runInfo =
        maintenance.performMaintenance(
            MaintenanceRunSpec.builder()
                .includeSystemRealm(false)
                .realmsToProcess(Set.of(realmOne, realmTwo))
                .build());

    soft.assertThat(runInfo.referenceStats())
        .contains(MaintenanceStats.builder().scanned(4L).purged(3L).retained(1L).newer(0L).build());
    soft.assertThat(runInfo.objStats())
        .contains(MaintenanceStats.builder().scanned(2L).purged(1L).retained(1L).newer(0L).build());
    soft.assertThat(runInfo.identifiedReferences().orElse(-1)).isEqualTo(1L);
    soft.assertThat(runInfo.identifiedObjs().orElse(-1)).isEqualTo(1L);

    soft.assertThatCode(() -> persOne.fetchReference("ref1")).doesNotThrowAnyException();
    soft.assertThatExceptionOfType(ReferenceNotFoundException.class)
        .isThrownBy(() -> persOne.fetchReference("ref2"));
    soft.assertThatExceptionOfType(ReferenceNotFoundException.class)
        .isThrownBy(() -> persTwo.fetchReference("ref1"));
    soft.assertThatExceptionOfType(ReferenceNotFoundException.class)
        .isThrownBy(() -> persTwo.fetchReference("ref2"));

    soft.assertThat(persOne.fetch(objRef(rOneObj1), ObjOne.class)).isEqualTo(rOneObj1);
    soft.assertThat(persTwo.fetch(objRef(rTwoObj2), ObjTwo.class)).isNull();
  }

  @Test
  public void simpleRetainViaObjTypeIdentifier() {
    var rOneObj1 =
        persOne.write(ObjOne.builder().text("foo1").id(persOne.generateId()).build(), ObjOne.class);
    var rOneObj2 =
        persOne.write(ObjTwo.builder().text("foo2").id(persOne.generateId()).build(), ObjTwo.class);
    var rTwoObj1 =
        persTwo.write(ObjOne.builder().text("bar2").id(persTwo.generateId()).build(), ObjOne.class);
    var rTwoObj2 =
        persTwo.write(ObjTwo.builder().text("bar2").id(persTwo.generateId()).build(), ObjTwo.class);

    // identify rOneObj1 as "live"
    RealmIdentOne.testCallback =
        c -> {
          c.retainObject(objRef(rOneObj1));
          return true;
        };
    // identify rObjObj2 via obj-type callback
    ObjTypeIdentOne.testCallback =
        (c, id) -> {
          if (id.equals(objRef(rOneObj1))) {
            c.retainObject(objRef(rOneObj2));
          }
        };
    // identify rTwoObj1
    RealmIdentTwo.testCallback =
        c -> {
          c.retainObject(objRef(rTwoObj1));
          return true;
        };

    mutableMonotonicClock.advanceBoth(GRACE_TIME);

    // Run maintenance, provide realms, must purge unidentified
    var runInfo =
        maintenance.performMaintenance(
            MaintenanceRunSpec.builder()
                .includeSystemRealm(false)
                .realmsToProcess(Set.of(realmOne, realmTwo))
                .build());

    soft.assertThat(runInfo.referenceStats())
        .contains(MaintenanceStats.builder().scanned(0L).purged(0L).retained(0L).newer(0L).build());
    soft.assertThat(runInfo.objStats())
        .contains(MaintenanceStats.builder().scanned(4L).purged(1L).retained(3L).newer(0L).build());
    soft.assertThat(runInfo.identifiedReferences().orElse(-1)).isEqualTo(0L);
    soft.assertThat(runInfo.identifiedObjs().orElse(-1)).isEqualTo(6L);

    soft.assertThat(persOne.fetch(objRef(rOneObj1), ObjOne.class)).isEqualTo(rOneObj1);
    soft.assertThat(persOne.fetch(objRef(rOneObj2), ObjTwo.class)).isEqualTo(rOneObj2);
    soft.assertThat(persTwo.fetch(objRef(rTwoObj1), ObjOne.class)).isEqualTo(rTwoObj1);
    soft.assertThat(persTwo.fetch(objRef(rTwoObj2), ObjTwo.class)).isNull();
  }

  @Test
  public void noRealmIdentifierHandlesRealms() {
    var rOneObj1 =
        persOne.write(ObjOne.builder().text("foo").id(persOne.generateId()).build(), ObjOne.class);
    var rTwoObj2 =
        persTwo.write(ObjTwo.builder().text("bar").id(persTwo.generateId()).build(), ObjTwo.class);
    persOne.createReference("ref1", Optional.empty());
    persOne.createReference("ref2", Optional.empty());
    persTwo.createReference("ref1", Optional.empty());
    persTwo.createReference("ref2", Optional.empty());

    RealmIdentOne.testCallback = c -> false;
    RealmIdentTwo.testCallback = c -> false;

    mutableMonotonicClock.advanceBoth(GRACE_TIME);

    // Run maintenance, provide realms, no realm-identifier handles realm, must NOT purge
    var runInfo =
        maintenance.performMaintenance(
            MaintenanceRunSpec.builder()
                .includeSystemRealm(false)
                .realmsToProcess(Set.of(realmOne, realmTwo))
                .build());

    soft.assertThat(runInfo.referenceStats())
        .contains(MaintenanceStats.builder().scanned(4L).purged(0L).retained(4L).newer(0L).build());
    soft.assertThat(runInfo.objStats())
        .contains(MaintenanceStats.builder().scanned(2L).purged(0L).retained(2L).newer(0L).build());
    soft.assertThat(runInfo.identifiedReferences().orElse(-1)).isEqualTo(0L);
    soft.assertThat(runInfo.identifiedObjs().orElse(-1)).isEqualTo(0L);

    soft.assertThat(persOne.fetch(objRef(rOneObj1), ObjOne.class)).isEqualTo(rOneObj1);
    soft.assertThat(persTwo.fetch(objRef(rTwoObj2), ObjTwo.class)).isEqualTo(rTwoObj2);
  }
}
