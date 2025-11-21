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
package org.apache.polaris.persistence.nosql.impl;

import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.assertj.core.api.InstanceOfAssertFactories.BYTE_ARRAY;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.PersistId;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceAlreadyExistsException;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.AnotherTestObj;
import org.apache.polaris.persistence.nosql.api.obj.ImmutableVersionedTestObj;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj;
import org.apache.polaris.persistence.nosql.api.obj.VersionedTestObj;
import org.apache.polaris.persistence.nosql.api.ref.ImmutableReference;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.testextension.PersistenceTestExtension;
import org.apache.polaris.persistence.nosql.testextension.PolarisPersistence;
import org.assertj.core.api.Assumptions;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

@ExtendWith({PersistenceTestExtension.class, SoftAssertionsExtension.class})
public abstract class AbstractPersistenceTests {
  @InjectSoftAssertions protected SoftAssertions soft;

  @PolarisPersistence protected Backend backend;

  protected abstract Persistence persistence();

  @Test
  public void referenceLifecycle(TestInfo testInfo) {
    var refName = testInfo.getTestMethod().orElseThrow().getName();
    soft.assertThatThrownBy(() -> persistence().fetchReference(refName))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessage(refName);
    soft.assertThatThrownBy(
            () ->
                persistence()
                    .updateReferencePointer(
                        Reference.builder()
                            .name(refName)
                            .createdAtMicros(123L)
                            .previousPointers()
                            .build(),
                        objRef("type", 123L, 1)))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessage(refName);

    persistence().createReference(refName, Optional.empty());
    soft.assertThatThrownBy(() -> persistence().createReference(refName, Optional.empty()))
        .isInstanceOf(ReferenceAlreadyExistsException.class)
        .hasMessage(refName);
  }

  @Test
  public void referenceWithInitialPointer(TestInfo testInfo) {
    var refName = testInfo.getTestMethod().orElseThrow().getName();
    var id1 = objRef(SimpleTestObj.TYPE, persistence().generateId(), 1);
    var id2 = objRef(SimpleTestObj.TYPE, persistence().generateId(), 1);

    var ref1 = persistence().createReference(refName, Optional.of(id1));
    soft.assertThat(persistence().fetchReference(refName)).isEqualTo(ref1);

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> persistence().updateReferencePointer(ref1, id1))
        .withMessage("New pointer must not be equal to the expected pointer.");

    var ref2 = persistence().updateReferencePointer(ref1, id2);
    soft.assertThat(ref2)
        .get()
        .isEqualTo(
            ImmutableReference.builder()
                .from(ref1)
                .pointer(id2)
                .previousPointers(id1.id())
                .build());
    soft.assertThat(persistence().fetchReference(refName)).isEqualTo(ref2.orElseThrow());
  }

  @Test
  public void referenceWithoutInitialPointer(TestInfo testInfo) {
    var refName = testInfo.getTestMethod().orElseThrow().getName();
    var id1 = objRef(SimpleTestObj.TYPE, persistence().generateId(), 1);
    var id2 = objRef(SimpleTestObj.TYPE, persistence().generateId(), 1);

    var ref1 = persistence().createReference(refName, Optional.empty());
    soft.assertThat(persistence().fetchReference(refName)).isEqualTo(ref1);

    var ref2 = persistence().updateReferencePointer(ref1, id1);
    soft.assertThat(ref2)
        .get()
        .isEqualTo(ImmutableReference.builder().from(ref1).pointer(id1).build());
    soft.assertThat(persistence().fetchReference(refName)).isEqualTo(ref2.orElseThrow());

    var ref3 = persistence().updateReferencePointer(ref2.orElseThrow(), id2);
    soft.assertThat(ref3)
        .get()
        .isEqualTo(
            ImmutableReference.builder()
                .from(ref1)
                .pointer(id2)
                .previousPointers(id1.id())
                .build());
    soft.assertThat(persistence().fetchReference(refName)).isEqualTo(ref3.orElseThrow());
  }

  @CartesianTest
  public void createReferencesSilent(
      @Values(ints = {0, 1, 2}) int numExisting, @Values(ints = {1, 2, 3, 5, 10}) int numRefs) {
    var refNamePrefix = "createReferencesSilent";

    var existingRefNames =
        IntStream.range(0, numExisting).mapToObj(i -> refNamePrefix + "_ex_" + i).toList();

    var existingRefs = new ArrayList<Reference>();
    for (var refName : existingRefNames) {
      var id = objRef(SimpleTestObj.TYPE, persistence().generateId(), 1);
      var ref = persistence().createReference(refName, Optional.of(id));
      existingRefs.add(ref);
    }

    soft.assertThat(existingRefNames).allSatisfy(refName -> persistence().fetchReference(refName));

    var allRefNames =
        IntStream.range(numExisting, numRefs)
            .mapToObj(i -> refNamePrefix + "_all_" + i)
            .collect(Collectors.toSet());

    persistence().createReferencesSilent(allRefNames);

    for (var existingRef : existingRefs) {
      var ref = persistence().fetchReference(existingRef.name());
      soft.assertThat(ref).describedAs(existingRef.name()).isEqualTo(existingRef);
    }

    var updatedRefs = new ArrayList<Reference>();
    for (var refName : allRefNames) {
      var ref = persistence().fetchReference(refName);
      var id = objRef(SimpleTestObj.TYPE, persistence().generateId(), 1);
      ref = persistence().updateReferencePointer(ref, id).orElseThrow();
      updatedRefs.add(ref);
    }

    persistence().createReferencesSilent(allRefNames);

    for (var updatedRef : updatedRefs) {
      var ref = persistence().fetchReference(updatedRef.name());
      soft.assertThat(ref).describedAs(updatedRef.name()).isEqualTo(updatedRef);
    }
  }

  @Test
  public void referenceRecentPointers(TestInfo testInfo) {
    var type = new AbstractObjType<>("dummyTest", "dummy", Obj.class) {};
    var refName = testInfo.getTestMethod().orElseThrow().getName();
    var id1 = objRef(type, persistence().generateId(), 1);
    var ref = persistence().createReference(refName, Optional.of(id1));

    var recentPointers = new ArrayList<Long>();
    for (var i = 0; i < persistence().params().referencePreviousHeadCount(); i++) {
      recentPointers.addFirst(ref.pointer().orElseThrow().id());
      var id = objRef(type, persistence().generateId(), 1);
      ref = persistence().updateReferencePointer(ref, id).orElseThrow();
      soft.assertThat(ref)
          .extracting(Reference::pointer, Reference::previousPointers)
          .containsExactly(
              Optional.of(id), recentPointers.stream().mapToLong(Long::longValue).toArray());
    }

    for (var i = 0; i < persistence().params().referencePreviousHeadCount(); i++) {
      recentPointers.removeLast();
      recentPointers.addFirst(ref.pointer().orElseThrow().id());
      var id = objRef(type, persistence().generateId(), 1);
      ref = persistence().updateReferencePointer(ref, id).orElseThrow();
      soft.assertThat(ref)
          .extracting(Reference::pointer, Reference::previousPointers)
          .containsExactly(
              Optional.of(id), recentPointers.stream().mapToLong(Long::longValue).toArray());
    }
  }

  /**
   * Exercises a bunch of reference names that can be problematic if the database uses collators,
   * that for example, collapse adjacent spaces.
   */
  @Test
  public void referenceNames() {
    List<String> refNames =
        List.of(
            //
            "a-01",
            "a-1",
            "a-10",
            "a-2",
            "a-20",
            "ä-01",
            "ä-1",
            "ä-  1",
            "ä-   1",
            //
            "a01",
            "a1",
            "a10",
            "a2",
            "a20",
            //
            "a-   01",
            "a-    1",
            "a-   10",
            "a-    2",
            "a-   20",
            //
            "ä-   01",
            "ä-    1",
            "ä-   10",
            "ä-    2",
            "ä-   20",
            //
            "b-   01",
            "b-    1",
            "b-   10",
            "b-    2",
            "b-   20",
            //
            "a-     01",
            "a-      1",
            "a-     10",
            "a-      2",
            "a-     20");

    var refToId = new HashMap<String, ObjRef>();

    for (String refName : refNames) {
      var id = objRef(SimpleTestObj.TYPE, persistence().generateId(), 1);
      soft.assertThatCode(() -> persistence().createReference(refName, Optional.of(id)))
          .describedAs("create ref: %s", refName)
          .doesNotThrowAnyException();
      refToId.put(refName, id);
    }

    for (String refName : refNames) {
      soft.assertThat(persistence().fetchReference(refName))
          .describedAs("fetch ref: %s", refName)
          .extracting(Reference::pointer)
          .isEqualTo(Optional.of(refToId.get(refName)));
    }
  }

  @Test
  public void objs() {
    var obj1 =
        (SimpleTestObj)
            SimpleTestObj.builder()
                .id(persistence().generateId())
                .numParts(0)
                .text("some text")
                .build();
    var obj2 =
        (SimpleTestObj)
            SimpleTestObj.builder()
                .id(persistence().generateId())
                .numParts(0)
                .text("other text")
                .build();
    var id1 = objRef(obj1);
    var id2 = objRef(obj2);
    soft.assertThat(
            persistence().fetch(objRef(obj1.type().id(), obj1.id(), 1), SimpleTestObj.class))
        .isNull();
    obj1 = persistence().write(obj1, SimpleTestObj.class);
    id1 = objRef(obj1);
    obj2 = persistence().write(obj2, SimpleTestObj.class);
    id2 = objRef(obj2);
    soft.assertThat(obj1).extracting(Obj::createdAtMicros, LONG).isGreaterThan(0L);
    soft.assertThat(obj2).extracting(Obj::createdAtMicros, LONG).isGreaterThan(0L);
    soft.assertThat(obj1).extracting(Obj::numParts).isEqualTo(1);
    soft.assertThat(obj2).extracting(Obj::numParts).isEqualTo(1);
    var fetched1 = persistence().fetch(id1, SimpleTestObj.class);
    soft.assertThat(fetched1)
        .isEqualTo(obj1)
        .extracting(Obj::createdAtMicros, LONG)
        .isEqualTo(obj1.createdAtMicros());
    soft.assertThat(persistence().getImmediate(id1, SimpleTestObj.class)).isEqualTo(obj1);
    // Check whether fetchMany() works with "0 expected parts"
    soft.assertThat(persistence().fetch(objRef(id1.type(), id1.id(), 0), SimpleTestObj.class))
        .isEqualTo(obj1);
    soft.assertThat(
            persistence().getImmediate(objRef(id1.type(), id1.id(), 0), SimpleTestObj.class))
        .isEqualTo(obj1);

    var id1final = id1;
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                persistence()
                    .fetch(objRef(id1final.type(), id1final.id(), 0), AnotherTestObj.class))
        .withMessageStartingWith(
            "Mismatch between persisted object type 'test-simple' (interface org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj) and deserialized interface org.apache.polaris.persistence.nosql.api.obj.AnotherTestObj.");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                persistence()
                    .fetch(
                        objRef(AnotherTestObj.TYPE.id(), id1final.id(), 0), AnotherTestObj.class))
        .withMessageStartingWith(
            "Mismatch between persisted object type 'test-simple' (interface org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj) and deserialized interface org.apache.polaris.persistence.nosql.api.obj.AnotherTestObj.");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                persistence()
                    .fetchMany(AnotherTestObj.class, objRef(id1final.type(), id1final.id(), 0)))
        .withMessageStartingWith(
            "Mismatch between persisted object type 'test-simple' (interface org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj) and deserialized interface org.apache.polaris.persistence.nosql.api.obj.AnotherTestObj.");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                persistence()
                    .fetchMany(
                        AnotherTestObj.class, objRef(AnotherTestObj.TYPE.id(), id1final.id(), 0)))
        .withMessageStartingWith(
            "Mismatch between persisted object type 'test-simple' (interface org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj) and deserialized interface org.apache.polaris.persistence.nosql.api.obj.AnotherTestObj.");

    var fetched2 = persistence().fetch(id2, SimpleTestObj.class);
    soft.assertThat(fetched2)
        .isEqualTo(obj2)
        .extracting(Obj::createdAtMicros, LONG)
        .isEqualTo(obj2.createdAtMicros());
    soft.assertThat(persistence().fetchMany(SimpleTestObj.class, id1, id2))
        .containsExactly(obj1, obj2);
    soft.assertThat(persistence().fetchMany(SimpleTestObj.class, id1, null, id2))
        .containsExactly(obj1, null, obj2);
    // Check whether fetchMany() works with "0 expected parts"
    soft.assertThat(
            persistence()
                .fetchMany(
                    SimpleTestObj.class,
                    objRef(id1.type(), id1.id(), 0),
                    null,
                    objRef(id2.type(), id2.id(), 0)))
        .containsExactly(obj1, null, obj2);
    soft.assertThat(
            persistence()
                .fetchMany(
                    SimpleTestObj.class,
                    id1,
                    null,
                    id2,
                    objRef(SimpleTestObj.TYPE, persistence().generateId(), 1)))
        .containsExactly(obj1, null, obj2, null);

    var obj1updated =
        SimpleTestObj.builder().from(obj1).text("some other text").number(123).build();
    var obj2updated = SimpleTestObj.builder().from(obj2).text("different text").number(456).build();
    soft.assertThat(persistence().write(obj1updated, SimpleTestObj.class))
        .extracting(Obj::createdAtMicros, LONG)
        .isGreaterThanOrEqualTo(obj1.createdAtMicros());
    soft.assertThat(persistence().write(obj2updated, SimpleTestObj.class))
        .extracting(Obj::createdAtMicros, LONG)
        .isGreaterThanOrEqualTo(obj1.createdAtMicros());
    soft.assertThat(persistence().fetch(id1, SimpleTestObj.class)).isEqualTo(obj1updated);
    soft.assertThat(persistence().fetch(id2, SimpleTestObj.class)).isEqualTo(obj2updated);
    soft.assertThat(persistence().fetchMany(SimpleTestObj.class, id1, id2))
        .containsExactly(obj1updated, obj2updated);

    persistence().delete(objRef(SimpleTestObj.TYPE, persistence().generateId(), 1));
    persistence()
        .deleteMany(
            objRef(SimpleTestObj.TYPE, persistence().generateId(), 1),
            objRef(SimpleTestObj.TYPE, persistence().generateId(), 1),
            null);
    soft.assertThat(persistence().fetchMany(SimpleTestObj.class, id1, id2))
        .containsExactly(obj1updated, obj2updated);

    persistence().delete(id1);
    soft.assertThat(persistence().fetchMany(SimpleTestObj.class, id1, id2))
        .containsExactly(null, obj2updated);
    persistence().delete(id2);
    soft.assertThat(persistence().fetchMany(SimpleTestObj.class, id1, id2))
        .containsExactly(null, null);

    var obj1updated2 = SimpleTestObj.builder().from(obj1updated).optional("optional2").build();
    var obj2updated2 = SimpleTestObj.builder().from(obj2updated).optional("optional2").build();
    soft.assertThat(persistence().writeMany(SimpleTestObj.class, obj1updated2, obj2updated2))
        .containsExactly(obj1updated2, obj2updated2);
    soft.assertThat(persistence().fetchMany(SimpleTestObj.class, id1, id2))
        .containsExactly(obj1updated2, obj2updated2);
  }

  @ParameterizedTest
  @ValueSource(ints = {50, 10 * 1024, 200 * 1024, 400 * 1024, 1024 * 1024, 13 * 1024 * 1024})
  public void hugeObject(int binaryLen) {
    var data = new byte[binaryLen];
    ThreadLocalRandom.current().nextBytes(data);

    var obj =
        (SimpleTestObj)
            SimpleTestObj.builder().id(persistence().generateId()).numParts(0).binary(data).build();
    obj = persistence().write(obj, SimpleTestObj.class);
    soft.assertThat(persistence().fetch(objRef(obj), SimpleTestObj.class))
        .isEqualTo(obj)
        .extracting(SimpleTestObj::binary, BYTE_ARRAY)
        .containsExactly(data);
    var updatedObj =
        (SimpleTestObj) SimpleTestObj.builder().from(obj).optional("optional2").build();
    persistence().write(updatedObj, SimpleTestObj.class);
    soft.assertThat(persistence().fetch(objRef(obj), SimpleTestObj.class))
        .isEqualTo(updatedObj)
        .extracting(SimpleTestObj::binary, BYTE_ARRAY)
        .containsExactly(data);

    // Fetch with the "wrong" number of parts
    soft.assertThat(persistence().fetch(objRef(obj.type(), obj.id(), 0), SimpleTestObj.class))
        .isEqualTo(updatedObj)
        .extracting(SimpleTestObj::binary, BYTE_ARRAY)
        .containsExactly(data);
    soft.assertThat(persistence().fetch(objRef(obj.type(), obj.id(), 1), SimpleTestObj.class))
        .isEqualTo(updatedObj)
        .extracting(SimpleTestObj::binary, BYTE_ARRAY)
        .containsExactly(data);
    soft.assertThat(persistence().fetch(objRef(obj.type(), obj.id(), 30), SimpleTestObj.class))
        .isEqualTo(updatedObj)
        .extracting(SimpleTestObj::binary, BYTE_ARRAY)
        .containsExactly(data);
    soft.assertThat(
            persistence().getImmediate(objRef(obj.type(), obj.id(), 0), SimpleTestObj.class))
        .isEqualTo(updatedObj)
        .extracting(SimpleTestObj::binary, BYTE_ARRAY)
        .containsExactly(data);
    soft.assertThat(
            persistence().getImmediate(objRef(obj.type(), obj.id(), 1), SimpleTestObj.class))
        .isEqualTo(updatedObj)
        .extracting(SimpleTestObj::binary, BYTE_ARRAY)
        .containsExactly(data);
    soft.assertThat(
            persistence().getImmediate(objRef(obj.type(), obj.id(), 30), SimpleTestObj.class))
        .isEqualTo(updatedObj)
        .extracting(SimpleTestObj::binary, BYTE_ARRAY)
        .containsExactly(data);
    soft.assertThat(persistence().fetchMany(SimpleTestObj.class, objRef(obj.type(), obj.id(), 0)))
        .containsExactly(updatedObj);
    soft.assertThat(persistence().fetchMany(SimpleTestObj.class, objRef(obj.type(), obj.id(), 1)))
        .containsExactly(updatedObj);
    soft.assertThat(persistence().fetchMany(SimpleTestObj.class, objRef(obj.type(), obj.id(), 30)))
        .containsExactly(updatedObj);
  }

  @Test
  public void conditionalObjects() {
    var nonVersionedObj1 =
        SimpleTestObj.builder()
            .id(persistence().generateId())
            .numParts(0)
            .text("some text")
            .build();
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> persistence().conditionalInsert(nonVersionedObj1, SimpleTestObj.class))
        .withMessage("'obj' must have a non-null 'versionToken'");

    var obj1initial =
        VersionedTestObj.builder()
            .id(persistence().generateId())
            .versionToken("t1")
            .someValue("foo")
            .build();
    var obj2initial =
        VersionedTestObj.builder()
            .id(persistence().generateId())
            .versionToken("t2")
            .someValue("bar")
            .build();
    var objNotPresent =
        VersionedTestObj.builder()
            .id(persistence().generateId())
            .versionToken("t3")
            .someValue("baz")
            .build();

    var obj1 = persistence().conditionalInsert(obj1initial, VersionedTestObj.class);
    soft.assertThat(obj1)
        .isEqualTo(ImmutableVersionedTestObj.builder().from(obj1initial).build())
        .extracting(Obj::createdAtMicros, LONG)
        .isGreaterThan(0L);
    var obj2 = persistence().conditionalInsert(obj2initial, VersionedTestObj.class);
    soft.assertThat(obj2)
        .isEqualTo(ImmutableVersionedTestObj.builder().from(obj2initial).build())
        .extracting(Obj::createdAtMicros, LONG)
        .isGreaterThan(0L);

    // Make IDEs happy
    requireNonNull(obj1);
    requireNonNull(obj2);

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                persistence()
                    .conditionalInsert(
                        (VersionedTestObj) obj1.withNumParts(0), VersionedTestObj.class))
        .withMessage("'obj' must have 'numParts' == 1");

    soft.assertThat(
            persistence()
                .conditionalInsert(
                    ImmutableVersionedTestObj.builder().from(obj1).build(), VersionedTestObj.class))
        .isNull();
    soft.assertThat(
            persistence()
                .conditionalInsert(
                    ImmutableVersionedTestObj.builder().from(obj2).build(), VersionedTestObj.class))
        .isNull();

    soft.assertThat(persistence().fetch(objRef(obj1), VersionedTestObj.class)).isEqualTo(obj1);
    soft.assertThat(persistence().fetch(objRef(obj2), VersionedTestObj.class)).isEqualTo(obj2);

    var obj1updated =
        VersionedTestObj.builder()
            .from(obj1)
            .someValue("updated foo")
            .versionToken("t1updated")
            .build();
    var obj2updated =
        VersionedTestObj.builder()
            .from(obj2)
            .someValue("updated bar")
            .versionToken("t2updated")
            .build();

    soft.assertThat(
            persistence()
                .conditionalUpdate(
                    VersionedTestObj.builder().from(obj1).versionToken("incorrect").build(),
                    obj1updated,
                    VersionedTestObj.class))
        .isNull();
    soft.assertThat(persistence().conditionalUpdate(obj1updated, obj1, VersionedTestObj.class))
        .isNull();
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () -> persistence().conditionalUpdate(obj1updated, obj1updated, VersionedTestObj.class))
        .withMessage("'versionToken' of 'expected' and 'update' must not be equal");

    soft.assertThat(persistence().fetch(objRef(obj1), VersionedTestObj.class)).isEqualTo(obj1);

    var updated1 = persistence().conditionalUpdate(obj1, obj1updated, VersionedTestObj.class);
    soft.assertThat(updated1)
        .isEqualTo(obj1updated)
        .extracting(Obj::createdAtMicros, LONG)
        .isGreaterThan(0L);
    var updated2 = persistence().conditionalUpdate(obj2, obj2updated, VersionedTestObj.class);
    soft.assertThat(updated2)
        .isEqualTo(obj2updated)
        .extracting(Obj::createdAtMicros, LONG)
        .isGreaterThan(0L);
    soft.assertThat(
            persistence()
                .conditionalUpdate(
                    ImmutableVersionedTestObj.builder().from(objNotPresent).build(),
                    VersionedTestObj.builder().from(objNotPresent).versionToken("meep").build(),
                    VersionedTestObj.class))
        .isNull();

    soft.assertThat(persistence().fetch(objRef(obj1), VersionedTestObj.class))
        .isEqualTo(obj1updated);
    soft.assertThat(persistence().fetch(objRef(obj2), VersionedTestObj.class))
        .isEqualTo(obj2updated);

    soft.assertThat(persistence().conditionalDelete(obj1, VersionedTestObj.class)).isFalse();

    soft.assertThat(persistence().fetch(objRef(obj1), VersionedTestObj.class))
        .isEqualTo(obj1updated);

    soft.assertThat(persistence().conditionalDelete(obj1updated, VersionedTestObj.class)).isTrue();
    soft.assertThat(persistence().conditionalDelete(obj1updated, VersionedTestObj.class)).isFalse();
    soft.assertThat(
            persistence().conditionalDelete(objNotPresent.withNumParts(1), VersionedTestObj.class))
        .isFalse();

    soft.assertThat(persistence().fetch(objRef(obj1), VersionedTestObj.class)).isNull();
    soft.assertThat(persistence().fetch(objRef(obj2), VersionedTestObj.class))
        .isEqualTo(obj2updated);
  }

  @Test
  public void backendRealmDeletion(
      @PolarisPersistence Persistence one,
      @PolarisPersistence Persistence two,
      @PolarisPersistence Persistence three) {
    Assumptions.assumeThat(backend.supportsRealmDeletion()).isTrue();
    soft.assertThat(one.realmId())
        .isNotEqualTo(two.realmId())
        .isNotEqualTo(persistence().realmId());

    var num = 20;

    var oneObjs = new ArrayList<ObjRef>();
    var twoObjs = new ArrayList<ObjRef>();
    var threeObjs = new ArrayList<ObjRef>();
    threeRealmsSetup(one, two, three, oneObjs, twoObjs, threeObjs, num);

    backend.deleteRealms(Set.of());

    // No realm deleted, all created refs + objs must still exist
    for (var i = 0; i < num; i++) {
      var ref = "ref-" + i;
      soft.assertThatCode(() -> one.fetchReference(ref)).doesNotThrowAnyException();
      soft.assertThatCode(() -> two.fetchReference(ref)).doesNotThrowAnyException();
      soft.assertThatCode(() -> three.fetchReference(ref)).doesNotThrowAnyException();
    }
    soft.assertThat(one.fetchMany(SimpleTestObj.class, oneObjs.toArray(new ObjRef[0])))
        .doesNotContainNull()
        .extracting(SimpleTestObj::number)
        .extracting(Number::intValue)
        .containsExactly(IntStream.range(0, num).boxed().toArray(Integer[]::new));
    soft.assertThat(two.fetchMany(SimpleTestObj.class, twoObjs.toArray(new ObjRef[0])))
        .doesNotContainNull()
        .extracting(SimpleTestObj::number)
        .extracting(Number::intValue)
        .containsExactly(IntStream.range(0, num).boxed().toArray(Integer[]::new));
    soft.assertThat(three.fetchMany(SimpleTestObj.class, threeObjs.toArray(new ObjRef[0])))
        .doesNotContainNull()
        .extracting(SimpleTestObj::number)
        .extracting(Number::intValue)
        .containsExactly(IntStream.range(0, num).boxed().toArray(Integer[]::new));

    backend.deleteRealms(Set.of(one.realmId()));

    // realm 1 deleted
    for (var i = 0; i < num; i++) {
      var ref = "ref-" + i;
      soft.assertThatThrownBy(() -> one.fetchReference(ref))
          .isInstanceOf(ReferenceNotFoundException.class);
      soft.assertThatCode(() -> two.fetchReference(ref)).doesNotThrowAnyException();
      soft.assertThatCode(() -> three.fetchReference(ref)).doesNotThrowAnyException();
    }
    soft.assertThat(one.fetchMany(SimpleTestObj.class, oneObjs.toArray(new ObjRef[0])))
        .hasSize(num)
        .containsOnlyNulls();
    soft.assertThat(two.fetchMany(SimpleTestObj.class, twoObjs.toArray(new ObjRef[0])))
        .doesNotContainNull()
        .extracting(SimpleTestObj::number)
        .extracting(Number::intValue)
        .containsExactly(IntStream.range(0, num).boxed().toArray(Integer[]::new));
    soft.assertThat(three.fetchMany(SimpleTestObj.class, threeObjs.toArray(new ObjRef[0])))
        .doesNotContainNull()
        .extracting(SimpleTestObj::number)
        .extracting(Number::intValue)
        .containsExactly(IntStream.range(0, num).boxed().toArray(Integer[]::new));

    backend.deleteRealms(Set.of(two.realmId(), three.realmId()));

    // realms 1+2+3 deleted
    for (var i = 0; i < num; i++) {
      var ref = "ref-" + i;
      soft.assertThatThrownBy(() -> one.fetchReference(ref))
          .isInstanceOf(ReferenceNotFoundException.class);
      soft.assertThatThrownBy(() -> two.fetchReference(ref))
          .isInstanceOf(ReferenceNotFoundException.class);
      soft.assertThatThrownBy(() -> three.fetchReference(ref))
          .isInstanceOf(ReferenceNotFoundException.class);
    }
    soft.assertThat(one.fetchMany(SimpleTestObj.class, oneObjs.toArray(new ObjRef[0])))
        .hasSize(num)
        .containsOnlyNulls();
    soft.assertThat(two.fetchMany(SimpleTestObj.class, twoObjs.toArray(new ObjRef[0])))
        .hasSize(num)
        .containsOnlyNulls();
    soft.assertThat(three.fetchMany(SimpleTestObj.class, threeObjs.toArray(new ObjRef[0])))
        .hasSize(num)
        .containsOnlyNulls();
  }

  @Test
  public void backendScan(
      @PolarisPersistence Persistence one,
      @PolarisPersistence Persistence two,
      @PolarisPersistence Persistence three) {
    soft.assertThat(one.realmId())
        .isNotEqualTo(two.realmId())
        .isNotEqualTo(persistence().realmId());

    var num = 20;

    var oneObjs = new ArrayList<ObjRef>();
    var twoObjs = new ArrayList<ObjRef>();
    var threeObjs = new ArrayList<ObjRef>();
    threeRealmsSetup(one, two, three, oneObjs, twoObjs, threeObjs, num);

    var realmRefs = new HashMap<String, List<String>>();
    var realmObjs = new HashMap<String, List<ObjRef>>();
    backend.scanBackend(
        (realm, ref, c) -> realmRefs.computeIfAbsent(realm, k -> new ArrayList<>()).add(ref),
        (realm, type, persistId, c) ->
            realmObjs
                .computeIfAbsent(realm, k -> new ArrayList<>())
                .add(objRef(type, persistId.id(), 1)));

    soft.assertThat(realmRefs).containsKeys(one.realmId(), two.realmId(), three.realmId());
    soft.assertThat(realmObjs).containsKeys(one.realmId(), two.realmId(), three.realmId());

    var refNames = IntStream.range(0, num).mapToObj(i -> "ref-" + i).toArray(String[]::new);
    soft.assertThat(realmRefs.get(one.realmId())).containsExactlyInAnyOrder(refNames);
    soft.assertThat(realmRefs.get(two.realmId())).containsExactlyInAnyOrder(refNames);
    soft.assertThat(realmRefs.get(three.realmId())).containsExactlyInAnyOrder(refNames);

    soft.assertThat(realmObjs.get(one.realmId())).containsExactlyInAnyOrderElementsOf(oneObjs);
    soft.assertThat(realmObjs.get(two.realmId())).containsExactlyInAnyOrderElementsOf(twoObjs);
    soft.assertThat(realmObjs.get(three.realmId())).containsExactlyInAnyOrderElementsOf(threeObjs);
  }

  @Test
  public void backendBulkDeletions(
      @PolarisPersistence Persistence one,
      @PolarisPersistence Persistence two,
      @PolarisPersistence Persistence three) {
    soft.assertThat(one.realmId())
        .isNotEqualTo(two.realmId())
        .isNotEqualTo(persistence().realmId());

    var num = 20;

    var oneObjs = new HashSet<ObjRef>();
    var twoObjs = new HashSet<ObjRef>();
    var threeObjs = new HashSet<ObjRef>();
    threeRealmsSetup(one, two, three, oneObjs, twoObjs, threeObjs, num);

    var refNames = IntStream.range(0, num).mapToObj(i -> "ref-" + i).collect(Collectors.toSet());
    backend.batchDeleteRefs(
        Map.of(
            one.realmId(), refNames,
            two.realmId(), refNames,
            three.realmId(), refNames));
    backend.batchDeleteObjs(
        Map.of(
            one.realmId(),
                oneObjs.stream()
                    .map(id -> PersistId.persistId(id.id(), 0))
                    .collect(Collectors.toSet()),
            two.realmId(),
                twoObjs.stream()
                    .map(id -> PersistId.persistId(id.id(), 0))
                    .collect(Collectors.toSet()),
            three.realmId(),
                threeObjs.stream()
                    .map(id -> PersistId.persistId(id.id(), 0))
                    .collect(Collectors.toSet())));

    for (var i = 0; i < num; i++) {
      var ref = "ref-" + i;
      soft.assertThatThrownBy(() -> one.fetchReference(ref))
          .describedAs("realm one: %s", ref)
          .isInstanceOf(ReferenceNotFoundException.class);
      soft.assertThatThrownBy(() -> two.fetchReference(ref))
          .describedAs("realm two: %s", ref)
          .isInstanceOf(ReferenceNotFoundException.class);
      soft.assertThatThrownBy(() -> three.fetchReference(ref))
          .describedAs("realm three: %s", ref)
          .isInstanceOf(ReferenceNotFoundException.class);
    }
    soft.assertThat(one.fetchMany(SimpleTestObj.class, oneObjs.toArray(new ObjRef[0])))
        .hasSize(num)
        .containsOnlyNulls();
    soft.assertThat(two.fetchMany(SimpleTestObj.class, twoObjs.toArray(new ObjRef[0])))
        .hasSize(num)
        .containsOnlyNulls();
    soft.assertThat(three.fetchMany(SimpleTestObj.class, threeObjs.toArray(new ObjRef[0])))
        .hasSize(num)
        .containsOnlyNulls();
  }

  private void threeRealmsSetup(
      Persistence one,
      Persistence two,
      Persistence three,
      Collection<ObjRef> oneObjs,
      Collection<ObjRef> twoObjs,
      Collection<ObjRef> threeObjs,
      int num) {
    for (var i = 0; i < num; i++) {
      one.createReference("ref-" + i, Optional.empty());
      two.createReference("ref-" + i, Optional.empty());
      three.createReference("ref-" + i, Optional.empty());

      var o =
          one.write(
              SimpleTestObj.builder().id(one.generateId()).number(i).build(), SimpleTestObj.class);
      soft.assertThat(o).isNotNull();
      oneObjs.add(objRef(o));
      o =
          two.write(
              SimpleTestObj.builder().id(two.generateId()).number(i).build(), SimpleTestObj.class);
      soft.assertThat(o).isNotNull();
      twoObjs.add(objRef(o));
      o =
          three.write(
              SimpleTestObj.builder().id(three.generateId()).number(i).build(),
              SimpleTestObj.class);
      soft.assertThat(o).isNotNull();
      threeObjs.add(objRef(o));
    }
  }
}
