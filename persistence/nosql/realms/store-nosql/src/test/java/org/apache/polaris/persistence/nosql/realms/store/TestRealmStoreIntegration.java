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
package org.apache.polaris.persistence.nosql.realms.store;

import static java.lang.String.format;
import static java.time.Instant.now;
import static org.apache.polaris.persistence.nosql.api.Realms.SYSTEM_REALM_ID;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.ACTIVE;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.CREATED;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.INACTIVE;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.INITIALIZING;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.PURGED;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.PURGING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.inject.Inject;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.realms.api.RealmAlreadyExistsException;
import org.apache.polaris.persistence.nosql.realms.api.RealmDefinition;
import org.apache.polaris.persistence.nosql.realms.api.RealmExpectedStateMismatchException;
import org.apache.polaris.persistence.nosql.realms.api.RealmManagement;
import org.apache.polaris.persistence.nosql.realms.api.RealmNotFoundException;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
@EnableWeld
public class TestRealmStoreIntegration {
  @InjectSoftAssertions protected SoftAssertions soft;
  @WeldSetup WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  RealmManagement realmManagement;

  @Test
  public void nonSystemPersistence() {
    var nonSystemPersistence = mock(Persistence.class);
    var params = mock(PersistenceParams.class);
    when(nonSystemPersistence.realmId()).thenReturn("nonSystemPersistence");
    when(nonSystemPersistence.params()).thenReturn(params);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> new RealmStoreImpl(nonSystemPersistence))
        .withMessage("Realms management must happen in the ::system:: realm");
  }

  @Test
  public void createUpdateDelete() {
    var something =
        RealmDefinition.builder()
            .id("something")
            .created(now())
            .updated(now())
            .status(ACTIVE)
            .build();
    var another =
        RealmDefinition.builder()
            .id("another")
            .created(now())
            .updated(now())
            .status(ACTIVE)
            .build();

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> realmManagement.create(SYSTEM_REALM_ID))
        .withMessage("Invalid realm ID '%s'", SYSTEM_REALM_ID);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> realmManagement.create("::something"))
        .withMessage("Invalid realm ID '%s'", "::something");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                realmManagement.update(
                    something.withId("::something"), something.withId("::something")))
        .withMessage("Invalid realm ID '%s'", "::something");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> realmManagement.delete(something.withId("::something")))
        .withMessage("Invalid realm ID '%s'", "::something");

    // empty index
    soft.assertThatThrownBy(
            () ->
                realmManagement.update(
                    something, RealmDefinition.builder().from(something).build()))
        .isInstanceOf(RealmNotFoundException.class)
        .hasMessage("No realm with ID 'something' exists");
    soft.assertThatThrownBy(() -> realmManagement.delete(something.withStatus(PURGED)))
        .hasMessage("No realm with ID 'something' exists");

    var created = realmManagement.create(something.id());
    soft.assertThat(created).extracting(RealmDefinition::id).isEqualTo(something.id());
    soft.assertThatThrownBy(() -> realmManagement.create(something.id()))
        .isInstanceOf(RealmAlreadyExistsException.class)
        .hasMessage("A realm with ID 'something' already exists");
    var gotOpt = realmManagement.get(something.id());
    soft.assertThat(gotOpt).contains(created);
    var got = gotOpt.orElseThrow();

    var createdAnother = realmManagement.create(another.id());
    soft.assertThat(createdAnother).extracting(RealmDefinition::id).isEqualTo(another.id());

    // RealmsStateObj present
    soft.assertThatThrownBy(
            () -> realmManagement.update(something.withId("foo"), something.withId("foo")))
        .isInstanceOf(RealmNotFoundException.class)
        .hasMessage("No realm with ID 'foo' exists");
    soft.assertThatThrownBy(
            () -> realmManagement.delete(something.withId("foo").withStatus(PURGED)))
        .isInstanceOf(RealmNotFoundException.class)
        .hasMessage("No realm with ID 'foo' exists");

    // Update with different realm-IDs (duh!)
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                realmManagement.update(
                    got, RealmDefinition.builder().from(got).id("something-else").build()));
    // Update with different expected state
    soft.assertThatThrownBy(
            () ->
                realmManagement.update(
                    RealmDefinition.builder().from(got).putProperty("foo", "bar").build(),
                    RealmDefinition.builder().from(got).putProperty("meep", "meep").build()))
        .isInstanceOf(RealmExpectedStateMismatchException.class)
        .hasMessage("Realm '%s' does not match the expected state", got.id());

    var updated =
        realmManagement.update(
            got, RealmDefinition.builder().from(got).putProperty("foo", "bar").build());
    soft.assertThat(updated)
        .extracting(RealmDefinition::id, RealmDefinition::properties)
        .containsExactly(something.id(), Map.of("foo", "bar"));
    var got2Opt = realmManagement.get(something.id());
    soft.assertThat(got2Opt).contains(updated);
    var got2 = got2Opt.orElseThrow();

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> realmManagement.delete(got2))
        .withMessage("Realm '%s' must be in state PURGED to be deleted", got2.id());
    var initializing =
        realmManagement.update(
            got2, RealmDefinition.builder().from(got2).status(INITIALIZING).build());
    var active =
        realmManagement.update(
            initializing, RealmDefinition.builder().from(initializing).status(ACTIVE).build());
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> realmManagement.delete(active))
        .withMessage("Realm '%s' must be in state PURGED to be deleted", active.id());
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                realmManagement.update(
                    active, RealmDefinition.builder().from(active).status(CREATED).build()))
        .withMessage(
            "Invalid realm state transition from ACTIVE to CREATED for realm '%s'", active.id());
    var inactive =
        realmManagement.update(
            active, RealmDefinition.builder().from(got2).status(INACTIVE).build());
    var purging =
        realmManagement.update(
            inactive, RealmDefinition.builder().from(inactive).status(PURGING).build());
    soft.assertThat(purging).extracting(RealmDefinition::status).isSameAs(PURGING);
    var purged =
        realmManagement.update(
            purging, RealmDefinition.builder().from(inactive).status(PURGED).build());
    soft.assertThat(purged).extracting(RealmDefinition::status).isSameAs(PURGED);
    soft.assertThatCode(() -> realmManagement.delete(purged)).doesNotThrowAnyException();

    soft.assertThat(realmManagement.get(something.id())).isEmpty();

    soft.assertThat(realmManagement.get(another.id())).contains(createdAnother);
  }

  @Test
  public void list() {
    var toRealmId = (IntFunction<String>) i -> format("realm_%05d", i);

    // Check that the bucketizing used in .list() implementation works correctly. Need to iterate
    // more often than the (default) PersistenceParams.bucketizedBulkFetchSize() value.
    for (int i = 0; i < 47; i++) {
      try (var realms = realmManagement.list()) {
        var realmDefs = realms.toList();
        soft.assertThat(realmDefs)
            .describedAs("i=%d", i)
            .hasSize(i)
            .map(RealmDefinition::id)
            .containsExactlyElementsOf(IntStream.range(0, i).mapToObj(toRealmId).toList());

        realmManagement.create(toRealmId.apply(i));
      }
    }
  }
}
