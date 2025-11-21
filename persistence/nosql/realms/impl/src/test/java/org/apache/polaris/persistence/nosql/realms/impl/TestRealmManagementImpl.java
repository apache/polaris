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
package org.apache.polaris.persistence.nosql.realms.impl;

import static java.time.Instant.now;
import static org.apache.polaris.persistence.nosql.api.Realms.SYSTEM_REALM_ID;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.ACTIVE;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.CREATED;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.INACTIVE;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.INITIALIZING;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.PURGED;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.PURGING;

import java.time.Instant;
import java.util.Map;
import org.apache.polaris.persistence.nosql.realms.api.RealmAlreadyExistsException;
import org.apache.polaris.persistence.nosql.realms.api.RealmDefinition;
import org.apache.polaris.persistence.nosql.realms.api.RealmExpectedStateMismatchException;
import org.apache.polaris.persistence.nosql.realms.api.RealmNotFoundException;
import org.apache.polaris.persistence.nosql.realms.spi.MockRealmStore;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestRealmManagementImpl {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void createUpdateDelete() {
    var realmsManagement = new RealmManagementImpl(new MockRealmStore(), Instant::now);

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
        .isThrownBy(() -> realmsManagement.create(SYSTEM_REALM_ID))
        .withMessage("Invalid realm ID '%s'", SYSTEM_REALM_ID);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> realmsManagement.create("::something"))
        .withMessage("Invalid realm ID '%s'", "::something");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                realmsManagement.update(
                    something.withId("::something"), something.withId("::something")))
        .withMessage("Invalid realm ID '%s'", "::something");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> realmsManagement.delete(something.withId("::something")))
        .withMessage("Invalid realm ID '%s'", "::something");

    // empty index
    soft.assertThatThrownBy(
            () ->
                realmsManagement.update(
                    something, RealmDefinition.builder().from(something).build()))
        .isInstanceOf(RealmNotFoundException.class)
        .hasMessage("No realm with ID 'something' exists");
    soft.assertThatThrownBy(() -> realmsManagement.delete(something.withStatus(PURGED)))
        .hasMessage("No realm with ID 'something' exists");

    var created = realmsManagement.create(something.id());

    soft.assertThat(created).extracting(RealmDefinition::id).isEqualTo(something.id());
    soft.assertThatThrownBy(() -> realmsManagement.create(something.id()))
        .isInstanceOf(RealmAlreadyExistsException.class)
        .hasMessage("A realm with ID 'something' already exists");
    var gotOpt = realmsManagement.get(something.id());
    soft.assertThat(gotOpt).contains(created);
    var got = gotOpt.orElse(null);

    var createdAnother = realmsManagement.create(another.id());
    soft.assertThat(createdAnother).extracting(RealmDefinition::id).isEqualTo(another.id());

    // RealmsStateObj present
    soft.assertThatThrownBy(
            () -> realmsManagement.update(something.withId("foo"), something.withId("foo")))
        .isInstanceOf(RealmNotFoundException.class)
        .hasMessage("No realm with ID 'foo' exists");
    soft.assertThatThrownBy(
            () -> realmsManagement.delete(something.withId("foo").withStatus(PURGED)))
        .isInstanceOf(RealmNotFoundException.class)
        .hasMessage("No realm with ID 'foo' exists");

    // Update with different realm-IDs (duh!)
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                realmsManagement.update(
                    got, RealmDefinition.builder().from(got).id("something-else").build()));
    // Update with different expected state
    soft.assertThatThrownBy(
            () ->
                realmsManagement.update(
                    RealmDefinition.builder().from(got).putProperty("foo", "bar").build(),
                    RealmDefinition.builder().from(got).putProperty("meep", "meep").build()))
        .isInstanceOf(RealmExpectedStateMismatchException.class)
        .hasMessage("Realm '%s' does not match the expected state", created.id());

    var updated =
        realmsManagement.update(
            got, RealmDefinition.builder().from(got).putProperty("foo", "bar").build());
    soft.assertThat(updated)
        .extracting(RealmDefinition::id, RealmDefinition::properties)
        .containsExactly(something.id(), Map.of("foo", "bar"));
    var got2Opt = realmsManagement.get(something.id());
    soft.assertThat(got2Opt).contains(updated);
    var got2 = got2Opt.orElse(null);

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> realmsManagement.delete(got2))
        .withMessage("Realm '%s' must be in state PURGED to be deleted", got2.id());
    var initializing =
        realmsManagement.update(
            got2, RealmDefinition.builder().from(got2).status(INITIALIZING).build());
    var active =
        realmsManagement.update(
            initializing, RealmDefinition.builder().from(initializing).status(ACTIVE).build());
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> realmsManagement.delete(active))
        .withMessage("Realm '%s' must be in state PURGED to be deleted", active.id());
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                realmsManagement.update(
                    active, RealmDefinition.builder().from(active).status(CREATED).build()))
        .withMessage(
            "Invalid realm state transition from ACTIVE to CREATED for realm '%s'", active.id());
    var inactive =
        realmsManagement.update(
            active, RealmDefinition.builder().from(got2).status(INACTIVE).build());
    var purging =
        realmsManagement.update(
            inactive, RealmDefinition.builder().from(inactive).status(PURGING).build());
    soft.assertThat(purging).extracting(RealmDefinition::status).isSameAs(PURGING);
    var purged =
        realmsManagement.update(
            purging, RealmDefinition.builder().from(inactive).status(PURGED).build());
    soft.assertThat(purged).extracting(RealmDefinition::status).isSameAs(PURGED);
    soft.assertThatCode(() -> realmsManagement.delete(purged)).doesNotThrowAnyException();

    soft.assertThat(realmsManagement.get(something.id())).isEmpty();

    soft.assertThat(realmsManagement.get(another.id())).contains(createdAnother);
  }
}
