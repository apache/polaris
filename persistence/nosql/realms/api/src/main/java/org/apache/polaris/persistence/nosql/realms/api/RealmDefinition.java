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
package org.apache.polaris.persistence.nosql.realms.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.Map;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

@PolarisImmutable
@JsonSerialize(as = ImmutableRealmDefinition.class)
@JsonDeserialize(as = ImmutableRealmDefinition.class)
public interface RealmDefinition {
  String id();

  Instant created();

  Instant updated();

  RealmStatus status();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> properties();

  static ImmutableRealmDefinition.Builder builder() {
    return ImmutableRealmDefinition.builder();
  }

  @JsonIgnore
  @Value.NonAttribute
  default boolean needsBootstrap() {
    return switch (status()) {
      case CREATED, LOADING, INITIALIZING -> true;
      default -> false;
    };
  }

  /** Realms are assigned */
  enum RealmStatus {
    /**
     * The initial state of a realm is "created", which means that the realm ID is reserved, but the
     * realm is not yet usable. This state can transition to {@link #LOADING} or {@link
     * #INITIALIZING} or the realm can be directly deleted.
     */
    CREATED,
    /**
     * State used to indicate that the realm data is being imported. This state can transition to
     * {@link #ACTIVE} or {@link #INACTIVE} or {@link #PURGING}.
     */
    LOADING,
    /**
     * State used to indicate that the realm is being initialized. This state can transition to
     * {@link #ACTIVE} or {@link #INACTIVE} or {@link #PURGING}.
     */
    INITIALIZING,
    /**
     * When a realm is fully set up, its state is "active". This state can only transition to {@link
     * #INACTIVE}.
     */
    ACTIVE,
    /**
     * An {@link #ACTIVE} realm can be put into "inactive" state, which means that the realm cannot
     * be used, but it can be put back into {@link #ACTIVE} state.
     */
    INACTIVE,
    /**
     * An {@link #INACTIVE} realm can be put into "purging" state, which means that the realm's data
     * is being purged from the persistence database. This is next to the final and terminal state
     * {@link #PURGED} of a realm. Once all data of the realm has been purged, it must at least be
     * set into {@link #PURGED} status or be entirely removed.
     */
    PURGING,
    /**
     * "Purged" is the terminal state of every realm. A purged realm can be safely {@linkplain
     * RealmManagement#delete(RealmDefinition) deleted}. The difference between a "purged" realm and
     * a non-existing (deleted) realm is that the ID of a purged realm cannot be (re)used.
     */
    PURGED,
  }
}
