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
package org.apache.polaris.persistence.nosql.maintenance.api;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.maintenance.spi.ObjTypeRetainedIdentifier;
import org.apache.polaris.persistence.nosql.maintenance.spi.PerRealmRetainedIdentifier;
import org.apache.polaris.persistence.nosql.maintenance.spi.RetainedCollector;
import org.immutables.value.Value;

@PolarisImmutable
@JsonSerialize(as = ImmutableMaintenanceRunInformation.class)
@JsonDeserialize(as = ImmutableMaintenanceRunInformation.class)
public interface MaintenanceRunInformation {

  Instant started();

  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Instant> finished();

  @Value.Default
  default boolean success() {
    return false;
  }

  /** Human-readable status message. */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> statusMessage();

  /** Human-readable detailed information, possibly including technical error information. */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> detailedInformation();

  Optional<MaintenanceStats> referenceStats();

  Optional<MaintenanceStats> objStats();

  Map<String, MaintenanceStats> perRealmReferenceStats();

  Map<String, Map<String, MaintenanceStats>> perRealmPerObjTypeStats();

  /**
   * Number of realms that were directly purges, if the {@linkplain Backend backend} {@linkplain
   * Backend#supportsRealmDeletion() supports} this.
   */
  OptionalInt purgedRealms();

  /** Number of invocations of {@link RetainedCollector#retainObject(ObjRef)}. */
  OptionalLong identifiedObjs();

  /** Number of invocations of {@link RetainedCollector#retainReference(String)}. */
  OptionalLong identifiedReferences();

  static ImmutableMaintenanceRunInformation.Builder builder() {
    return ImmutableMaintenanceRunInformation.builder();
  }

  @PolarisImmutable
  @JsonSerialize(as = ImmutableMaintenanceStats.class)
  @JsonDeserialize(as = ImmutableMaintenanceStats.class)
  interface MaintenanceStats {

    static ImmutableMaintenanceStats.Builder builder() {
      return ImmutableMaintenanceStats.builder();
    }

    /**
     * Number of scanned items.
     *
     * <p>If a persisted object has been persisted using multiple parts, each part is counted.
     */
    OptionalLong scanned();

    /**
     * Number of scanned items that were retained, because those were {@linkplain
     * RetainedCollector#retainObject(ObjRef) indicated} to be retained by a {@linkplain
     * PerRealmRetainedIdentifier realm identifier} or {@linkplain ObjTypeRetainedIdentifier
     * obj-type identifier}.
     *
     * <p>If a persisted object has been persisted using multiple parts, each part is counted.
     */
    OptionalLong retained();

    /**
     * Number of items that were written after the {@linkplain
     * MaintenanceConfig#createdAtGraceTime() calculated grace time}.
     *
     * <p>If a persisted object has been persisted using multiple parts, each part is counted.
     */
    OptionalLong newer();

    /**
     * Number of scanned items that have been purged.
     *
     * <p>If a persisted object has been persisted using multiple parts, each part is counted.
     */
    OptionalLong purged();
  }
}
