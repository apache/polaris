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
package org.apache.polaris.persistence.nosql.quarkus.backend;

import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.MeterFilter;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.FetchedObj;
import org.apache.polaris.persistence.nosql.api.backend.PersistId;
import org.apache.polaris.persistence.nosql.api.backend.WriteObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;

/** Provides telemetry and tracing for all persistence backend operations. */
@ApplicationScoped
@Default
public class ObservingBackend implements Backend {
  public static final String TELEMETRY_PREFIX = "polaris.persistence";

  private final Backend backend;

  public ObservingBackend(@NotObserved Backend backend) {
    this.backend = backend;
  }

  @Nonnull
  @Override
  public String type() {
    return backend.type();
  }

  @Nonnull
  @Override
  public Persistence newPersistence(
      Function<Backend, Backend> backendWrapper,
      @Nonnull PersistenceParams persistenceParams,
      String realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator) {
    return backend.newPersistence(
        backendWrapper, persistenceParams, realmId, monotonicClock, idGenerator);
  }

  @Override
  public boolean supportsRealmDeletion() {
    return backend.supportsRealmDeletion();
  }

  @Override
  public void close() throws Exception {
    backend.close();
  }

  @WithSpan(TELEMETRY_PREFIX + ".setupSchema")
  @Counted(TELEMETRY_PREFIX + ".setupSchema")
  @Timed(value = TELEMETRY_PREFIX + ".setupSchema", histogram = true)
  @Override
  public Optional<String> setupSchema() {
    return backend.setupSchema();
  }

  @WithSpan(TELEMETRY_PREFIX + ".deleteRealms")
  @Counted(TELEMETRY_PREFIX + ".deleteRealms")
  @Timed(value = TELEMETRY_PREFIX + ".deleteRealms", histogram = true)
  @Override
  public void deleteRealms(@SpanAttribute("realms") Set<String> realmIds) {
    backend.deleteRealms(realmIds);
  }

  @WithSpan(TELEMETRY_PREFIX + ".batchDeleteRefs")
  @Counted(TELEMETRY_PREFIX + ".batchDeleteRefs")
  @Timed(value = TELEMETRY_PREFIX + ".batchDeleteRefs", histogram = true)
  @Override
  public void batchDeleteRefs(Map<String, Set<String>> realmRefs) {
    backend.batchDeleteRefs(realmRefs);
  }

  @WithSpan(TELEMETRY_PREFIX + ".batchDeleteObjs")
  @Counted(TELEMETRY_PREFIX + ".batchDeleteObjs")
  @Timed(value = TELEMETRY_PREFIX + ".batchDeleteObjs", histogram = true)
  @Override
  public void batchDeleteObjs(Map<String, Set<PersistId>> realmObjs) {
    backend.batchDeleteObjs(realmObjs);
  }

  @WithSpan(TELEMETRY_PREFIX + ".scanBackend")
  @Counted(TELEMETRY_PREFIX + ".scanBackend")
  @Timed(value = TELEMETRY_PREFIX + ".scanBackend", histogram = true, longTask = true)
  @Override
  public void scanBackend(
      @Nonnull ReferenceScanCallback referenceConsumer, @Nonnull ObjScanCallback objConsumer) {
    backend.scanBackend(referenceConsumer, objConsumer);
  }

  @WithSpan(TELEMETRY_PREFIX + ".createReference")
  @Counted(TELEMETRY_PREFIX + ".createReference")
  @Timed(value = TELEMETRY_PREFIX + ".createReference", histogram = true)
  @Override
  public boolean createReference(
      @SpanAttribute("realm-id") @Nonnull String realmId, @Nonnull Reference newRef) {
    return backend.createReference(realmId, newRef);
  }

  @WithSpan(TELEMETRY_PREFIX + ".createReferencesSilent")
  @Counted(TELEMETRY_PREFIX + ".createReferencesSilent")
  @Timed(value = TELEMETRY_PREFIX + ".createReferencesSilent", histogram = true)
  @Override
  public void createReferences(
      @SpanAttribute("realm-id") @Nonnull String realmId, @Nonnull List<Reference> newRefs) {
    backend.createReferences(realmId, newRefs);
  }

  @WithSpan(TELEMETRY_PREFIX + ".updateReference")
  @Counted(TELEMETRY_PREFIX + ".updateReference")
  @Timed(value = TELEMETRY_PREFIX + ".updateReference", histogram = true)
  @Override
  public boolean updateReference(
      @SpanAttribute("realm-id") @Nonnull String realmId,
      @Nonnull Reference updatedRef,
      @Nonnull Optional<ObjRef> expectedPointer) {
    return backend.updateReference(realmId, updatedRef, expectedPointer);
  }

  @WithSpan(TELEMETRY_PREFIX + ".fetchReference")
  @Counted(TELEMETRY_PREFIX + ".fetchReference")
  @Timed(value = TELEMETRY_PREFIX + ".fetchReference", histogram = true)
  @Nonnull
  @Override
  public Reference fetchReference(
      @SpanAttribute("realm-id") @Nonnull String realmId, @Nonnull String name) {
    return backend.fetchReference(realmId, name);
  }

  @WithSpan(TELEMETRY_PREFIX + ".fetch")
  @Counted(TELEMETRY_PREFIX + ".fetch")
  @Timed(value = TELEMETRY_PREFIX + ".fetch", histogram = true)
  @Nonnull
  @Override
  public Map<PersistId, FetchedObj> fetch(
      @SpanAttribute("realm-id") @Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    return backend.fetch(realmId, ids);
  }

  @WithSpan(TELEMETRY_PREFIX + ".write")
  @Counted(TELEMETRY_PREFIX + ".write")
  @Timed(value = TELEMETRY_PREFIX + ".write", histogram = true)
  @Override
  public void write(
      @SpanAttribute("realm-id") @Nonnull String realmId, @Nonnull List<WriteObj> writes) {
    backend.write(realmId, writes);
  }

  @WithSpan(TELEMETRY_PREFIX + ".delete")
  @Counted(TELEMETRY_PREFIX + ".delete")
  @Timed(value = TELEMETRY_PREFIX + ".delete", histogram = true)
  @Override
  public void delete(
      @SpanAttribute("realm-id") @Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    backend.delete(realmId, ids);
  }

  @WithSpan(TELEMETRY_PREFIX + ".conditionalInsert")
  @Counted(TELEMETRY_PREFIX + ".conditionalInsert")
  @Timed(value = TELEMETRY_PREFIX + ".conditionalInsert", histogram = true)
  @Override
  public boolean conditionalInsert(
      @SpanAttribute("realm-id") @Nonnull String realmId,
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String versionToken,
      @Nonnull byte[] serializedValue) {
    return backend.conditionalInsert(
        realmId, objTypeId, persistId, createdAtMicros, versionToken, serializedValue);
  }

  @WithSpan(TELEMETRY_PREFIX + ".conditionalUpdate")
  @Counted(TELEMETRY_PREFIX + ".conditionalUpdate")
  @Timed(value = TELEMETRY_PREFIX + ".conditionalUpdate", histogram = true)
  @Override
  public boolean conditionalUpdate(
      @SpanAttribute("realm-id") @Nonnull String realmId,
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String updateToken,
      @Nonnull String expectedToken,
      @Nonnull byte[] serializedValue) {
    return backend.conditionalUpdate(
        realmId,
        objTypeId,
        persistId,
        createdAtMicros,
        updateToken,
        expectedToken,
        serializedValue);
  }

  @WithSpan(TELEMETRY_PREFIX + ".conditionalDelete")
  @Counted(TELEMETRY_PREFIX + ".conditionalDelete")
  @Timed(value = TELEMETRY_PREFIX + ".conditionalDelete", histogram = true)
  @Override
  public boolean conditionalDelete(
      @SpanAttribute("realm-id") @Nonnull String realmId,
      @Nonnull PersistId persistId,
      @Nonnull String expectedToken) {
    return backend.conditionalDelete(realmId, persistId, expectedToken);
  }

  @Produces
  @Singleton
  public MeterFilter renameApplicationMeters() {
    return new MeterFilter() {
      @Override
      @Nonnull
      public Meter.Id map(@Nonnull Meter.Id id) {
        var tags = id.getTags();
        var tag = Tag.of("class", ObservingBackend.class.getName());
        if (tags.contains(tag)) {
          // drop the 'class' tag, but leave the 'method' tag
          tags = tags.stream().filter(t -> !"class".equals(t.getKey())).toList();
          return id.replaceTags(tags);
        }
        return id;
      }
    };
  }
}
