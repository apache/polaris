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
package org.apache.polaris.persistence.nosql.metastore;

import static org.apache.polaris.core.entity.PolarisEntityConstants.ENTITY_BASE_LOCATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.BOOLEAN;

import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.BasePolarisMetaStoreManagerTest;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.ids.api.MonotonicClock;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@SuppressWarnings("CdiInjectionPointsInspection")
@EnableWeld
@ExtendWith(SoftAssertionsExtension.class)
public class TestPersistenceMetaStoreManager extends BasePolarisMetaStoreManagerTest {
  @WeldSetup WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  @InjectSoftAssertions SoftAssertions soft;

  @Inject
  @Identifier("nosql")
  MetaStoreManagerFactory metaStoreManagerFactory;

  @Inject PolarisConfigurationStore configurationStore;
  @Inject MonotonicClock monotonicClock;

  String realmId;
  RealmContext realmContext;

  PolarisMetaStoreManager metaStore;
  PolarisCallContext callContext;

  @Override
  protected PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    realmId = UUID.randomUUID().toString();
    realmContext = () -> realmId;

    var startTime = monotonicClock.currentTimeMillis();

    metaStoreManagerFactory.bootstrapRealms(List.of(realmId), RootCredentialsSet.fromEnvironment());

    var manager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    var session = metaStoreManagerFactory.getOrCreateSession(realmContext);

    var callCtx = new PolarisCallContext(realmContext, session, configurationStore);

    return new PolarisTestMetaStoreManager(manager, callCtx, startTime, false);
  }

  @Override
  @Disabled("No entity cache, no need to test it")
  protected void testEntityCache() {}

  @Override
  @Disabled(
      "Nothing in the code base calls 'loadTasks', the contract of that function is not what the test exercises")
  protected void testLoadTasksInParallel() {}

  @Override
  @Disabled(
      "Nothing in the code base calls 'loadTasks', the contract of that function is not what the test exercises")
  protected void testLoadTasks() {}

  @BeforeEach
  void setup() {
    this.metaStore = polarisTestMetaStoreManager.polarisMetaStoreManager();
    this.callContext = polarisTestMetaStoreManager.polarisCallContext();
  }

  @Test
  public void overlappingLocations() {
    PolarisBaseEntity catalog =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            metaStore.generateNewEntityId(callContext).getId(),
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            "overlappingLocations");
    CreateCatalogResult catalogCreated = metaStore.createCatalog(callContext, catalog, List.of());
    assertThat(catalogCreated).isNotNull();
    catalog = catalogCreated.getCatalog();

    var nsFoo =
        createEntity(
            List.of(catalog),
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.NULL_SUBTYPE,
            "ns2",
            Map.of(ENTITY_BASE_LOCATION, "s3://bucket/foo/"));
    assertThat(nsFoo).extracting(EntityResult::isSuccess, BOOLEAN).isTrue();
    var nsBar =
        createEntity(
            List.of(catalog),
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.NULL_SUBTYPE,
            "ns3",
            Map.of(ENTITY_BASE_LOCATION, "s3://bucket/bar/"));
    assertThat(nsBar).extracting(EntityResult::isSuccess, BOOLEAN).isTrue();
    var nsFoobar =
        createEntity(
            List.of(catalog),
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.NULL_SUBTYPE,
            "foobar",
            Map.of(ENTITY_BASE_LOCATION, "s3://bucket/foo/bar/"));
    assertThat(nsFoobar).extracting(EntityResult::isSuccess, BOOLEAN).isTrue();
    var nsFoobar2 =
        createEntity(
            List.of(catalog),
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.NULL_SUBTYPE,
            "foobar2",
            // Same location again
            Map.of(ENTITY_BASE_LOCATION, "s3://bucket/foo/bar/"));
    assertThat(nsFoobar2).extracting(EntityResult::isSuccess, BOOLEAN).isTrue();

    soft.assertThat(
            metaStore.hasOverlappingSiblings(
                callContext,
                new NamespaceEntity.Builder(Namespace.of("x"))
                    .setCatalogId(catalog.getId())
                    .setBaseLocation("s3://bucket/foo/")
                    .build()))
        .isPresent()
        .contains(Optional.of("s3://bucket/foo/"));

    for (var check :
        List.of(
            "s3://bucket/foo/bar",
            "s3://bucket/foo/bar/",
            "s3a://bucket/foo/bar/",
            "gs://bucket/foo/bar/")) {
      soft.assertThat(
              metaStore.hasOverlappingSiblings(
                  callContext,
                  new NamespaceEntity.Builder(Namespace.of("x"))
                      .setCatalogId(catalog.getId())
                      .setBaseLocation(check)
                      .build()))
          .isPresent()
          .contains(Optional.of("s3://bucket/foo/bar/"));
    }

    soft.assertThat(
            metaStore.hasOverlappingSiblings(
                callContext,
                new NamespaceEntity.Builder(Namespace.of("x"))
                    .setCatalogId(catalog.getId())
                    .setBaseLocation("s3://other/data/stuff/")
                    .build()))
        .isPresent()
        .contains(Optional.empty());

    // Drop one of the entities with the duplicate base location
    metaStore.dropEntityIfExists(callContext, List.of(catalog), nsFoobar.getEntity(), null, false);
    // Must still report an overlap
    soft.assertThat(
            metaStore.hasOverlappingSiblings(
                callContext,
                new NamespaceEntity.Builder(Namespace.of("x"))
                    .setCatalogId(catalog.getId())
                    .setBaseLocation("s3://bucket/foo/bar")
                    .build()))
        .isPresent()
        .contains(Optional.of("s3://bucket/foo/bar/"));

    // Drop one of the entities with the duplicate base location
    metaStore.dropEntityIfExists(callContext, List.of(catalog), nsFoobar2.getEntity(), null, false);
    // No more overlaps
    soft.assertThat(
            metaStore.hasOverlappingSiblings(
                callContext,
                new NamespaceEntity.Builder(Namespace.of("x"))
                    .setCatalogId(catalog.getId())
                    .setBaseLocation("s3://bucket/foo/bar")
                    .build()))
        .isPresent()
        .contains(Optional.empty());
  }

  EntityResult createEntity(
      List<PolarisBaseEntity> catalogPath,
      PolarisEntityType entityType,
      PolarisEntitySubType entitySubType,
      String name,
      Map<String, String> properties) {
    var entityId = metaStore.generateNewEntityId(callContext).getId();
    PolarisBaseEntity newEntity =
        new PolarisBaseEntity.Builder()
            .catalogId(catalogPath.getFirst().getId())
            .id(entityId)
            .typeCode(entityType.getCode())
            .subTypeCode(entitySubType.getCode())
            .parentId(catalogPath.getLast().getId())
            .name(name)
            .propertiesAsMap(properties)
            .build();
    @SuppressWarnings({"unchecked", "rawtypes"})
    var path = (List<PolarisEntityCore>) (List) catalogPath;
    return metaStore.createEntityIfNotExists(callContext, path, newEntity);
  }
}
