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

package org.apache.polaris.persistence.nosql.coretypes.mapping;

import static java.lang.String.format;
import static org.apache.polaris.core.entity.PolarisEntityConstants.getStorageConfigInfoPropertyName;
import static org.apache.polaris.core.entity.PolarisEntityConstants.getStorageIntegrationIdentifierPropertyName;
import static org.apache.polaris.persistence.nosql.api.obj.ObjSerializationHelper.contextualReader;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.BaseTestMapping.MappingSample.entityWithInternalProperties;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.BaseTestMapping.MappingSample.entityWithInternalProperty;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static tools.jackson.databind.MapperFeature.DEFAULT_VIEW_INCLUSION;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogBaseObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStorageObj;
import org.apache.polaris.persistence.nosql.coretypes.content.ContentObj;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@ParameterizedClass
@ExtendWith(SoftAssertionsExtension.class)
public abstract class BaseTestMapping {
  @InjectSoftAssertions public SoftAssertions soft;

  @Parameter static BaseTestParameter parameter;

  public static ObjectMapper objectMapper;

  @BeforeAll
  public static void beforeAll() {
    objectMapper =
        JsonMapper.builder()
            .findAndAddModules()
            .disable(FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DEFAULT_VIEW_INCLUSION)
            .build();
  }

  public BaseMapping<?, ?> mapping;

  @BeforeEach
  public void setUp() {
    mapping = EntityObjMappings.byEntityType(parameter.entityType);
  }

  public abstract Class<? extends ObjBase> baseObjClass();

  public abstract ObjType containerObjType();

  public abstract String refName();

  public abstract boolean isCatalogRelated();

  public abstract boolean isCatalogContent();

  public abstract boolean isWithStorage();

  @Test
  public void containerAndPoly() {
    if (containerObjType() != null) {
      soft.assertThat(mapping.containerObjType).isSameAs(containerObjType());
      soft.assertThat(EntityObjMappings.containerTypeForEntityType(mapping.entityType()))
          .isSameAs(containerObjType().targetClass());
      soft.assertThat(mapping.objTypeForSubType(parameter.subType)).isSameAs(parameter.objType);
    } else {
      soft.assertThat(mapping.objType).isSameAs(parameter.objType);
      soft.assertThatIllegalArgumentException()
          .isThrownBy(() -> EntityObjMappings.containerTypeForEntityType(mapping.entityType()));
    }
    if (baseObjClass() != null) {
      if (containerObjType() != null) {
        soft.assertThat(mapping.objTypeForSubType(parameter.subType).targetClass())
            .isAssignableTo(baseObjClass());
        soft.assertThat(mapping.objType).isNull();
      } else {
        soft.assertThat(mapping.objType).isNotNull();
        soft.assertThat(mapping.objType.targetClass()).isAssignableTo(baseObjClass());
      }
    } else {
      soft.assertThat(mapping.objType).isNotNull();
    }
  }

  @Test
  public void catalogStorage() {
    if (isWithStorage()) {
      soft.assertThat(parameter.objType.targetClass()).isAssignableTo(CatalogStorageObj.class);
    }
    if (isCatalogContent()) {
      soft.assertThat(parameter.objType.targetClass()).isAssignableTo(ContentObj.class);
      soft.assertThat(mapping).isInstanceOf(BaseCatalogContentMapping.class);
    }
    if (isCatalogRelated()) {
      soft.assertThat(parameter.objType.targetClass()).isAssignableTo(CatalogBaseObj.class);
      soft.assertThat(mapping).isInstanceOf(BaseCatalogMapping.class);
      soft.assertThat(LongStream.of(1, 42, 1000))
          .allSatisfy(
              c -> assertThatCode(() -> mapping.checkCatalogId(c)).doesNotThrowAnyException());
      soft.assertThat(LongStream.of(0, -1, -42, -1000))
          .allSatisfy(
              c ->
                  assertThatIllegalArgumentException().isThrownBy(() -> mapping.checkCatalogId(c)));
    } else {
      soft.assertThatCode(() -> mapping.checkCatalogId(0)).doesNotThrowAnyException();
      soft.assertThat(LongStream.of(1, 42, 1000, -1, -42, -1000))
          .allSatisfy(
              c ->
                  assertThatIllegalArgumentException().isThrownBy(() -> mapping.checkCatalogId(c)));
    }
  }

  @Test
  public void basics() {
    soft.assertThat(mapping.entityType()).isEqualTo(parameter.entityType);
    soft.assertThat(mapping.catalogContent()).isEqualTo(isCatalogContent());

    soft.assertThat(EntityObjMappings.isCatalogContent(parameter.entityType))
        .isEqualTo(isCatalogContent());
    soft.assertThat(EntityObjMappings.isCatalogContent(parameter.entityType.getCode()))
        .isEqualTo(isCatalogContent());

    soft.assertThatCode(() -> mapping.validateSubType(parameter.subType))
        .doesNotThrowAnyException();
  }

  @Test
  public void referenceName() {
    soft.assertThat(mapping.refName).isEqualTo(refName());

    if (isCatalogRelated()) {
      soft.assertThat(mapping.refNameForCatalog(1)).isEqualTo(format(refName(), 1L));
      soft.assertThatIllegalArgumentException().isThrownBy(() -> mapping.refNameForCatalog(0));

      // Static via EntityObjMappings
      soft.assertThat(EntityObjMappings.referenceName(mapping.entityType(), 1))
          .isEqualTo(format(refName(), 1L));
      soft.assertThat(EntityObjMappings.referenceName(mapping.entityType(), OptionalLong.of(1)))
          .isEqualTo(format(refName(), 1L));
      soft.assertThatIllegalArgumentException()
          .isThrownBy(() -> EntityObjMappings.referenceName(mapping.entityType(), 0));
      soft.assertThatIllegalArgumentException()
          .isThrownBy(
              () -> EntityObjMappings.referenceName(mapping.entityType(), OptionalLong.of(0)));
      soft.assertThatIllegalArgumentException()
          .isThrownBy(
              () -> EntityObjMappings.referenceName(mapping.entityType(), OptionalLong.empty()));
    } else {
      soft.assertThat(mapping.refNameForCatalog(0)).isEqualTo(refName());
      soft.assertThatIllegalArgumentException().isThrownBy(() -> mapping.refNameForCatalog(1));

      // Static via EntityObjMappings
      soft.assertThat(EntityObjMappings.referenceName(mapping.entityType(), 0))
          .isEqualTo(refName());
      soft.assertThat(EntityObjMappings.referenceName(mapping.entityType(), OptionalLong.of(0)))
          .isEqualTo(refName());
      soft.assertThat(EntityObjMappings.referenceName(mapping.entityType(), OptionalLong.empty()))
          .isEqualTo(refName());
      soft.assertThatIllegalArgumentException()
          .isThrownBy(() -> EntityObjMappings.referenceName(mapping.entityType(), 1));
      soft.assertThatIllegalArgumentException()
          .isThrownBy(
              () -> EntityObjMappings.referenceName(mapping.entityType(), OptionalLong.of(1)));
    }
  }

  @ParameterizedTest
  @MethodSource("objEntityMapping")
  public void objEntityMapping(MappingSample sample) throws IOException {
    var catalogId = sample.entity().getCatalogId();
    soft.assertThatCode(() -> mapping.checkCatalogId(catalogId)).doesNotThrowAnyException();

    var entity = mapping.mapToEntity(sample.obj(), catalogId);
    var entityToObjBuilder = mapping.mapToObj(entity, Optional.empty()).id(sample.obj().id());
    if (sample.obj() instanceof BaseCommitObj baseCommitObj) {
      //noinspection RedundantClassCall
      BaseCommitObj.Builder.class
          .cast(entityToObjBuilder)
          .seq(baseCommitObj.seq())
          .tail(baseCommitObj.tail())
          .build();
    }
    var entityToObj = entityToObjBuilder.build();

    var objectWriter = objectMapper.writer().withView(Object.class);
    var objectReader =
        contextualReader(
            objectMapper,
            sample.obj().type(),
            sample.obj().id(),
            sample.obj().numParts(),
            null,
            0L);

    soft.assertThat(entityToObj).isEqualTo(sample.obj());
    entityEquals(entity, sample.entity());

    // serialization roundtrip
    var objAsJson = objectWriter.writeValueAsString(entityToObj);
    var reserialized = objectReader.readValue(objAsJson);
    soft.assertThat(reserialized)
        .isEqualTo(entityToObj)
        .isInstanceOf(mapping.objTypeForSubType(entity.getSubType()).targetClass());

    // Via EntityObjMapping
    entity = EntityObjMappings.mapToEntity(sample.obj(), catalogId);
    entityToObjBuilder = EntityObjMappings.mapToObj(entity, Optional.empty()).id(sample.obj().id());
    if (sample.obj() instanceof BaseCommitObj baseCommitObj) {
      //noinspection RedundantClassCall
      BaseCommitObj.Builder.class
          .cast(entityToObjBuilder)
          .seq(baseCommitObj.seq())
          .tail(baseCommitObj.tail())
          .build();
    }
    entityToObj = entityToObjBuilder.build();

    soft.assertThat(entityToObj).isEqualTo(sample.obj());
    entityEquals(entity, sample.entity());

    // serialization roundtrip
    objAsJson = objectWriter.writeValueAsString(entityToObj);
    reserialized = objectReader.readValue(objAsJson);
    soft.assertThat(reserialized)
        .isEqualTo(entityToObj)
        .isInstanceOf(mapping.objTypeForSubType(entity.getSubType()).targetClass());

    //
    var entityNameLookupRecord =
        EntityObjMappings.mapToEntityNameLookupRecord(sample.obj(), catalogId);
    soft.assertThat(entityNameLookupRecord)
        .extracting(
            EntityNameLookupRecord::getCatalogId,
            EntityNameLookupRecord::getParentId,
            EntityNameLookupRecord::getType,
            EntityNameLookupRecord::getTypeCode,
            EntityNameLookupRecord::getSubType,
            EntityNameLookupRecord::getSubTypeCode,
            EntityNameLookupRecord::getName,
            EntityNameLookupRecord::getId)
        .containsExactly(
            catalogId,
            entity.getParentId(),
            entity.getType(),
            entity.getTypeCode(),
            entity.getSubType(),
            entity.getSubTypeCode(),
            entity.getName(),
            entity.getId());

    var entitySubType =
        EntityObjMappings.entitySubTypeCodeFromObjType(
            ObjRef.objRef(sample.obj().type(), sample.obj().id()));
    soft.assertThat(entitySubType).isEqualTo(Integer.toString(sample.entity().getSubTypeCode()));
  }

  public void entityEquals(PolarisBaseEntity a, PolarisBaseEntity b) {
    soft.assertThat(a.getClass()).isEqualTo(b.getClass());
    soft.assertThat(a)
        .extracting(
            PolarisBaseEntity::getType,
            PolarisBaseEntity::getTypeCode,
            PolarisBaseEntity::getSubType,
            PolarisBaseEntity::getSubTypeCode,
            PolarisBaseEntity::getCreateTimestamp,
            PolarisBaseEntity::getLastUpdateTimestamp,
            PolarisBaseEntity::getGrantRecordsVersion,
            PolarisBaseEntity::getEntityVersion,
            PolarisBaseEntity::getParentId,
            PolarisBaseEntity::getPropertiesAsMap,
            PolarisBaseEntity::getInternalPropertiesAsMap)
        .containsExactly(
            b.getType(),
            b.getTypeCode(),
            b.getSubType(),
            b.getSubTypeCode(),
            b.getCreateTimestamp(),
            b.getLastUpdateTimestamp(),
            b.getGrantRecordsVersion(),
            b.getEntityVersion(),
            b.getParentId(),
            b.getPropertiesAsMap(),
            b.getInternalPropertiesAsMap());
  }

  static Stream<MappingSample> objEntityMapping() {
    var entity = buildWithDefaults(parameter.entityBuilder()).build();
    var obj = buildWithDefaults(parameter.objBuilder()).build();
    return parameter
        .typeVariations(new MappingSample(obj, entity))
        .flatMap(
            base ->
                Stream.of(
                    //
                    base.varyCreatedTimestamp(parameter.objBuilder()),
                    base.varyUpdatedTimestamp(parameter.objBuilder()),
                    base.varyProperties(parameter.objBuilder()),
                    base.varyInternalProperties(parameter.objBuilder()),
                    base.varyEntityVersion(parameter.objBuilder()),
                    base.varyParentId(parameter.objBuilder()),
                    //
                    base.varyCreatedTimestamp(parameter.objBuilder())
                        .varyUpdatedTimestamp(parameter.objBuilder())
                        .varyProperties(parameter.objBuilder())
                        .varyInternalProperties(parameter.objBuilder())
                        .varyEntityVersion(parameter.objBuilder())
                        .varyParentId(parameter.objBuilder())
                    //
                    ));
  }

  protected PolarisBaseEntity.Builder entityBuilder(
      PolarisEntitySubType subType, long entityId, String name) {
    return new PolarisBaseEntity.Builder()
        .typeCode(parameter.entityType.getCode())
        .subTypeCode(subType.getCode())
        .id(entityId)
        .name(name);
  }

  public record MappingSample(ObjBase obj, PolarisBaseEntity entity) {
    public MappingSample varyCreatedTimestamp(ObjBase.Builder<?, ?> builder) {
      return new MappingSample(
          builder.from(obj).createTimestamp(Instant.ofEpochSecond(888888888L)).build(),
          new PolarisBaseEntity.Builder(entity)
              .createTimestamp(Instant.ofEpochSecond(888888888L).toEpochMilli())
              .build());
    }

    public MappingSample varyUpdatedTimestamp(ObjBase.Builder<?, ?> builder) {
      return new MappingSample(
          builder.from(obj).updateTimestamp(Instant.ofEpochSecond(9999999999L)).build(),
          new PolarisBaseEntity.Builder(entity)
              .lastUpdateTimestamp(Instant.ofEpochSecond(9999999999L).toEpochMilli())
              .build());
    }

    public static PolarisBaseEntity entityWithProperty(
        PolarisBaseEntity entity, String key, String value) {
      var newProps = new HashMap<>(entity.getPropertiesAsMap());
      newProps.put(key, value);
      return new PolarisBaseEntity.Builder(entity).propertiesAsMap(newProps).build();
    }

    public static PolarisBaseEntity entityWithProperties(
        PolarisBaseEntity entity, Map<String, String> properties) {
      var newProps = new HashMap<>(entity.getPropertiesAsMap());
      newProps.putAll(properties);
      return new PolarisBaseEntity.Builder(entity).propertiesAsMap(newProps).build();
    }

    public static PolarisBaseEntity entityWithInternalProperty(
        PolarisBaseEntity entity, String key, String value) {
      var newProps = new HashMap<>(entity.getInternalPropertiesAsMap());
      newProps.put(key, value);
      return new PolarisBaseEntity.Builder(entity).internalPropertiesAsMap(newProps).build();
    }

    public static PolarisBaseEntity entityWithInternalProperties(
        PolarisBaseEntity entity, Map<String, String> properties) {
      var newProps = new HashMap<>(entity.getInternalPropertiesAsMap());
      newProps.putAll(properties);
      return new PolarisBaseEntity.Builder(entity).internalPropertiesAsMap(newProps).build();
    }

    public MappingSample varyProperties(ObjBase.Builder<?, ?> builder) {
      return new MappingSample(
          builder.from(obj).putProperty("foo", "bar").putProperty("baz", "meep").build(),
          entityWithProperty(entityWithProperty(entity, "foo", "bar"), "baz", "meep"));
    }

    public MappingSample varyInternalProperties(ObjBase.Builder<?, ?> builder) {
      return new MappingSample(
          builder
              .from(obj)
              .putInternalProperty("foo", "bar")
              .putInternalProperty("baz", "meep")
              .build(),
          entityWithInternalProperty(
              entityWithInternalProperty(entity, "foo", "bar"), "baz", "meep"));
    }

    public MappingSample varyEntityVersion(ObjBase.Builder<?, ?> builder) {
      return new MappingSample(
          builder.from(obj).entityVersion(6666).build(),
          new PolarisBaseEntity.Builder(entity).entityVersion(6666).build());
    }

    public MappingSample varyParentId(ObjBase.Builder<?, ?> builder) {
      return new MappingSample(
          builder.from(obj).parentStableId(6666).build(),
          new PolarisBaseEntity.Builder(entity).parentId(6666).build());
    }
  }

  public abstract static class BaseTestParameter {
    public final PolarisEntityType entityType;
    public final PolarisEntitySubType subType;
    public final ObjType objType;

    public BaseTestParameter(
        PolarisEntityType entityType, PolarisEntitySubType subType, ObjType objType) {
      this.entityType = entityType;
      this.subType = subType;
      this.objType = objType;
    }

    @Override
    public String toString() {
      return subType.name() + " / " + objType.name() + " / " + objType.id();
    }

    public abstract ObjBase.Builder<?, ?> objBuilder();

    public PolarisBaseEntity.Builder entityBuilder() {
      return new PolarisBaseEntity.Builder()
          .typeCode(entityType.getCode())
          .subTypeCode(subType.getCode());
    }

    /** Collect type-specific test case variations. */
    public Stream<MappingSample> typeVariations(MappingSample base) {
      return Stream.of(base);
    }

    /** Expands variations with varying storage information. */
    public Stream<MappingSample> storageVariations(
        Supplier<CatalogStorageObj.Builder<?, ?>> builderSupplier, MappingSample base) {
      var storageConfigurationInfo =
          AwsStorageConfigurationInfo.builder()
              .addAllowedLocation("s3://bucket/allowed")
              .endpoint("https://s3.amazonaws.com")
              .endpointInternal("https://s3.internal.amazonaws.com")
              .region("us-east-1")
              .roleARN("arn:aws:iam::123456789012:role/PolarisS3Access")
              .stsEndpoint("https://sts.amazonaws.com")
              .stsUnavailable(true)
              .pathStyleAccess(true)
              .userARN("arn:aws:iam::123456789012:user/PolarisS3Access")
              .externalId("external-id")
              .build();
      return Stream.of(
          base,
          //
          new MappingSample(
              builderSupplier
                  .get()
                  .from(base.obj())
                  .storageIntegrationIdentifier("storageIntegId")
                  .build(),
              entityWithInternalProperty(
                  base.entity(), getStorageIntegrationIdentifierPropertyName(), "storageIntegId")),
          new MappingSample(
              builderSupplier
                  .get()
                  .from(base.obj())
                  .storageConfigurationInfo(storageConfigurationInfo)
                  .build(),
              entityWithInternalProperty(
                  base.entity(),
                  getStorageConfigInfoPropertyName(),
                  storageConfigurationInfo.serialize())),
          //
          new MappingSample(
              builderSupplier
                  .get()
                  .from(base.obj())
                  .storageConfigurationInfo(storageConfigurationInfo)
                  .storageIntegrationIdentifier("storageIntegId")
                  .build(),
              entityWithInternalProperties(
                  base.entity(),
                  Map.of(
                      getStorageConfigInfoPropertyName(),
                      storageConfigurationInfo.serialize(),
                      getStorageIntegrationIdentifierPropertyName(),
                      "storageIntegId")))
          //
          );
    }
  }

  public static PolarisBaseEntity.Builder buildWithDefaults(
      PolarisBaseEntity.Builder entityBuilder) {
    var props = entityBuilder.build().getPropertiesAsMap();
    var intProps = entityBuilder.build().getInternalPropertiesAsMap();
    entityBuilder
        .id(0L)
        .name("root")
        .createTimestamp(Instant.ofEpochSecond(1234567890L).toEpochMilli())
        .lastUpdateTimestamp(Instant.ofEpochSecond(1299999999L).toEpochMilli())
        .propertiesAsMap(props)
        .internalPropertiesAsMap(intProps);

    return entityBuilder;
  }

  public static <B extends ObjBase.Builder<?, ?>> B buildWithDefaults(B builder) {
    if (builder instanceof BaseCommitObj.Builder<?, ?> commitBuilder) {
      commitBuilder.seq(42).tail(new long[0]);
    }
    builder
        .id(42L)
        .stableId(0L)
        .name("root")
        .createTimestamp(Instant.ofEpochSecond(1234567890L))
        .updateTimestamp(Instant.ofEpochSecond(1299999999L));
    return builder;
  }
}
