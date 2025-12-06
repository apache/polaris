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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.apache.polaris.core.entity.PolarisEntitySubType.ANY_SUBTYPE;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStorageObj;

public abstract class BaseMapping<O extends ObjBase, B extends ObjBase.Builder<O, B>> {

  private final Class<? extends ObjBase> baseObjTypeClass;
  final ObjType objType;
  final Map<PolarisEntitySubType, ObjType> subTypes;
  final Map<ObjType, PolarisEntitySubType> subTypeByObjType;
  final ObjType containerObjType;
  final String refName;
  final PolarisEntityType entityType;

  @SuppressWarnings("unchecked")
  BaseMapping(
      @Nonnull ObjType objType,
      @Nullable ObjType containerObjType,
      @Nullable String refName,
      @Nonnull PolarisEntityType entityType) {
    this.baseObjTypeClass = (Class<? extends ObjBase>) objType.targetClass();
    this.objType = objType;
    this.subTypes = Map.of(PolarisEntitySubType.NULL_SUBTYPE, objType);
    this.subTypeByObjType = Map.of(objType, PolarisEntitySubType.NULL_SUBTYPE);
    this.containerObjType = containerObjType;
    this.refName = refName;
    this.entityType = entityType;
  }

  BaseMapping(
      @Nonnull Class<? extends ObjBase> baseObjTypeClass,
      @Nonnull Map<PolarisEntitySubType, ObjType> subTypes,
      @Nonnull ObjType containerObjType,
      @Nonnull String refName,
      @Nonnull PolarisEntityType entityType) {
    this.baseObjTypeClass = baseObjTypeClass;
    this.objType = null;
    this.subTypes = subTypes;
    this.subTypeByObjType =
        subTypes.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getValue,
                    Map.Entry::getKey,
                    (a, b) -> {
                      throw new IllegalStateException(format("Duplicate key %s", a));
                    }));
    this.containerObjType = containerObjType;
    this.refName = refName;
    this.entityType = entityType;
  }

  /** Check whether the given subtype is valid. */
  public void validateSubType(@Nonnull PolarisEntitySubType subType) {
    objTypeForSubType(subType);
  }

  /** Retrieve the {@link ObjType} for the given {@link PolarisEntitySubType}. */
  public @Nonnull ObjType objTypeForSubType(@Nonnull PolarisEntitySubType subType) {
    checkArgument(
        subType != ANY_SUBTYPE,
        "Unresolvable subtype ANY_SUBTYPE for exact match for %s",
        entityType);
    var objType = subTypes.get(subType);
    checkArgument(objType != null, "Invalid subType %s for %s", subType, entityType);
    return objType;
  }

  /**
   * Retrieve the {@link Class} used to filter objects for the given {@link PolarisEntitySubType}.
   */
  public @Nonnull Class<? extends ObjBase> objTypeClassForSubTypeForFiltering(
      @Nonnull PolarisEntitySubType subType) {
    if (subType == ANY_SUBTYPE) {
      if (objType != null) {
        return cast(objType.targetClass());
      }
      checkState(baseObjTypeClass != null, "No baseObjTypeClass for %s", entityType);
      return baseObjTypeClass;
    }

    var objType = subTypes.get(subType);
    checkState(objType != null, "Invalid subType %s for %s", subType, entityType);
    return cast(objType.targetClass());
  }

  /** Checks whether the given catalog ID is valid for this mapping. */
  public void checkCatalogId(long catalogId) {
    checkArgument(
        catalogId == 0L, "Catalog ID must be 0L, but is %s for %s", catalogId, entityType);
  }

  /**
   * Returns the persistence reference name to manage entities for this mapping for the given
   * catalog ID.
   */
  public @Nonnull String refNameForCatalog(long catalogId) {
    checkArgument(
        catalogId == 0L, "Catalog ID must be 0L, but is %s for %s", catalogId, entityType);
    return refName;
  }

  public @Nonnull PolarisEntityType entityType() {
    return entityType;
  }

  /**
   * Returns whether this mapping refers to catalog content, aka tables, views and policies, called
   * from {@link #mapToObj(PolarisBaseEntity, Optional)}
   */
  public boolean catalogContent() {
    return false;
  }

  public @Nullable ObjType containerObjType() {
    return containerObjType;
  }

  @SuppressWarnings("unchecked")
  public @Nonnull <C extends ContainerObj> Class<C> containerObjTypeClass() {
    return (Class<C>) containerObjType.targetClass();
  }

  /** Type specific mapping from a {@link PolarisBaseEntity} to an {@link ObjBase.Builder}. */
  public abstract B newObjBuilder(@Nonnull PolarisEntitySubType subType);

  /**
   * Type specific mapping from a {@link PolarisBaseEntity} to an {@link ObjBase.Builder}.
   *
   * <p>Implementations <em>remove</em> properties from {@code properties} and {@code
   * internalProperties} maps that have been set on the type-safe attributes of the {@link
   * ObjBase.Builder}.
   */
  void mapToObjTypeSpecific(
      B baseBuilder,
      @Nonnull PolarisBaseEntity entity,
      Optional<PolarisPrincipalSecrets> principalSecrets,
      Map<String, String> properties,
      Map<String, String> internalProperties) {}

  static void mapToObjStorageSpecific(
      CatalogStorageObj.Builder<?, ?> catalogObjBaseBuilder,
      HashMap<String, String> internalProperties) {
    catalogObjBaseBuilder.storageConfigurationInfo(
        Optional.ofNullable(
                internalProperties.remove(
                    PolarisEntityConstants.getStorageConfigInfoPropertyName()))
            .map(PolarisStorageConfigurationInfo::deserialize));
    catalogObjBaseBuilder.storageIntegrationIdentifier(
        Optional.ofNullable(
            internalProperties.remove(
                PolarisEntityConstants.getStorageIntegrationIdentifierPropertyName())));
  }

  /**
   * Produces a builder for the {@link ObjBase} from the given {@link PolarisBaseEntity} with the
   * type-specific attributes populated.
   */
  public final B mapToObj(
      @Nonnull PolarisBaseEntity entity, Optional<PolarisPrincipalSecrets> principalSecrets) {
    var properties = new HashMap<>(entity.getPropertiesAsMap());
    var internalProperties = new HashMap<>(entity.getInternalPropertiesAsMap());
    validateSubType(entity.getSubType());

    B baseBuilder = newObjBuilder(entity.getSubType());
    mapToObjTypeSpecific(baseBuilder, entity, principalSecrets, properties, internalProperties);

    if (baseBuilder instanceof CatalogStorageObj.Builder<?, ?> catalogObjBaseBuilder) {
      mapToObjStorageSpecific(catalogObjBaseBuilder, internalProperties);
    }

    baseBuilder
        .name(entity.getName())
        .entityVersion(entity.getEntityVersion())
        .stableId(entity.getId())
        .parentStableId(entity.getParentId())
        .createTimestamp(Instant.ofEpochMilli(entity.getCreateTimestamp()))
        .updateTimestamp(Instant.ofEpochMilli(entity.getLastUpdateTimestamp()));

    properties.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .forEach(baseBuilder::putProperty);
    internalProperties.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .forEach(baseBuilder::putInternalProperty);

    return baseBuilder;
  }

  /**
   * Type specific mapping from an {@link ObjBase} to a {@link PolarisBaseEntity}, called from
   * {@link #mapToEntity(ObjBase, long)}.
   */
  void mapToEntityTypeSpecific(
      O o,
      HashMap<String, String> properties,
      HashMap<String, String> internalProperties,
      PolarisEntitySubType subType) {}

  static void mapToEntityStorageSpecific(
      CatalogStorageObj catalogStorageObj, HashMap<String, String> internalProperties) {
    catalogStorageObj
        .storageConfigurationInfo()
        .ifPresent(
            polarisStorageConfigInfo ->
                internalProperties.put(
                    PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                    polarisStorageConfigInfo.serialize()));
    catalogStorageObj
        .storageIntegrationIdentifier()
        .ifPresent(
            ident ->
                internalProperties.put(
                    PolarisEntityConstants.getStorageIntegrationIdentifierPropertyName(), ident));
  }

  /** Maps an {@link ObjBase} to a {@link PolarisBaseEntity} for the given catalog ID. */
  public final PolarisBaseEntity mapToEntity(ObjBase o, long catalogId) {
    var properties = new HashMap<>(o.properties());
    var internalProperties = new HashMap<>(o.internalProperties());
    checkCatalogId(catalogId);
    var subType = subTypeByObjType.get(o.type());
    checkState(subType != null, "Invalid no subtype for objType %s for %s", o.type(), entityType);

    mapToEntityTypeSpecific(cast(o), properties, internalProperties, subType);

    if (o instanceof CatalogStorageObj catalogStorageObj) {
      mapToEntityStorageSpecific(catalogStorageObj, internalProperties);
    }

    return new PolarisBaseEntity.Builder()
        .catalogId(catalogId)
        .id(o.stableId())
        .typeCode(entityType.getCode())
        .subTypeCode(subType.getCode())
        .parentId(o.parentStableId())
        .name(o.name())
        .propertiesAsMap(properties)
        .internalPropertiesAsMap(internalProperties)
        .createTimestamp(o.createTimestamp().toEpochMilli())
        .lastUpdateTimestamp(o.updateTimestamp().toEpochMilli())
        .entityVersion(o.entityVersion())
        .build();
  }

  @SuppressWarnings("unchecked")
  static <R> R cast(Object r) {
    return (R) r;
  }

  @Override
  public String toString() {
    return "BaseMapping{"
        + "entityType="
        + entityType
        + ", subTypes="
        + subTypes.keySet()
        + ", objType="
        + objType
        + '}';
  }

  /**
   * Returns {@code 0L} for non-catalog-related types, otherwise the checked catalog ID.
   *
   * <p>At least some tests use a non-{@code 0} catalog ID for non-catalog entity types.
   *
   * <p>Could do the "fix" in {@link #refNameForCatalog(long)}, but the goal should be to fix the
   * call sites and get rid of this workaround.
   */
  public long fixCatalogId(long catalogId) {
    return 0;
  }
}
