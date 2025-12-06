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
import static java.util.function.Function.identity;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjTypes;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj;

public final class EntityObjMappings {

  private static final List<BaseMapping<?, ?>> MAPPINGS =
      List.of(
          new RootMapping(),
          //
          new CatalogMapping(),
          new PrincipalMapping(),
          new PrincipalRoleMapping(),
          new TaskMapping(),
          new FileMapping(),
          //
          new CatalogRoleMapping(),
          new NamespaceMapping<>(),
          new TableLikeMapping<>(),
          new PolicyMapping());

  private static final Map<PolarisEntityType, BaseMapping<?, ?>> BY_ENTITY_TYPE =
      MAPPINGS.stream()
          .collect(
              Collectors.toMap(
                  BaseMapping::entityType,
                  identity(),
                  (a, b) -> {
                    throw new IllegalStateException(String.format("Duplicate key %s", a));
                  },
                  () -> new EnumMap<>(PolarisEntityType.class)));

  private static final BaseMapping<?, ?>[] BY_ENTITY_TYPE_CODE;

  /**
   * Contains mappings from all obj-type {@link Class}es to the {@link BaseMapping} referencing it
   * either via {@link BaseMapping#objType} or {@link BaseMapping#subTypes}.
   */
  private static final Map<Class<? extends Obj>, BaseMapping<?, ?>> BY_OBJ_TYPE_TARGET_CLASS;

  static {
    var missingEntityTypes = new HashSet<>(Set.of(PolarisEntityType.values()));
    missingEntityTypes.remove(PolarisEntityType.NULL_TYPE);
    missingEntityTypes.removeAll(BY_ENTITY_TYPE.keySet());
    checkState(
        missingEntityTypes.isEmpty(),
        "Missing TypeMapping for some PolarisEntityType %s",
        missingEntityTypes);

    var byEntityTypeCode =
        new BaseMapping<?, ?>
            [Arrays.stream(PolarisEntityType.values())
                    .mapToInt(PolarisEntityType::getCode)
                    .max()
                    .orElseThrow()
                + 1];

    var byObjTypeTargetClass = new HashMap<Class<? extends Obj>, BaseMapping<?, ?>>();
    for (var mapping : MAPPINGS) {
      if (mapping.objType != null) {
        byObjTypeTargetClass.put(mapping.objType.targetClass(), mapping);
      } else {
        for (var subType : mapping.subTypes.values()) {
          byObjTypeTargetClass.put(subType.targetClass(), mapping);
        }
      }
      var existing = byEntityTypeCode[mapping.entityType().getCode()];
      checkState(existing == null, "Duplicate entity type codes for %s and %s", mapping, existing);
      byEntityTypeCode[mapping.entityType().getCode()] = mapping;
    }
    BY_OBJ_TYPE_TARGET_CLASS = byObjTypeTargetClass;
    BY_ENTITY_TYPE_CODE = byEntityTypeCode;
  }

  private EntityObjMappings() {}

  /**
   * Produces a builder for the {@link ObjBase} from the given {@link PolarisBaseEntity} with the
   * type-specific attributes populated.
   */
  @Nonnull
  public static <O extends ObjBase, B extends ObjBase.Builder<O, B>> B mapToObj(
      @Nonnull PolarisBaseEntity entity, Optional<PolarisPrincipalSecrets> principalSecrets) {
    @SuppressWarnings("unchecked")
    var mapping = (BaseMapping<O, B>) byEntityType(entity.getType());
    return mapping.mapToObj(entity, principalSecrets);
  }

  /** Maps an {@link ObjBase} to a {@link PolarisBaseEntity} for the given catalog ID. */
  @Nonnull
  public static PolarisBaseEntity mapToEntity(@Nonnull ObjBase objBase, long catalogId) {
    var entityTypeAndSubType = entityTypeAndSubType(objBase.type());
    return entityTypeAndSubType.typeMapping().mapToEntity(objBase, catalogId);
  }

  /** Maps an {@link ObjBase} to an {@link EntityNameLookupRecord} for the given catalog ID. */
  @Nonnull
  public static EntityNameLookupRecord mapToEntityNameLookupRecord(
      @Nonnull ObjBase objBase, long catalogId) {

    var entityTypeAndSubType = entityTypeAndSubType(objBase.type());
    entityTypeAndSubType.typeMapping().checkCatalogId(catalogId);

    return new EntityNameLookupRecord(
        catalogId,
        objBase.stableId(),
        objBase.parentStableId(),
        objBase.name(),
        entityTypeAndSubType.entityType().getCode(),
        entityTypeAndSubType.subType().getCode());
  }

  // TODO move to MutationAttempt in the follow-up change
  @Nullable
  public static String entitySubTypeCodeFromObjType(ObjRef objRef) {
    if (objRef != null) {
      var objType = ObjTypes.objTypeById(objRef.type());
      var entityTypeAndSubType = entityTypeAndSubType(objType);
      return Integer.toString(entityTypeAndSubType.subType().getCode());
    }
    return null;
  }

  /**
   * Extract the PolarisPrincipalSecrets from the given object, if it is a {@link PrincipalObj} and
   * has secrets.
   */
  // TODO move to MutationAttempt in the follow-up change
  public static @Nonnull Optional<PolarisPrincipalSecrets> maybeObjToPolarisPrincipalSecrets(
      @Nonnull ObjBase obj) {
    if (obj instanceof PrincipalObj principalObj && principalObj.clientId().isPresent()) {
      return Optional.of(principalObjToPolarisPrincipalSecrets(principalObj));
    }
    return Optional.empty();
  }

  /**
   * Produces a {@link PolarisPrincipalSecrets} from the given {@link PrincipalObj} assuming a
   * {@code null} {@link PolarisPrincipalSecrets}.
   */
  public static @Nonnull PolarisPrincipalSecrets principalObjToPolarisPrincipalSecrets(
      @Nonnull PrincipalObj principalObj) {
    return principalObjToPolarisPrincipalSecrets(principalObj, null);
  }

  /**
   * Produces a {@link PolarisPrincipalSecrets} from the given {@link PrincipalObj} and the given
   * {@link PolarisPrincipalSecrets} if not {@code null}.
   */
  public static @Nonnull PolarisPrincipalSecrets principalObjToPolarisPrincipalSecrets(
      @Nonnull PrincipalObj principalObj, @Nullable PolarisPrincipalSecrets newPrincipalSecrets) {
    return new PolarisPrincipalSecrets(
        principalObj.stableId(),
        principalObj.clientId().orElse(null),
        newPrincipalSecrets != null ? newPrincipalSecrets.getMainSecret() : null,
        newPrincipalSecrets != null ? newPrincipalSecrets.getSecondarySecret() : null,
        principalObj.secretSalt().orElse(null),
        principalObj.mainSecretHash().orElse(null),
        principalObj.secondarySecretHash().orElse(null));
  }

  /** Retrieve the {@link BaseMapping} for the given {@link PolarisEntityType}. */
  public static @Nonnull BaseMapping<?, ?> byEntityType(@Nonnull PolarisEntityType entityType) {
    var mapping = BY_ENTITY_TYPE.get(entityType);
    checkArgument(mapping != null, "No type mapping for entity type %s", entityType);
    return mapping;
  }

  /** Retrieve the {@link BaseMapping} for a {@link PolarisEntityType} by their {@code int} code. */
  public static @Nonnull BaseMapping<?, ?> byEntityTypeCode(int entityType) {
    checkArgument(
        entityType > 0 && entityType < BY_ENTITY_TYPE_CODE.length,
        "No type mapping for entity type code %s",
        entityType);
    var mapping = BY_ENTITY_TYPE_CODE[entityType];
    checkArgument(mapping != null, "No type mapping for entity type code %s", entityType);
    return mapping;
  }

  public record EntityTypeAndSubType(
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType subType,
      @Nonnull BaseMapping<?, ?> typeMapping) {}

  /**
   * Retrieve the {@link PolarisEntityType}, {@link PolarisEntitySubType} and the corresponding
   * {@link BaseMapping} for the given {@link ObjType}.
   */
  public static @Nonnull EntityTypeAndSubType entityTypeAndSubType(@Nonnull ObjType objType) {
    var mapping = BY_OBJ_TYPE_TARGET_CLASS.get(objType.targetClass());
    checkArgument(
        mapping != null,
        "No TypeMapping for %s (%s)",
        objType.name(),
        objType.targetClass().getSimpleName());
    var subType = mapping.subTypeByObjType.get(objType);
    checkArgument(
        subType != null,
        "No subType mapping in existing TypeMapping for %s (%s)",
        objType.name(),
        objType.targetClass().getSimpleName());
    return new EntityTypeAndSubType(mapping.entityType, subType, mapping);
  }

  /** Checks whether the entity type defines catalog-content, aka tables, views and policies. */
  public static boolean isCatalogContent(int entityTypeCode) {
    return byEntityTypeCode(entityTypeCode).catalogContent();
  }

  /** Checks whether the entity type defines catalog-content, aka tables, views and policies. */
  public static boolean isCatalogContent(@Nonnull PolarisEntityType entityType) {
    return byEntityType(entityType).catalogContent();
  }

  /**
   * Returns the object type for a Polaris entity type and subtype that can be used for
   * querying/filtering, allowing {@link PolarisEntitySubType#ANY_SUBTYPE} as the subtype.
   */
  public static @Nonnull Class<? extends ObjBase> objTypeForPolarisTypeForFiltering(
      @Nonnull PolarisEntityType entityType, @Nonnull PolarisEntitySubType subType) {
    return byEntityType(entityType).objTypeClassForSubTypeForFiltering(subType);
  }

  /**
   * Returns the "exact" object type for a Polaris entity type and subtype, <em>not</em> allowing
   * {@link PolarisEntitySubType#ANY_SUBTYPE} as the subtype.
   */
  public static @Nonnull ObjType objTypeForPolarisType(
      @Nonnull PolarisEntityType entityType, @Nonnull PolarisEntitySubType subType) {
    return byEntityType(entityType).objTypeForSubType(subType);
  }

  /**
   * Retrieve the objet type that {@linkplain ContainerObj contains} the given entity-type.
   *
   * <p>For example, all catalog content is contained in a {@link CatalogStateObj}, all principals
   * in a {@link PrincipalsObj}.
   */
  public static @Nonnull Class<? extends ContainerObj> containerTypeForEntityType(
      @Nonnull PolarisEntityType entityType) {
    var containerObjType = byEntityType(entityType).containerObjType;
    checkArgument(containerObjType != null, "Not a container managed ObjType for %s", entityType);
    @SuppressWarnings("unchecked")
    var clazz = (Class<? extends ContainerObj>) containerObjType.targetClass();
    return clazz;
  }

  /** Return the {@link PolarisEntityType} for the given entity type code, never {@code null}. */
  public static @Nonnull PolarisEntityType typeFromCode(int entityTypeCode) {
    return byEntityTypeCode(entityTypeCode).entityType;
  }

  /**
   * Yields the given object, if it matches the given entity type, otherwise returns an empty
   * optional.
   */
  public static <C extends ObjBase> @Nonnull Optional<C> filterIsEntityType(
      @Nonnull C objBase, int entityTypeCode) {
    return filterIsEntityType(objBase, typeFromCode(entityTypeCode));
  }

  /**
   * Yields the given object, if it matches the given entity type, otherwise returns an empty
   * optional.
   */
  public static <C extends ObjBase> @Nonnull Optional<C> filterIsEntityType(
      @Nonnull C objBase, @Nonnull PolarisEntityType entityType) {
    return objTypeForPolarisTypeForFiltering(entityType, PolarisEntitySubType.ANY_SUBTYPE)
            .isInstance(objBase)
        ? Optional.of(objBase)
        : Optional.empty();
  }

  public static @Nonnull String referenceName(
      @Nonnull PolarisEntityType entityType, @Nonnull Optional<CatalogObj> catalog) {
    var catalogStableId = catalog.map(ObjBase::stableId).orElse(0L);
    return referenceName(entityType, catalogStableId);
  }

  public static @Nonnull String referenceName(
      @Nonnull PolarisEntityType entityType, @Nonnull OptionalLong catalogId) {
    return referenceName(entityType, catalogId.orElse(0L));
  }

  public static @Nonnull String referenceName(
      @Nonnull PolarisEntityType entityType, long catalogId) {
    return BY_ENTITY_TYPE.get(entityType).refNameForCatalog(catalogId);
  }

  /** Checks that the given catalog ID is valid, aka positive. */
  public static void checkCatalogId(long catalogId) {
    checkArgument(catalogId > 0L, "Invalid catalog ID %s", catalogId);
  }
}
