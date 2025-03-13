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
package org.apache.polaris.persistence.bridge;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.core.entity.CatalogEntity.CATALOG_TYPE_PROPERTY;
import static org.apache.polaris.core.entity.CatalogEntity.DEFAULT_BASE_LOCATION_KEY;
import static org.apache.polaris.core.entity.CatalogEntity.REMOTE_URL;
import static org.apache.polaris.core.entity.IcebergTableLikeEntity.METADATA_LOCATION_KEY;
import static org.apache.polaris.core.entity.PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.persistence.api.Persistence;
import org.apache.polaris.persistence.api.obj.ObjRef;
import org.apache.polaris.persistence.api.obj.ObjType;
import org.apache.polaris.persistence.api.obj.ObjTypes;
import org.apache.polaris.persistence.coretypes.ContainerObj;
import org.apache.polaris.persistence.coretypes.ObjBase;
import org.apache.polaris.persistence.coretypes.catalog.CatalogGrantsObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogObjBase;
import org.apache.polaris.persistence.coretypes.catalog.CatalogRoleObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogRolesObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogStatus;
import org.apache.polaris.persistence.coretypes.catalog.CatalogType;
import org.apache.polaris.persistence.coretypes.catalog.CatalogsObj;
import org.apache.polaris.persistence.coretypes.catalog.FileObj;
import org.apache.polaris.persistence.coretypes.content.IcebergTableLikeObj;
import org.apache.polaris.persistence.coretypes.content.IcebergTableObj;
import org.apache.polaris.persistence.coretypes.content.IcebergViewObj;
import org.apache.polaris.persistence.coretypes.content.LocalNamespaceObj;
import org.apache.polaris.persistence.coretypes.content.NamespaceObj;
import org.apache.polaris.persistence.coretypes.content.RemoteNamespaceObj;
import org.apache.polaris.persistence.coretypes.content.TableLikeObj;
import org.apache.polaris.persistence.coretypes.principals.PrincipalObj;
import org.apache.polaris.persistence.coretypes.principals.PrincipalRoleObj;
import org.apache.polaris.persistence.coretypes.principals.PrincipalRolesObj;
import org.apache.polaris.persistence.coretypes.principals.PrincipalsObj;
import org.apache.polaris.persistence.coretypes.realm.ImmediateTaskObj;
import org.apache.polaris.persistence.coretypes.realm.ImmediateTasksObj;
import org.apache.polaris.persistence.coretypes.realm.RealmGrantsObj;
import org.apache.polaris.persistence.coretypes.realm.RootObj;

class TypeMapping {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .findAndRegisterModules()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  static byte[] serializeStringCompressed(String entityAsJson) {
    try (var baos = new ByteArrayOutputStream()) {
      try (var gzip = new GZIPOutputStream(baos);
          var out = new DataOutputStream(gzip)) {
        out.writeUTF(entityAsJson);
      }
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static String deserializeStringGompressed(byte[] bytes) {
    try (var in = new DataInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)))) {
      return in.readUTF();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static String serializeMapAsJson(Map<String, String> map) {
    try {
      return MAPPER.writeValueAsString(map);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static Map<String, String> deserializeMapFromJson(String json) {
    try {
      return MAPPER.readValue(json, new TypeReference<>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  static <O extends ObjBase, B extends ObjBase.Builder<O, B>> B mapToObj(
      @Nonnull PolarisBaseEntity entity, Optional<PolarisPrincipalSecrets> principalSecrets) {
    var type = entity.getType();
    var subType = entity.getSubType();
    var properties = new HashMap<>(entity.getPropertiesAsMap());
    var internalProperties = new HashMap<>(entity.getInternalPropertiesAsMap());
    var baseBuilder = (B) null;
    baseBuilder =
        cast(
            switch (type) {
              case CATALOG -> {
                checkArgument(
                    subType == PolarisEntitySubType.NULL_SUBTYPE, "invalid subtype for %s", type);
                var defaultBaseLocation = properties.remove(DEFAULT_BASE_LOCATION_KEY);
                var remoteUrl = properties.remove(REMOTE_URL);
                var catalogTypeString = internalProperties.remove(CATALOG_TYPE_PROPERTY);
                if (catalogTypeString == null) {
                  catalogTypeString = CatalogType.INTERNAL.name();
                }
                var catalogType =
                    switch (catalogTypeString.toUpperCase(Locale.ROOT)) {
                      case "INTERNAL" -> CatalogType.INTERNAL;
                      case "EXTERNAL" -> CatalogType.EXTERNAL;
                      default ->
                          throw new IllegalArgumentException(
                              "Invalid catalog type " + catalogTypeString);
                    };
                yield CatalogObj.builder()
                    .catalogType(catalogType)
                    .defaultBaseLocation(defaultBaseLocation)
                    .remoteUrl(Optional.ofNullable(remoteUrl))
                    .status(CatalogStatus.ACTIVE);
              }
              case CATALOG_ROLE -> {
                checkArgument(
                    subType == PolarisEntitySubType.NULL_SUBTYPE, "invalid subtype for %s", type);
                yield CatalogRoleObj.builder();
              }
              case FILE -> {
                checkArgument(
                    subType == PolarisEntitySubType.NULL_SUBTYPE, "invalid subtype for %s", type);
                yield FileObj.builder();
              }
              case NAMESPACE -> {
                checkArgument(
                    subType == PolarisEntitySubType.NULL_SUBTYPE, "invalid subtype for %s", type);
                // TODO RemoteNamespaceObj ?
                yield LocalNamespaceObj.builder();
              }
              case ROOT -> {
                checkArgument(
                    subType == PolarisEntitySubType.NULL_SUBTYPE, "invalid subtype for %s", type);
                yield RootObj.builder();
              }
              case PRINCIPAL -> {
                checkArgument(
                    subType == PolarisEntitySubType.NULL_SUBTYPE, "invalid subtype for %s", type);
                var secrets =
                    principalSecrets.orElseThrow(
                        () -> new IllegalArgumentException("Missing principal secrets"));
                var credentialRotationRequired =
                    internalProperties.remove(PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE);
                internalProperties.remove(PolarisEntityConstants.getClientIdPropertyName());
                yield PrincipalObj.builder()
                    .clientId(secrets.getPrincipalClientId())
                    .credentialRotationRequired(credentialRotationRequired != null)
                    .mainSecretHash(secrets.getMainSecretHash())
                    .secondarySecretHash(secrets.getSecondarySecretHash())
                    .secretSalt(secrets.getSecretSalt());
              }
              case PRINCIPAL_ROLE -> {
                checkArgument(
                    subType == PolarisEntitySubType.NULL_SUBTYPE, "invalid subtype for %s", type);
                yield PrincipalRoleObj.builder();
              }
              case TASK -> {
                checkArgument(
                    subType == PolarisEntitySubType.NULL_SUBTYPE, "invalid subtype for %s", type);
                var b = ImmediateTaskObj.builder();
                var taskTypeCode = properties.remove(PolarisTaskConstants.TASK_TYPE);
                var lastAttemptExecutorId =
                    properties.remove(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID);
                if (lastAttemptExecutorId != null) {
                  b.lastAttemptExecutorId(lastAttemptExecutorId);
                }
                var lastAttemptStartTime =
                    properties.remove(PolarisTaskConstants.LAST_ATTEMPT_START_TIME);
                if (lastAttemptStartTime != null) {
                  b.lastAttemptStartTime(
                      Instant.ofEpochMilli(Long.parseLong(lastAttemptStartTime)));
                }
                var attemptCount = properties.remove(PolarisTaskConstants.ATTEMPT_COUNT);
                if (attemptCount != null) {
                  b.attemptCount(Integer.parseInt(attemptCount));
                }
                if (taskTypeCode != null) {
                  b.taskType(AsyncTaskType.fromTypeCode(Integer.parseInt(taskTypeCode)));
                }
                yield b.serializedEntity(
                    Optional.ofNullable(properties.remove("data"))
                        .map(TypeMapping::serializeStringCompressed));
              }
              case ICEBERG_TABLE_LIKE -> {
                var tlBuilder = (IcebergTableLikeObj.Builder<?, ?>) null;
                tlBuilder =
                    switch (subType) {
                      case TABLE -> IcebergTableObj.builder();
                      case VIEW -> IcebergViewObj.builder();
                      default ->
                          throw new IllegalArgumentException("Unknown or invalid subtype " + type);
                    };

                Optional.ofNullable(internalProperties.remove(METADATA_LOCATION_KEY))
                    .ifPresent(tlBuilder::metadataLocation);

                yield tlBuilder;
              }
              default -> throw new IllegalArgumentException("Unknown type " + type);
            });

    if (baseBuilder instanceof CatalogObjBase.Builder<?, ?> catalogObjBaseBuilder) {
      catalogObjBaseBuilder.storageConfigurationInfo(
          Optional.ofNullable(
                  internalProperties.remove(
                      PolarisEntityConstants.getStorageConfigInfoPropertyName()))
              .map(
                  json ->
                      PolarisStorageConfigurationInfo.deserialize(
                          new PolarisDefaultDiagServiceImpl(), json)));
      catalogObjBaseBuilder.storageIntegrationIdentifier(
          Optional.ofNullable(
              internalProperties.remove(
                  PolarisEntityConstants.getStorageIntegrationIdentifierPropertyName())));
    }

    baseBuilder
        .name(entity.getName())
        .entityVersion(entity.getEntityVersion())
        .stableId(entity.getId())
        .parentStableId(entity.getParentId())
        .createTimestamp(Instant.ofEpochMilli(entity.getCreateTimestamp()))
        .updateTimestamp(Instant.ofEpochMilli(entity.getLastUpdateTimestamp()));

    for (var e : properties.entrySet()) {
      if (e.getValue() != null) {
        baseBuilder.putProperty(e.getKey(), e.getValue());
      }
    }
    for (var e : internalProperties.entrySet()) {
      if (e.getValue() != null) {
        baseBuilder.putInternalProperty(e.getKey(), e.getValue());
      }
    }

    return baseBuilder;
  }

  static boolean isCatalogContent(int entityTypeCode) {
    return entityTypeCode == PolarisEntityType.NAMESPACE.getCode()
        || entityTypeCode == PolarisEntityType.ICEBERG_TABLE_LIKE.getCode();
  }

  static boolean isCatalogContent(PolarisEntityType entityType) {
    return switch (entityType) {
      case NAMESPACE, ICEBERG_TABLE_LIKE -> true;
      default -> false;
    };
  }

  static Class<? extends ObjBase> objTypeForPolarisTypeForFiltering(
      PolarisEntityType entityType, PolarisEntitySubType subType) {
    return switch (entityType) {
      case PRINCIPAL -> PrincipalObj.class;
      case TASK -> ImmediateTaskObj.class;
      case ICEBERG_TABLE_LIKE ->
          switch (subType) {
            case TABLE -> IcebergTableObj.class;
            case VIEW -> IcebergViewObj.class;
            case ANY_SUBTYPE -> IcebergTableLikeObj.class;
            default -> throw new IllegalArgumentException("Illegal subtype " + subType);
          };
      case NAMESPACE -> NamespaceObj.class;
      case CATALOG -> CatalogObj.class;
      case CATALOG_ROLE -> CatalogRoleObj.class;
      case PRINCIPAL_ROLE -> PrincipalRoleObj.class;
      case ROOT -> RootObj.class;
      case FILE -> FileObj.class;
      default -> throw new IllegalArgumentException("Illegal entity type " + entityType);
    };
  }

  static ObjType objTypeForPolarisType(PolarisEntityType entityType, PolarisEntitySubType subType) {
    return switch (entityType) {
      case PRINCIPAL -> PrincipalObj.TYPE;
      case TASK -> ImmediateTaskObj.TYPE;
      case ICEBERG_TABLE_LIKE ->
          switch (subType) {
            case TABLE -> IcebergTableObj.TYPE;
            case VIEW -> IcebergViewObj.TYPE;
            default -> throw new IllegalArgumentException("Illegal subtype " + subType);
          };
      case NAMESPACE -> LocalNamespaceObj.TYPE;
      case CATALOG -> CatalogObj.TYPE;
      case CATALOG_ROLE -> CatalogRoleObj.TYPE;
      case PRINCIPAL_ROLE -> PrincipalRoleObj.TYPE;
      case ROOT -> RootObj.TYPE;
      case FILE -> FileObj.TYPE;
      default -> throw new IllegalArgumentException("Illegal entity type " + entityType);
    };
  }

  static Class<? extends ContainerObj> containerTypeForEntityType(
      int entityTypeCode, boolean forCatalog) {
    return containerTypeForEntityType(typeFromCode(entityTypeCode), forCatalog);
  }

  static Class<? extends ContainerObj> containerTypeForEntityType(
      PolarisEntityType entityType, boolean forCatalog) {
    return switch (entityType) {
      case CATALOG -> forCatalog ? CatalogStateObj.class : CatalogsObj.class;
      case PRINCIPAL -> PrincipalsObj.class;
      case PRINCIPAL_ROLE -> PrincipalRolesObj.class;
      case TASK -> ImmediateTasksObj.class;

      // per catalog
      case CATALOG_ROLE -> CatalogRolesObj.class;
      case NAMESPACE, ICEBERG_TABLE_LIKE -> CatalogStateObj.class;

      default -> throw new IllegalArgumentException("Unsupported entity type: " + entityType);
    };
  }

  static ContainerObj.Builder<? extends ContainerObj, ? extends ContainerObj.Builder<?, ?>>
      newContainerBuilderForEntityType(PolarisEntityType entityType) {
    return switch (entityType) {
      case CATALOG -> CatalogsObj.builder();
      case PRINCIPAL -> PrincipalsObj.builder();
      case PRINCIPAL_ROLE -> PrincipalRolesObj.builder();
      case TASK -> ImmediateTasksObj.builder();

      // per catalog
      case CATALOG_ROLE -> CatalogRolesObj.builder();
      case NAMESPACE, ICEBERG_TABLE_LIKE -> CatalogStateObj.builder();

      default -> throw new IllegalArgumentException("Unsupported entity type: " + entityType);
    };
  }

  static PolarisEntityType typeFromCode(int entityTypeCode) {
    return requireNonNull(PolarisEntityType.fromCode(entityTypeCode), "Invalid type code");
  }

  @Nonnull
  static PolarisBaseEntity mapToEntity(@Nonnull ObjBase objBase, long catalogId) {
    var properties = new HashMap<>(objBase.properties());
    var internalProperties = new HashMap<>(objBase.internalProperties());
    var type = PolarisEntityType.NULL_TYPE;
    var subType = PolarisEntitySubType.NULL_SUBTYPE;
    var parentId = objBase.parentStableId();

    switch (objBase) {
      case TableLikeObj o -> {
        catalogMandatory(catalogId);
        type = PolarisEntityType.ICEBERG_TABLE_LIKE;
        o.metadataLocation().ifPresent(v -> internalProperties.put(METADATA_LOCATION_KEY, v));
        if (objBase instanceof IcebergTableObj) {
          subType = PolarisEntitySubType.TABLE;
        }
        if (objBase instanceof IcebergViewObj) {
          subType = PolarisEntitySubType.VIEW;
        }
      }
      case LocalNamespaceObj ignored -> {
        catalogMandatory(catalogId);
        type = PolarisEntityType.NAMESPACE;
      }
      case RemoteNamespaceObj ignored -> {
        catalogMandatory(catalogId);
        // TODO RemoteNamespaceObj ?
      }
      case CatalogObj o -> {
        realmMandatory(catalogId);
        type = PolarisEntityType.CATALOG;
        internalProperties.put(CATALOG_TYPE_PROPERTY, o.catalogType().name());
        o.remoteUrl().ifPresent(v -> properties.put(REMOTE_URL, v));
        o.defaultBaseLocation().ifPresent(v -> properties.put(DEFAULT_BASE_LOCATION_KEY, v));
      }
      case CatalogRoleObj ignored -> {
        catalogMandatory(catalogId);
        type = PolarisEntityType.CATALOG_ROLE;
      }
      case RootObj ignored -> {
        realmMandatory(catalogId);
        type = PolarisEntityType.ROOT;
      }
      case ImmediateTaskObj o -> {
        realmMandatory(catalogId);
        o.serializedEntity()
            .map(TypeMapping::deserializeStringGompressed)
            .ifPresent(s -> properties.put("data", s));
        properties.put(PolarisTaskConstants.TASK_TYPE, Integer.toString(o.taskType().typeCode()));
        o.lastAttemptExecutorId()
            .ifPresent(v -> properties.put(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID, v));
        o.lastAttemptStartTime()
            .ifPresent(
                v ->
                    properties.put(
                        PolarisTaskConstants.LAST_ATTEMPT_START_TIME,
                        Long.toString(v.toEpochMilli())));
        o.attemptCount()
            .ifPresent(
                v -> properties.put(PolarisTaskConstants.ATTEMPT_COUNT, Integer.toString(v)));
        type = PolarisEntityType.TASK;
      }
      case PrincipalObj o -> {
        realmMandatory(catalogId);
        type = PolarisEntityType.PRINCIPAL;
        if (o.credentialRotationRequired()) {
          internalProperties.put(PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true");
        }
        internalProperties.put(PolarisEntityConstants.getClientIdPropertyName(), o.clientId());
      }
      case PrincipalRoleObj ignored -> type = PolarisEntityType.PRINCIPAL_ROLE;
      default ->
          throw new IllegalStateException(
              "Cannot map " + objBase.type().targetClass().getSimpleName() + " to a PolarisEntity");
    }

    if (objBase instanceof CatalogObjBase catalogObjBase) {
      catalogObjBase
          .storageConfigurationInfo()
          .ifPresent(
              polarisStorageConfigInfo ->
                  internalProperties.put(
                      PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                      polarisStorageConfigInfo.serialize()));
      catalogObjBase
          .storageIntegrationIdentifier()
          .ifPresent(
              ident ->
                  internalProperties.put(
                      PolarisEntityConstants.getStorageIntegrationIdentifierPropertyName(), ident));
    }

    var id = objBase.stableId();
    var name = objBase.name();
    var b = new PolarisBaseEntity(catalogId, id, type, subType, parentId, name);
    b.setPropertiesAsMap(properties);
    b.setInternalPropertiesAsMap(internalProperties);
    b.setCreateTimestamp(objBase.createTimestamp().toEpochMilli());
    b.setLastUpdateTimestamp(objBase.updateTimestamp().toEpochMilli());
    b.setEntityVersion(objBase.entityVersion());
    return b;
  }

  @Nonnull
  static EntityNameLookupRecord mapToEntityNameLookupRecord(
      @Nonnull ObjBase objBase, long catalogStableId) {
    var type = PolarisEntityType.NULL_TYPE;
    var subType = PolarisEntitySubType.NULL_SUBTYPE;

    switch (objBase) {
      case TableLikeObj ignored -> {
        catalogMandatory(catalogStableId);
        type = PolarisEntityType.ICEBERG_TABLE_LIKE;
        if (objBase instanceof IcebergTableObj) {
          subType = PolarisEntitySubType.TABLE;
        }
        if (objBase instanceof IcebergViewObj) {
          subType = PolarisEntitySubType.VIEW;
        }
      }
      case LocalNamespaceObj ignored -> {
        catalogMandatory(catalogStableId);
        type = PolarisEntityType.NAMESPACE;
      }
      case RemoteNamespaceObj ignored -> {
        // TODO RemoteNamespaceObj ?
      }
      case CatalogObj ignored -> {
        realmMandatory(catalogStableId);
        type = PolarisEntityType.CATALOG;
      }
      case CatalogRoleObj ignored -> {
        catalogMandatory(catalogStableId);
        type = PolarisEntityType.CATALOG_ROLE;
      }
      case RootObj ignored -> {
        realmMandatory(catalogStableId);
        type = PolarisEntityType.ROOT;
      }
      case ImmediateTaskObj ignored -> {
        realmMandatory(catalogStableId);
        type = PolarisEntityType.TASK;
      }
      case PrincipalObj ignored -> {
        realmMandatory(catalogStableId);
        type = PolarisEntityType.PRINCIPAL;
      }
      case PrincipalRoleObj ignored -> {
        realmMandatory(catalogStableId);
        type = PolarisEntityType.PRINCIPAL_ROLE;
      }
      default ->
          throw new IllegalStateException(
              "Cannot map "
                  + objBase.type().targetClass().getSimpleName()
                  + " to a entity type/sub-type");
    }

    // TODO
    var parentId = 0L;

    return new EntityNameLookupRecord(
        catalogStableId,
        objBase.stableId(),
        parentId,
        objBase.name(),
        type.getCode(),
        subType.getCode());
  }

  @SuppressWarnings("unchecked")
  private static <R> R cast(Object r) {
    return (R) r;
  }

  static String referenceName(PolarisEntityType entityType, Optional<CatalogObj> catalog) {
    var catalogStableId = catalog.map(ObjBase::stableId).orElse(0L);
    return referenceName(entityType, catalogStableId);
  }

  static String referenceName(PolarisEntityType entityType, OptionalLong catalogId) {
    return referenceName(entityType, catalogId.orElse(0L));
  }

  static String referenceName(PolarisEntityType entityType, long catalogStableId) {
    return switch (entityType) {
      case CATALOG -> CatalogsObj.CATALOGS_REF_NAME;
      case PRINCIPAL -> PrincipalsObj.PRINCIPALS_REF_NAME;
      case PRINCIPAL_ROLE -> PrincipalRolesObj.PRINCIPAL_ROLES_REF_NAME;
      case TASK -> ImmediateTasksObj.IMMEDIATE_TASKS_REF_NAME;
      case ROOT -> RootObj.ROOT_REF_NAME;

      // per catalog
      case CATALOG_ROLE ->
          perCatalogReferenceName(CatalogRolesObj.CATALOG_ROLES_REF_NAME_PATTERN, catalogStableId);
      case NAMESPACE, ICEBERG_TABLE_LIKE ->
          perCatalogReferenceName(CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN, catalogStableId);

      default -> throw new IllegalArgumentException("Unsupported entity type: " + entityType);
    };
  }

  static String perCatalogReferenceName(String refNamePattern, long catalogStableId) {
    return format(refNamePattern, catalogStableId);
  }

  static void initializeRealmIfNecessary(Persistence persistence) {
    persistence.createReferenceSilent(RootObj.ROOT_REF_NAME);
    persistence.createReferenceSilent(CatalogsObj.CATALOGS_REF_NAME);
    persistence.createReferenceSilent(PrincipalsObj.PRINCIPALS_REF_NAME);
    persistence.createReferenceSilent(PrincipalRolesObj.PRINCIPAL_ROLES_REF_NAME);
    persistence.createReferenceSilent(RealmGrantsObj.REALM_GRANTS_REF_NAME);
    persistence.createReferenceSilent(ImmediateTasksObj.IMMEDIATE_TASKS_REF_NAME);
  }

  static void initializeCatalogIfNecessary(Persistence persistence, CatalogObj catalog) {
    persistence.createReferenceSilent(
        perCatalogReferenceName(
            CatalogRolesObj.CATALOG_ROLES_REF_NAME_PATTERN, catalog.stableId()));
    persistence.createReferenceSilent(
        perCatalogReferenceName(
            CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN, catalog.stableId()));
    persistence.createReferenceSilent(
        perCatalogReferenceName(
            CatalogGrantsObj.CATALOG_GRANTS_REF_NAME_PATTERN, catalog.stableId()));
  }

  static void catalogMandatory(long catalogStableId) {
    checkArgument(catalogStableId != 0L, "Mandatory catalog not present");
  }

  static void realmMandatory(long catalogStableId) {
    checkArgument(catalogStableId == 0L, "Catalog present");
  }

  static String entitySubTypeCodeFromObjType(ObjRef objRef) {
    if (objRef != null) {
      var objType = ObjTypes.objTypeById(objRef.type());
      if (objType.equals(IcebergTableObj.TYPE)) {
        return Integer.toString(PolarisEntitySubType.TABLE.getCode());
      }
      if (objType.equals(IcebergViewObj.TYPE)) {
        return Integer.toString(PolarisEntitySubType.VIEW.getCode());
      }
    }
    return null;
  }

  static Optional<PolarisPrincipalSecrets> maybeObjToPolarisPrincipalSecrets(ObjBase obj) {
    if (obj instanceof PrincipalObj principalObj) {
      return Optional.of(principalObjToPolarisPrincipalSecrets(principalObj));
    }
    return Optional.empty();
  }

  static PolarisPrincipalSecrets principalObjToPolarisPrincipalSecrets(PrincipalObj principalObj) {
    return principalObjToPolarisPrincipalSecrets(principalObj, null);
  }

  static PolarisPrincipalSecrets principalObjToPolarisPrincipalSecrets(
      PrincipalObj principalObj, PolarisPrincipalSecrets newPrincipalSecrets) {
    return new PolarisPrincipalSecrets(
        principalObj.stableId(),
        principalObj.clientId(),
        newPrincipalSecrets != null ? newPrincipalSecrets.getMainSecret() : null,
        newPrincipalSecrets != null ? newPrincipalSecrets.getSecondarySecret() : null,
        principalObj.secretSalt().orElse(null),
        principalObj.mainSecretHash().orElse(null),
        principalObj.secondarySecretHash().orElse(null));
  }

  static void polarisPrincipalSecretsToPrincipal(
      PolarisPrincipalSecrets principalSecrets, PrincipalObj.Builder updatedPrincipal) {
    updatedPrincipal
        .clientId(principalSecrets.getPrincipalClientId())
        .mainSecretHash(principalSecrets.getMainSecretHash())
        .secondarySecretHash(principalSecrets.getSecondarySecretHash())
        .secretSalt(principalSecrets.getSecretSalt());
  }
}
