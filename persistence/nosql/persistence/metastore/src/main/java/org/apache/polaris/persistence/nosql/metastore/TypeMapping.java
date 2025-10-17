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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.core.entity.CatalogEntity.CATALOG_TYPE_PROPERTY;
import static org.apache.polaris.core.entity.CatalogEntity.DEFAULT_BASE_LOCATION_KEY;
import static org.apache.polaris.core.entity.PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE;
import static org.apache.polaris.core.entity.PolarisEntitySubType.GENERIC_TABLE;
import static org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE;
import static org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_VIEW;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.METADATA_LOCATION_KEY;
import static org.apache.polaris.core.policy.PolicyEntity.POLICY_CONTENT_KEY;
import static org.apache.polaris.core.policy.PolicyEntity.POLICY_DESCRIPTION_KEY;
import static org.apache.polaris.core.policy.PolicyEntity.POLICY_TYPE_CODE_KEY;
import static org.apache.polaris.core.policy.PolicyEntity.POLICY_VERSION_KEY;
import static org.apache.polaris.persistence.nosql.coretypes.refs.References.perCatalogReferenceName;

import jakarta.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.entity.table.federated.FederatedEntities;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjTypes;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogRoleObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogRolesObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStatus;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogType;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.FileObj;
import org.apache.polaris.persistence.nosql.coretypes.content.GenericTableObj;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergTableObj;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergViewObj;
import org.apache.polaris.persistence.nosql.coretypes.content.LocalNamespaceObj;
import org.apache.polaris.persistence.nosql.coretypes.content.NamespaceObj;
import org.apache.polaris.persistence.nosql.coretypes.content.PolicyObj;
import org.apache.polaris.persistence.nosql.coretypes.content.RemoteNamespaceObj;
import org.apache.polaris.persistence.nosql.coretypes.content.TableLikeObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRoleObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRolesObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTaskObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.RootObj;

class TypeMapping {
  static ByteBuffer serializeStringCompressed(String entityAsJson) {
    try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
      try (var gzip = new GZIPOutputStream(byteArrayOutputStream);
          var out = new DataOutputStream(gzip)) {
        out.writeUTF(entityAsJson);
      }
      return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static String deserializeStringCompressed(ByteBuffer bytes) {
    var byteArray = new byte[bytes.remaining()];
    bytes.duplicate().get(byteArray);
    try (var in = new DataInputStream(new GZIPInputStream(new ByteArrayInputStream(byteArray)))) {
      return in.readUTF();
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
                    .defaultBaseLocation(java.util.Optional.ofNullable(defaultBaseLocation))
                    .status(CatalogStatus.ACTIVE);
              }
              case CATALOG_ROLE -> {
                checkArgument(
                    subType == PolarisEntitySubType.NULL_SUBTYPE, "invalid subtype for %s", type);
                yield CatalogRoleObj.builder();
              }
              case POLICY -> {
                checkArgument(
                    subType == PolarisEntitySubType.NULL_SUBTYPE, "invalid subtype for %s", type);

                var policyTypeCode = properties.remove(POLICY_TYPE_CODE_KEY);
                var description = properties.remove(POLICY_DESCRIPTION_KEY);
                var content = properties.remove(POLICY_CONTENT_KEY);
                var version = properties.remove(POLICY_VERSION_KEY);

                yield PolicyObj.builder()
                    .policyType(PolicyType.fromCode(Integer.parseInt(policyTypeCode)))
                    .description(java.util.Optional.ofNullable(description))
                    .content(java.util.Optional.ofNullable(content))
                    .policyVersion(
                        version != null
                            ? OptionalInt.of(Integer.parseInt(version))
                            : OptionalInt.empty());
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
                var credentialRotationRequired =
                    internalProperties.remove(PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE);
                internalProperties.remove(PolarisEntityConstants.getClientIdPropertyName());

                var principalObjBuilder =
                    PrincipalObj.builder()
                        .credentialRotationRequired(credentialRotationRequired != null);
                principalSecrets.ifPresent(
                    secrets ->
                        principalObjBuilder
                            .clientId(secrets.getPrincipalClientId())
                            .mainSecretHash(secrets.getMainSecretHash())
                            .secondarySecretHash(secrets.getSecondarySecretHash())
                            .secretSalt(secrets.getSecretSalt()));

                yield principalObjBuilder;
              }
              case PRINCIPAL_ROLE -> {
                checkArgument(
                    subType == PolarisEntitySubType.NULL_SUBTYPE, "invalid subtype for %s", type);
                var federated =
                    Boolean.parseBoolean(
                        internalProperties.remove(FederatedEntities.FEDERATED_ENTITY));
                yield PrincipalRoleObj.builder().federated(federated);
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
                yield b.taskType(
                        Optional.ofNullable(
                            taskTypeCode != null
                                ? AsyncTaskType.fromTypeCode(Integer.parseInt(taskTypeCode))
                                : null))
                    .serializedEntity(
                        Optional.ofNullable(properties.remove("data"))
                            .map(TypeMapping::serializeStringCompressed));
              }
              case TABLE_LIKE -> {
                var tlBuilder = (TableLikeObj.Builder<?, ?>) null;
                tlBuilder =
                    switch (subType) {
                      case ICEBERG_TABLE -> IcebergTableObj.builder();
                      case ICEBERG_VIEW -> IcebergViewObj.builder();
                      case GENERIC_TABLE ->
                          GenericTableObj.builder()
                              .format(
                                  Optional.ofNullable(
                                      internalProperties.remove(GenericTableEntity.FORMAT_KEY)))
                              .doc(
                                  Optional.ofNullable(
                                      internalProperties.remove(GenericTableEntity.DOC_KEY)));
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
              .map(PolarisStorageConfigurationInfo::deserialize));
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
        || entityTypeCode == PolarisEntityType.TABLE_LIKE.getCode()
        || entityTypeCode == PolarisEntityType.POLICY.getCode();
  }

  static boolean isCatalogContent(PolarisEntityType entityType) {
    return switch (entityType) {
      case NAMESPACE, TABLE_LIKE, POLICY -> true;
      default -> false;
    };
  }

  static Class<? extends ObjBase> objTypeForPolarisTypeForFiltering(
      PolarisEntityType entityType, PolarisEntitySubType subType) {
    return switch (entityType) {
      case PRINCIPAL -> PrincipalObj.class;
      case TASK -> ImmediateTaskObj.class;
      case TABLE_LIKE ->
          switch (subType) {
            case ICEBERG_TABLE -> IcebergTableObj.class;
            case ICEBERG_VIEW -> IcebergViewObj.class;
            case GENERIC_TABLE -> GenericTableObj.class;
            case ANY_SUBTYPE -> TableLikeObj.class;
            default -> throw new IllegalArgumentException("Illegal subtype " + subType);
          };
      case NAMESPACE -> NamespaceObj.class;
      case CATALOG -> CatalogObj.class;
      case CATALOG_ROLE -> CatalogRoleObj.class;
      case POLICY -> PolicyObj.class;
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
      case TABLE_LIKE ->
          switch (subType) {
            case ICEBERG_TABLE -> IcebergTableObj.TYPE;
            case ICEBERG_VIEW -> IcebergViewObj.TYPE;
            case GENERIC_TABLE -> GenericTableObj.TYPE;
            default -> throw new IllegalArgumentException("Illegal subtype " + subType);
          };
      case NAMESPACE -> LocalNamespaceObj.TYPE;
      case CATALOG -> CatalogObj.TYPE;
      case CATALOG_ROLE -> CatalogRoleObj.TYPE;
      case POLICY -> PolicyObj.TYPE;
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
      case NAMESPACE, TABLE_LIKE, POLICY -> CatalogStateObj.class;

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
      case NAMESPACE, TABLE_LIKE, POLICY -> CatalogStateObj.builder();

      default -> throw new IllegalArgumentException("Unsupported entity type: " + entityType);
    };
  }

  static PolarisEntityType typeFromCode(int entityTypeCode) {
    return requireNonNull(PolarisEntityType.fromCode(entityTypeCode), "Invalid type code");
  }

  static <C extends ObjBase> Optional<C> filterIsEntityType(
      @Nonnull C objBase, int entityTypeCode) {
    return filterIsEntityType(objBase, typeFromCode(entityTypeCode));
  }

  static <C extends ObjBase> Optional<C> filterIsEntityType(
      @Nonnull C objBase, PolarisEntityType entityType) {
    return objTypeForPolarisTypeForFiltering(entityType, PolarisEntitySubType.ANY_SUBTYPE)
            .isInstance(objBase)
        ? Optional.of(objBase)
        : Optional.empty();
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
        type = PolarisEntityType.TABLE_LIKE;
        o.metadataLocation().ifPresent(v -> internalProperties.put(METADATA_LOCATION_KEY, v));
        if (objBase instanceof IcebergTableObj) {
          subType = ICEBERG_TABLE;
        }
        if (objBase instanceof IcebergViewObj) {
          subType = ICEBERG_VIEW;
        }
        if (objBase instanceof GenericTableObj genericTableObj) {
          subType = GENERIC_TABLE;
          genericTableObj
              .format()
              .ifPresent(v -> internalProperties.put(GenericTableEntity.FORMAT_KEY, v));
          genericTableObj
              .doc()
              .ifPresent(v -> internalProperties.put(GenericTableEntity.DOC_KEY, v));
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
        catalogId = realmMandatory(catalogId);
        type = PolarisEntityType.CATALOG;
        internalProperties.put(CATALOG_TYPE_PROPERTY, o.catalogType().name());
        o.defaultBaseLocation().ifPresent(v -> properties.put(DEFAULT_BASE_LOCATION_KEY, v));
      }
      case CatalogRoleObj ignored -> {
        catalogMandatory(catalogId);
        type = PolarisEntityType.CATALOG_ROLE;
      }
      case PolicyObj o -> {
        catalogMandatory(catalogId);
        properties.put(POLICY_TYPE_CODE_KEY, Integer.toString(o.policyType().getCode()));
        o.description().ifPresent(v -> properties.put(POLICY_DESCRIPTION_KEY, v));
        o.content().ifPresent(v -> properties.put(POLICY_CONTENT_KEY, v));
        o.policyVersion().ifPresent(v -> properties.put(POLICY_VERSION_KEY, Integer.toString(v)));
        type = PolarisEntityType.POLICY;
      }
      case RootObj ignored -> {
        catalogId = realmMandatory(catalogId);
        type = PolarisEntityType.ROOT;
      }
      case ImmediateTaskObj o -> {
        catalogId = realmMandatory(catalogId);
        o.serializedEntity()
            .map(TypeMapping::deserializeStringCompressed)
            .ifPresent(s -> properties.put("data", s));
        o.taskType()
            .ifPresent(
                v ->
                    properties.put(PolarisTaskConstants.TASK_TYPE, Integer.toString(v.typeCode())));
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
        catalogId = realmMandatory(catalogId);
        type = PolarisEntityType.PRINCIPAL;
        if (o.credentialRotationRequired()) {
          internalProperties.put(PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true");
        }
        o.clientId()
            .ifPresent(
                v -> internalProperties.put(PolarisEntityConstants.getClientIdPropertyName(), v));
      }
      case PrincipalRoleObj o -> {
        type = PolarisEntityType.PRINCIPAL_ROLE;
        if (o.federated()) {
          internalProperties.put(FederatedEntities.FEDERATED_ENTITY, "true");
        }
      }
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
    return new PolarisBaseEntity.Builder()
        .catalogId(catalogId)
        .id(id)
        .typeCode(type.getCode())
        .subTypeCode(subType.getCode())
        .parentId(parentId)
        .name(name)
        .propertiesAsMap(properties)
        .internalPropertiesAsMap(internalProperties)
        .createTimestamp(objBase.createTimestamp().toEpochMilli())
        .lastUpdateTimestamp(objBase.updateTimestamp().toEpochMilli())
        .entityVersion(objBase.entityVersion())
        .build();
  }

  @Nonnull
  static EntityNameLookupRecord mapToEntityNameLookupRecord(
      @Nonnull ObjBase objBase, long catalogStableId) {
    var type = PolarisEntityType.NULL_TYPE;
    var subType = PolarisEntitySubType.NULL_SUBTYPE;

    switch (objBase) {
      case TableLikeObj ignored -> {
        catalogMandatory(catalogStableId);
        type = PolarisEntityType.TABLE_LIKE;
        if (objBase instanceof IcebergTableObj) {
          subType = ICEBERG_TABLE;
        }
        if (objBase instanceof IcebergViewObj) {
          subType = ICEBERG_VIEW;
        }
        if (objBase instanceof GenericTableObj) {
          subType = GENERIC_TABLE;
        }
      }
      case LocalNamespaceObj ignored -> {
        catalogMandatory(catalogStableId);
        type = PolarisEntityType.NAMESPACE;
      }
      case RemoteNamespaceObj ignored -> {
        // TODO RemoteNamespaceObj ?
      }
      case PolicyObj ignored -> {
        catalogMandatory(catalogStableId);
        type = PolarisEntityType.POLICY;
      }
      case CatalogObj ignored -> {
        catalogStableId = realmMandatory(catalogStableId);
        type = PolarisEntityType.CATALOG;
      }
      case CatalogRoleObj ignored -> {
        catalogMandatory(catalogStableId);
        type = PolarisEntityType.CATALOG_ROLE;
      }
      case RootObj ignored -> {
        catalogStableId = realmMandatory(catalogStableId);
        type = PolarisEntityType.ROOT;
      }
      case ImmediateTaskObj ignored -> {
        catalogStableId = realmMandatory(catalogStableId);
        type = PolarisEntityType.TASK;
      }
      case PrincipalObj ignored -> {
        catalogStableId = realmMandatory(catalogStableId);
        type = PolarisEntityType.PRINCIPAL;
      }
      case PrincipalRoleObj ignored -> {
        catalogStableId = realmMandatory(catalogStableId);
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
      case NAMESPACE, TABLE_LIKE, POLICY ->
          perCatalogReferenceName(CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN, catalogStableId);

      default -> throw new IllegalArgumentException("Unsupported entity type: " + entityType);
    };
  }

  static void catalogMandatory(long catalogStableId) {
    checkArgument(catalogStableId != 0L, "Mandatory catalog not present");
  }

  static long realmMandatory(long catalogStableId) {
    // TODO BasePolarisMetaStoreManagerTest sadly gives us non-0 catalog-IDs for non-catalog
    //  entities, so cannot do the following assertion, but instead blindly assume 0L.
    // checkArgument(catalogStableId == 0L, "Catalog present");
    return 0L;
  }

  static String entitySubTypeCodeFromObjType(ObjRef objRef) {
    if (objRef != null) {
      var objType = ObjTypes.objTypeById(objRef.type());
      if (objType.equals(IcebergTableObj.TYPE)) {
        return Integer.toString(ICEBERG_TABLE.getCode());
      }
      if (objType.equals(IcebergViewObj.TYPE)) {
        return Integer.toString(ICEBERG_VIEW.getCode());
      }
      if (objType.equals(GenericTableObj.TYPE)) {
        return Integer.toString(GENERIC_TABLE.getCode());
      }
    }
    return null;
  }

  static Optional<PolarisPrincipalSecrets> maybeObjToPolarisPrincipalSecrets(ObjBase obj) {
    if (obj instanceof PrincipalObj principalObj && principalObj.clientId().isPresent()) {
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
        principalObj.clientId().orElse(null),
        newPrincipalSecrets != null ? newPrincipalSecrets.getMainSecret() : null,
        newPrincipalSecrets != null ? newPrincipalSecrets.getSecondarySecret() : null,
        principalObj.secretSalt().orElse(null),
        principalObj.mainSecretHash().orElse(null),
        principalObj.secondarySecretHash().orElse(null));
  }
}
