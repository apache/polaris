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

package org.apache.polaris.persistence.nosql.metastore.mutation;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.mapToEntity;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.mapToObj;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.principalObjToPolarisPrincipalSecrets;
import static org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj.PRINCIPALS_REF_NAME;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.commit.CommitException;
import org.apache.polaris.persistence.nosql.api.commit.CommitterState;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.UpdatableIndex;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj;
import org.apache.polaris.persistence.nosql.metastore.committers.ChangeResult;
import org.apache.polaris.persistence.nosql.metastore.committers.PrincipalsChangeCommitter;
import org.apache.polaris.persistence.nosql.metastore.committers.PrincipalsChangeCommitterWrapper;
import org.apache.polaris.persistence.nosql.metastore.indexaccess.MemoizedIndexedAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PrincipalMutations<RESULT> implements PrincipalsChangeCommitter<RESULT> {
  private final Persistence persistence;
  private final MemoizedIndexedAccess memoizedIndexedAccess;

  protected PrincipalMutations(
      Persistence persistence, MemoizedIndexedAccess memoizedIndexedAccess) {
    this.persistence = persistence;
    this.memoizedIndexedAccess = memoizedIndexedAccess;
  }

  PrincipalSecretsGenerator secretsGenerator(@Nullable RootCredentialsSet rootCredentialsSet) {
    if (rootCredentialsSet != null) {
      var realmId = this.persistence.realmId();
      return PrincipalSecretsGenerator.bootstrap(realmId, rootCredentialsSet);
    } else {
      return PrincipalSecretsGenerator.RANDOM_SECRETS;
    }
  }

  abstract Class<RESULT> resultType();

  public RESULT apply() {
    try {
      return persistence
          .createCommitter(PRINCIPALS_REF_NAME, PrincipalsObj.class, resultType())
          .synchronizingLocally()
          .commitRuntimeException(new PrincipalsChangeCommitterWrapper<>(this))
          .orElseThrow();
    } finally {
      memoizedIndexedAccess.invalidateIndexedAccess(0L, PolarisEntityType.PRINCIPAL.getCode());
    }
  }

  public static final class UpdateSecrets<R> extends PrincipalMutations<R> {

    @FunctionalInterface
    public interface SecretsUpdater<R> {
      R update(PrincipalObj principalObj, PrincipalObj.Builder principalObjBuilder);
    }

    static class PrincipalNotFoundException extends RuntimeException {}

    private final Class<R> resultType;
    private final long principalId;
    private final SecretsUpdater<R> updater;

    public UpdateSecrets(
        Persistence persistence,
        MemoizedIndexedAccess memoizedIndexedAccess,
        Class<R> resultType,
        long principalId,
        SecretsUpdater<R> updater) {
      super(persistence, memoizedIndexedAccess);
      this.resultType = resultType;
      this.principalId = principalId;
      this.updater = updater;
    }

    @Override
    Class<R> resultType() {
      return resultType;
    }

    @Nonnull
    @Override
    public ChangeResult<R> change(
        @Nonnull CommitterState<PrincipalsObj, R> state,
        @Nonnull PrincipalsObj.Builder ref,
        @Nonnull UpdatableIndex<ObjRef> byName,
        @Nonnull UpdatableIndex<IndexKey> byId,
        @Nonnull UpdatableIndex<ObjRef> byClientId)
        throws CommitException {
      var principalIdName = byId.get(IndexKey.key(principalId));
      if (principalIdName == null) {
        throw new PrincipalNotFoundException();
      }
      var principalObjRef = byName.get(principalIdName);
      if (principalObjRef == null) {
        throw new PrincipalNotFoundException();
      }

      var persistence = state.persistence();
      var principal = persistence.fetch(principalObjRef, PrincipalObj.class);
      if (principal == null) {
        throw new PrincipalNotFoundException();
      }

      var updatedPrincipalBuilder =
          PrincipalObj.builder()
              .from(principal)
              .id(persistence.generateId())
              .updateTimestamp(persistence.currentInstant());

      var apiResult = updater.update(principal, updatedPrincipalBuilder);

      var updatedPrincipal = updatedPrincipalBuilder.build();

      ObjRef updatedPrincipalObjRef = objRef(updatedPrincipal);
      byName.put(IndexKey.key(updatedPrincipal.name()), updatedPrincipalObjRef);

      principal.clientId().map(IndexKey::key).ifPresent(byClientId::remove);
      updatedPrincipal
          .clientId()
          .ifPresent(
              clientId -> {
                var clientIdKey = IndexKey.key(clientId);
                byClientId.put(clientIdKey, updatedPrincipalObjRef);
              });

      state.writeOrReplace("principal", updatedPrincipal);

      return new ChangeResult.CommitChange<>(apiResult);
    }
  }

  public static final class CreatePrincipal extends PrincipalMutations<CreatePrincipalResult> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreatePrincipal.class);
    private final PolarisBaseEntity principal;
    private final RootCredentialsSet rootCredentialsSet;

    public CreatePrincipal(
        Persistence persistence,
        MemoizedIndexedAccess memoizedIndexedAccess,
        PolarisBaseEntity principal,
        RootCredentialsSet rootCredentialsSet) {
      super(persistence, memoizedIndexedAccess);
      this.principal = principal;
      this.rootCredentialsSet = rootCredentialsSet;
    }

    @Override
    Class<CreatePrincipalResult> resultType() {
      return CreatePrincipalResult.class;
    }

    @Nonnull
    @Override
    public ChangeResult<CreatePrincipalResult> change(
        @Nonnull CommitterState<PrincipalsObj, CreatePrincipalResult> state,
        @Nonnull PrincipalsObj.Builder ref,
        @Nonnull UpdatableIndex<ObjRef> byName,
        @Nonnull UpdatableIndex<IndexKey> byId,
        @Nonnull UpdatableIndex<ObjRef> byClientId)
        throws CommitException {
      var principalName = principal.getName();
      var principalId = principal.getId();
      var nameKey = IndexKey.key(principalName);
      var persistence = state.persistence();

      var existingPrincipal =
          Optional.ofNullable(byName.get(nameKey))
              .map(objRef -> persistence.fetch(objRef, PrincipalObj.class));
      if (existingPrincipal.isPresent()) {
        var existing = existingPrincipal.get();
        var secrets = principalObjToPolarisPrincipalSecrets(existing);
        var forComparison =
            MutationAttempt.objForChangeComparison(principal, Optional.of(secrets), existing);
        return new ChangeResult.NoChange<>(
            existing.equals(forComparison)
                ? new CreatePrincipalResult(principal, secrets)
                : new CreatePrincipalResult(ENTITY_ALREADY_EXISTS, null));
      }

      LOGGER.debug("Creating principal '{}' ...", principalName);

      PolarisPrincipalSecrets newPrincipalSecrets;
      while (true) {
        newPrincipalSecrets =
            secretsGenerator(rootCredentialsSet).produceSecrets(principalName, principalId);
        var newClientId = newPrincipalSecrets.getPrincipalClientId();
        if (byClientId.get(IndexKey.key(newClientId)) == null) {
          LOGGER.debug("Generated secrets for principal '{}' ...", principalName);
          break;
        }
      }

      var now = persistence.currentInstant();
      // Map from the given entity to retain both the properties and internal-properties bags
      // (for example, PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)
      var updatedPrincipalBuilder =
          mapToObj(principal, Optional.of(newPrincipalSecrets))
              .name(principalName)
              .stableId(principalId)
              .entityVersion(1)
              .createTimestamp(now)
              .updateTimestamp(now)
              .id(persistence.generateId());
      var updatedPrincipal = updatedPrincipalBuilder.build();

      var updatedPrincipalObjRef = objRef(updatedPrincipal);
      byClientId.put(
          IndexKey.key(newPrincipalSecrets.getPrincipalClientId()), updatedPrincipalObjRef);
      byName.put(nameKey, updatedPrincipalObjRef);
      byId.put(IndexKey.key(principalId), nameKey);

      state.writeOrReplace("principal", updatedPrincipal);

      // return those
      return new ChangeResult.CommitChange<>(
          new CreatePrincipalResult(
              mapToEntity(updatedPrincipal, 0L),
              principalObjToPolarisPrincipalSecrets(
                  (PrincipalObj) updatedPrincipal, newPrincipalSecrets)));
    }
  }

  // TODO remove this?
  //  The code is not accessible via PolarisMetaStoreManager, would only be via BasePersistence.
  public static final class GenerateNewSecrets extends PrincipalMutations<PolarisPrincipalSecrets> {
    private final String principalName;
    private final long principalId;

    public GenerateNewSecrets(
        Persistence persistence,
        MemoizedIndexedAccess memoizedIndexedAccess,
        String principalName,
        long principalId) {
      super(persistence, memoizedIndexedAccess);
      this.principalName = principalName;
      this.principalId = principalId;
    }

    @Override
    Class<PolarisPrincipalSecrets> resultType() {
      return PolarisPrincipalSecrets.class;
    }

    @Nonnull
    @Override
    public ChangeResult<PolarisPrincipalSecrets> change(
        @Nonnull CommitterState<PrincipalsObj, PolarisPrincipalSecrets> state,
        @Nonnull PrincipalsObj.Builder ref,
        @Nonnull UpdatableIndex<ObjRef> byName,
        @Nonnull UpdatableIndex<IndexKey> byId,
        @Nonnull UpdatableIndex<ObjRef> byClientId)
        throws CommitException {
      var nameKey = IndexKey.key(principalName);
      var principalObjRef = byName.get(nameKey);

      var pers = state.persistence();
      var existingPrincipal =
          Optional.ofNullable(principalObjRef)
              .map(objRef -> pers.fetch(objRef, PrincipalObj.class));

      checkState(
          existingPrincipal.isEmpty() || principalId == existingPrincipal.get().stableId(),
          "Principal id mismatch");

      // generate new secrets
      PolarisPrincipalSecrets newPrincipalSecrets;
      while (true) {
        newPrincipalSecrets = secretsGenerator(null).produceSecrets(principalName, principalId);
        var newClientId = newPrincipalSecrets.getPrincipalClientId();
        if (byClientId.get(IndexKey.key(newClientId)) == null) {
          break;
        }
      }

      var updatedPrincipalBuilder = PrincipalObj.builder();
      existingPrincipal.ifPresent(updatedPrincipalBuilder::from);
      var now = pers.currentInstant();
      if (existingPrincipal.isEmpty()) {
        updatedPrincipalBuilder
            .name(principalName)
            .stableId(principalId)
            .entityVersion(1)
            .createTimestamp(now);
      }
      updatedPrincipalBuilder
          .id(pers.generateId())
          .updateTimestamp(now)
          .clientId(newPrincipalSecrets.getPrincipalClientId())
          .mainSecretHash(newPrincipalSecrets.getMainSecretHash())
          .secondarySecretHash(newPrincipalSecrets.getSecondarySecretHash())
          .secretSalt(newPrincipalSecrets.getSecretSalt());
      var updatedPrincipal = updatedPrincipalBuilder.build();

      existingPrincipal
          .flatMap(PrincipalObj::clientId)
          .map(IndexKey::key)
          .ifPresent(byClientId::remove);
      var updatedPrincipalObjRef = objRef(updatedPrincipal);
      updatedPrincipal
          .clientId()
          .ifPresent(c -> byClientId.put(IndexKey.key(c), updatedPrincipalObjRef));
      byName.put(nameKey, updatedPrincipalObjRef);
      byId.put(IndexKey.key(principalId), nameKey);

      state.writeOrReplace("principal", updatedPrincipal);

      // return those
      return new ChangeResult.CommitChange<>(newPrincipalSecrets);
    }
  }
}
