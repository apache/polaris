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

import static org.apache.polaris.persistence.nosql.api.index.IndexContainer.newUpdatableIndex;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.coretypes.realm.RealmGrantsObj.REALM_GRANTS_REF_NAME;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.commit.CommitException;
import org.apache.polaris.persistence.nosql.api.commit.CommitRetryable;
import org.apache.polaris.persistence.nosql.api.commit.CommitterState;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.UpdatableIndex;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.authz.api.Privilege;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;
import org.apache.polaris.persistence.nosql.coretypes.acl.AclObj;
import org.apache.polaris.persistence.nosql.coretypes.acl.GrantsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.RealmGrantsObj;
import org.apache.polaris.persistence.nosql.metastore.indexaccess.MemoizedIndexedAccess;
import org.apache.polaris.persistence.nosql.metastore.privs.GrantTriplet;
import org.apache.polaris.persistence.nosql.metastore.privs.SecurableGranteePrivilegeTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record GrantsMutation(
    Persistence persistence,
    MemoizedIndexedAccess memoizedIndexedAccess,
    Privileges privileges,
    boolean doGrant,
    SecurableGranteePrivilegeTuple... grants) {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrantsMutation.class);

  public boolean apply() {
    try {
      return persistence
          .createCommitter(REALM_GRANTS_REF_NAME, GrantsObj.class, String.class)
          .synchronizingLocally()
          .commitRuntimeException(
              new CommitRetryable<>() {
                @Nonnull
                @Override
                public Optional<GrantsObj> attempt(
                    @Nonnull CommitterState<GrantsObj, String> state,
                    @Nonnull Supplier<Optional<GrantsObj>> refObjSupplier)
                    throws CommitException {
                  var persistence = state.persistence();
                  var refObj = refObjSupplier.get();

                  var ref = RealmGrantsObj.builder();
                  refObj.ifPresent(ref::from);

                  var securablesIndex =
                      refObj
                          .map(GrantsObj::acls)
                          .map(c -> c.asUpdatableIndex(persistence, OBJ_REF_SERIALIZER))
                          .orElseGet(() -> newUpdatableIndex(persistence, OBJ_REF_SERIALIZER));

                  var changed = false;
                  for (var g : grants) {
                    var securable = GrantTriplet.forEntity(g.securable());
                    var grantee = GrantTriplet.forEntity(g.grantee());
                    var forSec = grantee.asDirected();
                    var privilege = privileges.byName(g.privilege().name());
                    changed |=
                        processGrant(state, securable, forSec, securablesIndex, privilege, doGrant);
                    changed |=
                        processGrant(
                            state, grantee, securable, securablesIndex, privilege, doGrant);
                  }

                  if (!changed) {
                    return state.noCommit();
                  }

                  ref.acls(securablesIndex.toIndexed("idx-sec-", state::writeOrReplace));

                  return commitResult(state, ref, refObj);
                }

                // Some fun with Java generics...
                @SuppressWarnings({"unchecked", "rawtypes"})
                private static <REF_BUILDER> Optional<GrantsObj> commitResult(
                    CommitterState<GrantsObj, String> state,
                    REF_BUILDER ref,
                    Optional<GrantsObj> refObj) {
                  var cs = (CommitterState) state;
                  var refBuilder = (BaseCommitObj.Builder) ref;
                  return cs.commitResult("", refBuilder, refObj);
                }

                private boolean processGrant(
                    CommitterState<? extends GrantsObj, String> state,
                    GrantTriplet aclTriplet,
                    GrantTriplet grantee,
                    UpdatableIndex<ObjRef> securablesIndex,
                    Privilege privilege,
                    boolean doGrant) {

                  var aclName = aclTriplet.toRoleName();
                  var granteeRoleName = grantee.toRoleName();

                  LOGGER.trace(
                      "{} {} {} '{}' ({}) on '{}' in ACL '{}' ({})",
                      doGrant ? "Granting" : "Revoking",
                      privilege.name(),
                      doGrant ? "on" : "from",
                      granteeRoleName,
                      PolarisEntityType.fromCode(grantee.typeCode()),
                      REALM_GRANTS_REF_NAME,
                      aclName,
                      PolarisEntityType.fromCode(aclTriplet.typeCode()));

                  var aclKey = IndexKey.key(aclName);

                  var aclRef = securablesIndex.get(aclKey);
                  var aclObjOptional =
                      Optional.ofNullable(aclRef)
                          .map(r -> state.persistence().fetch(r, AclObj.class));
                  var aclObjBuilder =
                      aclObjOptional
                          .map(AclObj.builder()::from)
                          .orElseGet(AclObj::builder)
                          .id(persistence.generateId())
                          .securableId(aclTriplet.id())
                          .securableTypeCode(aclTriplet.typeCode());

                  var aclBuilder =
                      aclObjOptional
                          .map(o -> privileges.newAclBuilder().from(o.acl()))
                          .orElseGet(privileges::newAclBuilder);

                  aclBuilder.modify(
                      granteeRoleName,
                      aclEntryBuilder -> {
                        if (doGrant) {
                          aclEntryBuilder.grant(privilege);
                        } else {
                          aclEntryBuilder.revoke(privilege);
                        }
                      });

                  var acl = aclBuilder.build();
                  if (aclObjOptional.map(obj -> obj.acl().equals(acl)).orElseGet(() -> !doGrant)) {
                    // aclObj not changed
                    return false;
                  }
                  aclObjBuilder.acl(acl);

                  var aclObj = aclObjBuilder.build();

                  state.writeOrReplace("acl-" + aclTriplet.id(), aclObj);

                  securablesIndex.put(aclKey, objRef(aclObj));

                  // aclObj changed
                  return true;
                }
              })
          .isPresent();
    } finally {
      memoizedIndexedAccess.invalidateGrantsIndex();
    }
  }
}
