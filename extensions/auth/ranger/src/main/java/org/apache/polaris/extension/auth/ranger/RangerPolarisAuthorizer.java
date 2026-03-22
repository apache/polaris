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
package org.apache.polaris.extension.auth.ranger;

import static org.apache.polaris.core.entity.PolarisEntityConstants.getRootPrincipalName;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.extension.auth.ranger.utils.RangerUtils;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.embedded.RangerEmbeddedAuthorizer;
import org.apache.ranger.authz.model.RangerAccessContext;
import org.apache.ranger.authz.model.RangerAccessInfo;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.apache.ranger.authz.model.RangerUserInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Authorizes operations based on policies defined in Apache Ranger. */
public class RangerPolarisAuthorizer implements PolarisAuthorizer {
  private static final Logger LOG = LoggerFactory.getLogger(RangerPolarisAuthorizer.class);

  public static final String SERVICE_TYPE = "polaris";

  private static final String OPERATION_NOT_ALLOWED_FOR_USER_ERROR =
      "Principal '%s' is not authorized for op %s due to PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE";
  private static final String ROOT_PRINCIPAL_NEEDED_ERROR =
      "Principal '%s' is not authorized for op %s as only root principal can perform this operation";
  private static final String RANGER_AUTH_FAILED_ERROR =
      "Principal '%s' is not authorized for op '%s'";
  private static final String RANGER_UNSUPPORTED_OPERATION =
      "Operation %s is not supported by Ranger authorizer";

  private static final Set<PolarisAuthorizableOperation> AUTHORIZED_OPERATIONS =
      initAuthorizedOperations();

  private final RangerEmbeddedAuthorizer authorizer;
  private final String serviceName;
  private final boolean enforceCredentialRotationRequiredState;

  public RangerPolarisAuthorizer(
      RangerEmbeddedAuthorizer authorizer, String serviceName, RealmConfig realmConfig) {
    this.authorizer = authorizer;
    this.serviceName = serviceName;
    this.enforceCredentialRotationRequiredState =
        realmConfig.getConfig(
            FeatureConfiguration.ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING);
  }

  @Override
  public void resolveAuthorizationInputs(
      @Nonnull AuthorizationState authzState, @Nonnull AuthorizationRequest request) {
    throw new UnsupportedOperationException(
        "resolveAuthorizationInputs is not implemented yet for RangerPolarisAuthorizer");
  }

  @Override
  public @Nonnull AuthorizationDecision authorize(
      @Nonnull AuthorizationState authzState, @Nonnull AuthorizationRequest request) {
    throw new UnsupportedOperationException(
        "authorize is not implemented yet for RangerPolarisAuthorizer");
  }

  @Override
  public void authorizeOrThrow(
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull Set<PolarisBaseEntity> activatedEntities,
      @Nonnull PolarisAuthorizableOperation authzOp,
      @Nullable PolarisResolvedPathWrapper target,
      @Nullable PolarisResolvedPathWrapper secondary) {
    authorizeOrThrow(
        polarisPrincipal,
        activatedEntities,
        authzOp,
        target == null ? null : List.of(target),
        secondary == null ? null : List.of(secondary));
  }

  @Override
  public void authorizeOrThrow(
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull Set<PolarisBaseEntity> activatedEntities,
      @Nonnull PolarisAuthorizableOperation authzOp,
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries) {

    RangerPolarisOperationSemantics semantics =
        RangerPolarisOperationSemantics.forOperation(authzOp);

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "authorizeOrThrow(principal={}, activatedEntities={}, authzOp={name: {}, targetPrivileges: {}, secondaryPrivileges: {}), targets={}, secondaries={}",
          polarisPrincipal,
          activatedEntities,
          authzOp,
          semantics.targetPrivileges(),
          semantics.secondaryPrivileges(),
          targets,
          secondaries);
    }

    try {
      if (enforceCredentialRotationRequiredState
          && authzOp != PolarisAuthorizableOperation.ROTATE_CREDENTIALS
          && polarisPrincipal
              .getProperties()
              .containsKey(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)) {
        throw new ForbiddenException(
            OPERATION_NOT_ALLOWED_FOR_USER_ERROR, polarisPrincipal.getName(), authzOp.name());
      }

      if (authzOp == PolarisAuthorizableOperation.RESET_CREDENTIALS) {
        boolean isRootPrincipal = getRootPrincipalName().equals(polarisPrincipal.getName());

        if (!isRootPrincipal) {
          // TODO: enable ranger audit from here to ensure that the request denied captured.
          throw new ForbiddenException(
              ROOT_PRINCIPAL_NEEDED_ERROR, polarisPrincipal.getName(), authzOp.name());
        }
      } else if (!AUTHORIZED_OPERATIONS.contains(authzOp)) {
        throw new ForbiddenException(RANGER_UNSUPPORTED_OPERATION, authzOp.name());
      } else if (!isAccessAuthorized(polarisPrincipal, authzOp, targets, secondaries)) {
        throw new ForbiddenException(
            RANGER_AUTH_FAILED_ERROR, polarisPrincipal.getName(), authzOp.name());
      }
    } catch (RangerAuthzException excp) {
      throw new IllegalStateException(
          "Failed to authorize principal " + polarisPrincipal + " for op {}" + authzOp, excp);
    }
  }

  private boolean isAccessAuthorized(
      @Nonnull PolarisPrincipal principal,
      @Nonnull PolarisAuthorizableOperation authzOp,
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries)
      throws RangerAuthzException {
    boolean isTargetSpecified = targets != null && !targets.isEmpty();
    boolean isSecondarySpecified = secondaries != null && !secondaries.isEmpty();
    List<RangerAccessInfo> accessInfos = new ArrayList<>();

    RangerPolarisOperationSemantics semantics =
        RangerPolarisOperationSemantics.forOperation(authzOp);

    if (!semantics.targetPrivileges().isEmpty()) {
      Preconditions.checkState(
          isTargetSpecified,
          "No target provided to authorize %s for privileges %s",
          authzOp,
          semantics.targetPrivileges());

      for (PolarisResolvedPathWrapper target : targets) {
        accessInfos.add(RangerUtils.toAccessInfo(target, authzOp, semantics.targetPrivileges()));
      }
    } else if (isTargetSpecified) {
      LOG.warn(
          "No privileges specified for target authorization. Ignoring target {}, op: {}, user: {}",
          RangerUtils.toResourcePath(targets),
          authzOp.name(),
          principal.getName());
    }

    if (!semantics.secondaryPrivileges().isEmpty()) {
      Preconditions.checkState(
          isSecondarySpecified,
          "No secondaries provided to authorize %s for privileges %s",
          authzOp,
          semantics.secondaryPrivileges());

      for (PolarisResolvedPathWrapper secondary : secondaries) {
        accessInfos.add(
            RangerUtils.toAccessInfo(secondary, authzOp, semantics.secondaryPrivileges()));
      }
    } else if (isSecondarySpecified) {
      LOG.warn(
          "No privileges specified for secondary authorization. Ignoring secondaries {}, op: {}, user: {}",
          RangerUtils.toResourcePath(secondaries),
          authzOp.name(),
          principal.getName());
    }

    RangerUserInfo userInfo = RangerUtils.toUserInfo(principal);
    RangerAccessContext context = new RangerAccessContext(SERVICE_TYPE, serviceName);
    RangerMultiAuthzRequest authzRequest =
        new RangerMultiAuthzRequest(userInfo, accessInfos, context);
    RangerMultiAuthzResult authzResult = authorizer.authorize(authzRequest);
    boolean isAllowed = RangerAuthzResult.AccessDecision.ALLOW.equals(authzResult.getDecision());

    if (LOG.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder();

      sb.append("User=")
          .append(userInfo.getName())
          .append(", operation=")
          .append(authzOp)
          .append(", result=[");
      for (int i = 0; i < accessInfos.size(); i++) {
        if (i > 0) {
          sb.append(",");
        }

        RangerAccessInfo accessInfo = accessInfos.get(i);
        RangerAuthzResult accessResult = authzResult.getAccesses().get(i);

        sb.append("{resource=")
            .append(accessInfo.getResource())
            .append(", decision=")
            .append(accessResult.getDecision())
            .append("}");
      }
      sb.append("]");
      sb.append(", isAllowed=").append(isAllowed);

      LOG.debug(sb.toString());
    }

    return isAllowed;
  }

  private static Set<PolarisAuthorizableOperation> initAuthorizedOperations() {
    Set<PolarisAuthorizableOperation> ret = new HashSet<>();

    for (PolarisAuthorizableOperation op : PolarisAuthorizableOperation.values()) {
      if (isAuthorizable(op)) {
        ret.add(op);
      }
    }

    return ret;
  }

  private static boolean isAuthorizable(PolarisAuthorizableOperation op) {
    switch (op) {
      case CREATE_PRINCIPAL:
      case DELETE_PRINCIPAL:
      case UPDATE_PRINCIPAL:
      case GET_PRINCIPAL:
      case LIST_PRINCIPALS:
      case ROTATE_CREDENTIALS:
      case RESET_CREDENTIALS:
        return true;

      case CREATE_CATALOG:
      case DELETE_CATALOG:
      case UPDATE_CATALOG:
      case GET_CATALOG:
      case LIST_CATALOGS:
      case ATTACH_POLICY_TO_CATALOG:
      case DETACH_POLICY_FROM_CATALOG:
      case GET_APPLICABLE_POLICIES_ON_CATALOG:
        return true;

      case CREATE_NAMESPACE:
      case DROP_NAMESPACE:
      case UPDATE_NAMESPACE_PROPERTIES:
      case LIST_NAMESPACES:
      case NAMESPACE_EXISTS:
      case LOAD_NAMESPACE_METADATA:
      case ATTACH_POLICY_TO_NAMESPACE:
      case DETACH_POLICY_FROM_NAMESPACE:
      case GET_APPLICABLE_POLICIES_ON_NAMESPACE:
        return true;

      case CREATE_TABLE_DIRECT:
      case CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION:
      case CREATE_TABLE_STAGED:
      case CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION:
      case REGISTER_TABLE:
      case DROP_TABLE_WITHOUT_PURGE:
      case DROP_TABLE_WITH_PURGE:
      case UPDATE_TABLE:
      case UPDATE_TABLE_FOR_STAGED_CREATE:
      case RENAME_TABLE:
      case LIST_TABLES:
      case TABLE_EXISTS:
      case LOAD_TABLE:
      case LOAD_TABLE_WITH_READ_DELEGATION:
      case LOAD_TABLE_WITH_WRITE_DELEGATION:
      case COMMIT_TRANSACTION:
      case ATTACH_POLICY_TO_TABLE:
      case DETACH_POLICY_FROM_TABLE:
      case GET_APPLICABLE_POLICIES_ON_TABLE:
      case REPORT_READ_METRICS:
      case REPORT_WRITE_METRICS:
      case ASSIGN_TABLE_UUID:
      case UPGRADE_TABLE_FORMAT_VERSION:
      case ADD_TABLE_SCHEMA:
      case SET_TABLE_CURRENT_SCHEMA:
      case ADD_TABLE_PARTITION_SPEC:
      case ADD_TABLE_SORT_ORDER:
      case SET_TABLE_DEFAULT_SORT_ORDER:
      case ADD_TABLE_SNAPSHOT:
      case SET_TABLE_SNAPSHOT_REF:
      case REMOVE_TABLE_SNAPSHOTS:
      case REMOVE_TABLE_SNAPSHOT_REF:
      case SET_TABLE_LOCATION:
      case SET_TABLE_PROPERTIES:
      case REMOVE_TABLE_PROPERTIES:
      case SET_TABLE_STATISTICS:
      case REMOVE_TABLE_STATISTICS:
      case REMOVE_TABLE_PARTITION_SPECS:
        return true;

      case CREATE_VIEW:
      case DROP_VIEW:
      case REPLACE_VIEW:
      case RENAME_VIEW:
      case LIST_VIEWS:
      case VIEW_EXISTS:
      case LOAD_VIEW:
        return true;

      case CREATE_POLICY:
      case DROP_POLICY:
      case UPDATE_POLICY:
      case LIST_POLICY:
      case LOAD_POLICY:
        return true;

      case SEND_NOTIFICATIONS:
        return true;

      default:
        LOG.error("{}: unsupported operation", op);

        return false;
    }
  }
}
