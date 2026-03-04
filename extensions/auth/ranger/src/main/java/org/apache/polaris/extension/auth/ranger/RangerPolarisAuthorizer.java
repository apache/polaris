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
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
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
import org.apache.ranger.authz.api.RangerAuthorizer;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.embedded.RangerEmbeddedAuthorizer;
import org.apache.ranger.authz.model.RangerAccessContext;
import org.apache.ranger.authz.model.RangerAccessInfo;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.apache.ranger.authz.model.RangerUserInfo;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Authorizes operations based on policies defined in Apache Ranger. */
public class RangerPolarisAuthorizer implements PolarisAuthorizer {
  private static final Logger LOG = LoggerFactory.getLogger(RangerPolarisAuthorizer.class);

  public static final String SERVICE_TYPE = "polaris";
  public static final String SERVICE_NAME_PROPERTY = "ranger.plugin.polaris.service.name";

  private static final String OPERATION_NOT_ALLOWED_FOR_USER_ERROR =
      "Principal '%s' is not authorized for op %s due to PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE";
  private static final String ROOT_PRINCIPLE_NEEDED_ERROR =
      "Principal '%s' is not authorized for op %s as only root principal can perform this operation";
  private static final String RANGER_AUTH_FAILED_ERROR =
      "Principal '%s' is not authorized for op '%s'";
  private static final String RANGER_UNSUPPORTED_OPERATION =
      "Operation %s is not supported by Ranger authorizer";

  private static final Set<PolarisAuthorizableOperation> AUTHORIZED_OPERATIONS =
      initAuthorizedOperations();

  private final RealmConfig realmConfig;
  private final RangerAuthorizer authorizer;
  private final String serviceName;

  public RangerPolarisAuthorizer(RangerPolarisAuthorizerConfig config, RealmConfig realmConfig) {
    LOG.info("Initializing RangerPolarisAuthorizer");

    Properties rangerProp = RangerUtils.loadProperties(config.configFileName().get());

    this.realmConfig = realmConfig;
    this.authorizer = new RangerEmbeddedAuthorizer(rangerProp);
    this.serviceName = rangerProp.getProperty(SERVICE_NAME_PROPERTY);

    try {
      authorizer.init();
    } catch (RangerAuthzException t) {
      LOG.error("Failed to initialize RangerPolarisAuthorizer", t);
      throw new RuntimeException(t);
    }

    LOG.info("RangerPolarisAuthorizer initialized successfully");
  }

  @Override
  public void resolveAuthorizationInputs(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
    throw new UnsupportedOperationException(
        "resolveAuthorizationInputs is not implemented yet for RangerPolarisAuthorizer");
  }

  @Override
  public @NonNull AuthorizationDecision authorize(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
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
    try {
      if (authzOp == PolarisAuthorizableOperation.ROTATE_CREDENTIALS) {
        boolean enforceCredentialRotationRequiredState =
            realmConfig.getConfig(
                FeatureConfiguration.ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING);

        if (enforceCredentialRotationRequiredState
            && !polarisPrincipal
                .getProperties()
                .containsKey(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE)) {
          // TODO: enable ranger audit from here to ensure that the request denied captured.
          throw new ForbiddenException(
              OPERATION_NOT_ALLOWED_FOR_USER_ERROR, polarisPrincipal.getName(), authzOp.name());
        }
      } else if (authzOp == PolarisAuthorizableOperation.RESET_CREDENTIALS) {
        boolean isRootPrincipal = getRootPrincipalName().equals(polarisPrincipal.getName());

        if (!isRootPrincipal) {
          // TODO: enable ranger audit from here to ensure that the request denied captured.
          throw new ForbiddenException(
              ROOT_PRINCIPLE_NEEDED_ERROR, polarisPrincipal.getName(), authzOp.name());
        }
      } else if (!AUTHORIZED_OPERATIONS.contains(authzOp)) {
        throw new ForbiddenException(RANGER_UNSUPPORTED_OPERATION, authzOp.name());
      } else if (!isAccessAuthorized(
          polarisPrincipal, activatedEntities, authzOp, targets, secondaries)) {
        throw new ForbiddenException(
            RANGER_AUTH_FAILED_ERROR, polarisPrincipal.getName(), authzOp.name());
      }
    } catch (RangerAuthzException excp) {
      LOG.error("Failed to authorize principal {} for op {}", polarisPrincipal, authzOp, excp);
      throw new IllegalStateException(excp);
    } catch (IllegalStateException ise) {
      LOG.error("Failed to authorize principal {} for op {}", polarisPrincipal, authzOp, ise);
      throw ise;
    }
  }

  private boolean isAccessAuthorized(
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull Set<PolarisBaseEntity> activatedEntities,
      @Nonnull PolarisAuthorizableOperation authzOp,
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries)
      throws RangerAuthzException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "isAuthorized: users={}, groups={}",
          polarisPrincipal.getName(),
          String.join(",", polarisPrincipal.getRoles()));

      LOG.debug(
          "isAuthorized: activatedEntities={}",
          activatedEntities.stream()
              .map(e -> RangerUtils.toResourceType(e.getType()) + ":" + e.getName())
              .collect(Collectors.joining(",")));

      LOG.debug("isAuthorized: authzOp={}", authzOp.name());

      LOG.debug(
          "isAuthorized: permissions={}",
          authzOp.getPrivilegesOnTarget().stream()
              .map(RangerUtils::toAccessType)
              .collect(Collectors.joining(",")));

      if (targets != null) {
        LOG.debug(
            "isAuthorized: targets={}",
            targets.stream().map(RangerUtils::toResourcePath).collect(Collectors.joining(",")));
      }

      if (secondaries != null) {
        LOG.debug(
            "isAuthorized: secondaries={}",
            secondaries.stream().map(RangerUtils::toResourcePath).collect(Collectors.joining(",")));
      }
    }

    return isAccessAuthorized(polarisPrincipal, authzOp, targets, secondaries);
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

    if (!authzOp.getPrivilegesOnTarget().isEmpty()) {
      Preconditions.checkState(
          isTargetSpecified,
          "No target provided to authorize %s for privileges %s",
          authzOp,
          authzOp.getPrivilegesOnTarget());

      for (PolarisResolvedPathWrapper target : targets) {
        accessInfos.add(RangerUtils.toAccessInfo(target, authzOp, authzOp.getPrivilegesOnTarget()));
      }
    } else if (isTargetSpecified) {
      LOG.warn(
          "No privileges specified for target authorization. Ignoring target {}, op: {}, user: {}",
          RangerUtils.toResourcePath(targets),
          authzOp.name(),
          principal.getName());
    }

    if (!authzOp.getPrivilegesOnSecondary().isEmpty()) {
      Preconditions.checkState(
          isSecondarySpecified,
          "No secondaries provided to authorize %s for privileges %s",
          authzOp,
          authzOp.getPrivilegesOnSecondary());

      for (PolarisResolvedPathWrapper secondary : secondaries) {
        accessInfos.add(
            RangerUtils.toAccessInfo(secondary, authzOp, authzOp.getPrivilegesOnSecondary()));
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

    if (!isAllowed && LOG.isDebugEnabled()) {
      for (int i = 0; i < accessInfos.size(); i++) {
        RangerAccessInfo accessInfo = accessInfos.get(i);
        RangerAuthzResult accessResult = authzResult.getAccesses().get(i);

        if (!RangerAuthzResult.AccessDecision.ALLOW.equals(accessResult.getDecision())) {
          LOG.debug(
              "User {} is not authorized for {} on {}",
              userInfo.getName(),
              authzOp,
              accessInfo.getResource());
        }
      }
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

      case CREATE_PRINCIPAL_ROLE:
      case DELETE_PRINCIPAL_ROLE:
      case UPDATE_PRINCIPAL_ROLE:
      case GET_PRINCIPAL_ROLE:
      case LIST_PRINCIPAL_ROLES:
      case ASSIGN_PRINCIPAL_ROLE:
      case REVOKE_PRINCIPAL_ROLE:
      case LIST_PRINCIPAL_ROLES_ASSIGNED:
      case LIST_ASSIGNEE_PRINCIPALS_FOR_PRINCIPAL_ROLE:
      case ADD_PRINCIPAL_GRANT_TO_PRINCIPAL_ROLE:
      case REVOKE_PRINCIPAL_GRANT_FROM_PRINCIPAL_ROLE:
      case LIST_GRANTS_ON_PRINCIPAL:
      case ADD_PRINCIPAL_ROLE_GRANT_TO_PRINCIPAL_ROLE:
      case REVOKE_PRINCIPAL_ROLE_GRANT_FROM_PRINCIPAL_ROLE:
      case LIST_GRANTS_ON_PRINCIPAL_ROLE:
      case ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE:
      case REVOKE_ROOT_GRANT_FROM_PRINCIPAL_ROLE:
      case LIST_GRANTS_ON_ROOT:
        return false;

      case CREATE_CATALOG_ROLE:
      case DELETE_CATALOG_ROLE:
      case UPDATE_CATALOG_ROLE:
      case GET_CATALOG_ROLE:
      case LIST_CATALOG_ROLES:
      case ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE:
      case REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE:
      case LIST_CATALOG_ROLES_FOR_PRINCIPAL_ROLE:
      case LIST_ASSIGNEE_PRINCIPAL_ROLES_FOR_CATALOG_ROLE:
      case LIST_GRANTS_FOR_CATALOG_ROLE:
      case ADD_CATALOG_ROLE_GRANT_TO_CATALOG_ROLE:
      case REVOKE_CATALOG_ROLE_GRANT_FROM_CATALOG_ROLE:
      case ADD_NAMESPACE_GRANT_TO_CATALOG_ROLE:
      case ADD_CATALOG_GRANT_TO_CATALOG_ROLE:
      case ADD_TABLE_GRANT_TO_CATALOG_ROLE:
      case ADD_VIEW_GRANT_TO_CATALOG_ROLE:
      case ADD_POLICY_GRANT_TO_CATALOG_ROLE:
      case REVOKE_NAMESPACE_GRANT_FROM_CATALOG_ROLE:
      case REVOKE_CATALOG_GRANT_FROM_CATALOG_ROLE:
      case REVOKE_TABLE_GRANT_FROM_CATALOG_ROLE:
      case REVOKE_VIEW_GRANT_FROM_CATALOG_ROLE:
      case REVOKE_POLICY_GRANT_FROM_CATALOG_ROLE:
      case LIST_GRANTS_ON_CATALOG_ROLE:
        return false;

      case LIST_GRANTS_ON_CATALOG:
      case LIST_GRANTS_ON_NAMESPACE:
      case LIST_GRANTS_ON_TABLE:
      case LIST_GRANTS_ON_VIEW:
        return false;

      default:
        LOG.error("{}: operation not recognized", op);

        return false;
    }
  }
}
