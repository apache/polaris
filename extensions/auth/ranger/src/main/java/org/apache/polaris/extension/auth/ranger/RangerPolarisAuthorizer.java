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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationPreConditions;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Authorizes operations based on policies defined in Apache Ranger. */
public class RangerPolarisAuthorizer implements PolarisAuthorizer {
  private static final Logger LOG = LoggerFactory.getLogger(RangerPolarisAuthorizer.class);

  public static final String SERVICE_TYPE = "polaris";

  private static final String RANGER_AUTH_FAILED_ERROR =
      "Principal '%s' is not authorized for op '%s'";

  private final RangerEmbeddedAuthorizer authorizer;
  private final String serviceName;
  private RealmContext realmContext;
  private String realmConextIdentifier;
  private final RealmConfig realmConfig;

  public RangerPolarisAuthorizer(
      RangerEmbeddedAuthorizer authorizer, String serviceName, RealmConfig realmConfig) {
    this.authorizer = authorizer;
    this.serviceName = serviceName;
    this.realmConfig = realmConfig;
  }

  public void setRealmContext(RealmContext aRealmContext) {
    this.realmContext = aRealmContext;
    this.realmConextIdentifier = aRealmContext.getRealmIdentifier();
  }

  /**
   * Resolves authorization inputs using {@code resolveAll()} for backward compatibility.
   *
   * <p>This scope is intentionally broad for now and will be narrowed in a future refactoring to
   * resolve only the selections required by Ranger authorization.
   */
  @Override
  public void resolveAuthorizationInputs(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
    authzState.getResolutionManifest().resolveAll();
  }

  @Override
  public @NonNull AuthorizationDecision authorize(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
    throw new UnsupportedOperationException("authorize is not implemented yet");
  }

  @Override
  public void authorizeOrThrow(
      @NonNull PolarisPrincipal polarisPrincipal,
      @NonNull Set<PolarisBaseEntity> activatedEntities,
      @NonNull PolarisAuthorizableOperation authzOp,
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
      @NonNull PolarisPrincipal polarisPrincipal,
      @NonNull Set<PolarisBaseEntity> activatedEntities,
      @NonNull PolarisAuthorizableOperation authzOp,
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries) {

    RangerPolarisOperationSemantics semantics =
        RangerPolarisOperationSemantics.forOperation(authzOp);

    if (semantics == null) {
      throw new ForbiddenException(
          RANGER_AUTH_FAILED_ERROR, polarisPrincipal.getName(), authzOp.name());
    }

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
      AuthorizationPreConditions.checkCredentialRotationRequired(
          polarisPrincipal, authzOp, realmConfig);

      if (!isAccessAuthorized(polarisPrincipal, authzOp, targets, secondaries)) {
        throw new ForbiddenException(
            RANGER_AUTH_FAILED_ERROR, polarisPrincipal.getName(), authzOp.name());
      }
    } catch (RangerAuthzException excp) {
      throw new IllegalStateException(
          "Failed to authorize principal " + polarisPrincipal + " for op {}" + authzOp, excp);
    }
  }

  private boolean isAccessAuthorized(
      @NonNull PolarisPrincipal principal,
      @NonNull PolarisAuthorizableOperation authzOp,
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
        accessInfos.add(
            RangerUtils.toAccessInfo(
                target, authzOp, semantics.targetPrivileges(), realmConextIdentifier));
      }
    } else if (isTargetSpecified) {
      LOG.warn(
          "No privileges specified for target authorization. Ignoring target {}, op: {}, user: {}",
          RangerUtils.toResourcePath(targets, realmConextIdentifier),
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
            RangerUtils.toAccessInfo(
                secondary, authzOp, semantics.secondaryPrivileges(), realmConextIdentifier));
      }
    } else if (isSecondarySpecified) {
      LOG.warn(
          "No privileges specified for secondary authorization. Ignoring secondaries {}, op: {}, user: {}",
          RangerUtils.toResourcePath(secondaries, realmConextIdentifier),
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
}
