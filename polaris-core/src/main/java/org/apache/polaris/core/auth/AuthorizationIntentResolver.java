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
package org.apache.polaris.core.auth;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.polaris.core.auth.RbacOperationSemantics.ResolvedPathRooting;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.jspecify.annotations.Nullable;

/**
 * Resolves an {@link AuthorizationIntent}'s securable references into the {@link
 * PolarisResolvedPathWrapper} targets and secondaries required by the resolved-path authorization
 * APIs.
 *
 * <p>This lives in the {@code org.apache.polaris.core.auth} package so it can consult the
 * package-private {@link RbacOperationSemantics} to decide whether a given operation roots its
 * target path at the root container. It is shared by {@link PolarisAuthorizerImpl} and by pluggable
 * authorizer extensions (such as the Apache Ranger authorizer) so that every authorizer resolves
 * the securables of the decision-native {@code authorize(state, request)} path identically.
 */
public final class AuthorizationIntentResolver {

  private AuthorizationIntentResolver() {}

  /**
   * The resolved target and secondary paths for a single {@link AuthorizationIntent}. Either list
   * may be {@code null} when the intent has no target/secondary to resolve.
   */
  public record ResolvedIntent(
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries) {}

  /**
   * Resolves the securable references carried by {@code intent} against {@code resolutionManifest}
   * into the target and secondary resolved paths expected by the resolved-path authorization APIs.
   *
   * @param resolutionManifest the populated resolution manifest for the current authorization flow
   * @param intent the authorization intent whose securables should be resolved
   * @return the resolved targets and secondaries for the intent
   */
  public static ResolvedIntent resolve(
      PolarisResolutionManifest resolutionManifest, AuthorizationIntent intent) {
    RbacOperationSemantics semantics = RbacOperationSemantics.forOperation(intent.getOperation());
    boolean prependRootContainer = semantics.rooting() == ResolvedPathRooting.ROOT;

    List<PolarisResolvedPathWrapper> resolvedTargets;
    List<PolarisResolvedPathWrapper> resolvedSecondaries;
    if (intent instanceof TargetlessAuthorizationIntent) {
      resolvedTargets =
          prependRootContainer
              ? List.of(resolutionManifest.getResolvedRootContainerEntityAsPath())
              : null;
      resolvedSecondaries = null;
    } else if (intent instanceof SingleTargetAuthorizationIntent singleTargetIntent) {
      resolvedTargets =
          List.of(
              getResolvedSecurable(
                  resolutionManifest, singleTargetIntent.target(), prependRootContainer));
      resolvedSecondaries = null;
    } else if (intent instanceof RenameAuthorizationIntent renameIntent) {
      resolvedTargets =
          List.of(
              getResolvedSecurable(resolutionManifest, renameIntent.from(), prependRootContainer));
      resolvedSecondaries =
          List.of(
              getResolvedSecurable(resolutionManifest, renameIntent.to(), prependRootContainer));
    } else if (intent instanceof PolicyAttachmentAuthorizationIntent policyAttachmentIntent) {
      resolvedTargets =
          List.of(
              getResolvedSecurable(
                  resolutionManifest, policyAttachmentIntent.policy(), prependRootContainer));
      resolvedSecondaries =
          List.of(
              getResolvedSecurable(
                  resolutionManifest, policyAttachmentIntent.attachedTo(), prependRootContainer));
    } else if (intent instanceof RoleAssignmentAuthorizationIntent roleAssignmentIntent) {
      resolvedTargets =
          List.of(
              getResolvedSecurable(
                  resolutionManifest, roleAssignmentIntent.role(), prependRootContainer));
      resolvedSecondaries =
          List.of(
              getResolvedSecurable(
                  resolutionManifest, roleAssignmentIntent.assignee(), prependRootContainer));
    } else if (intent instanceof PrivilegeGrantAuthorizationIntent privilegeGrantIntent) {
      resolvedTargets =
          List.of(
              getResolvedSecurable(
                  resolutionManifest, privilegeGrantIntent.grantTarget(), prependRootContainer));
      resolvedSecondaries =
          List.of(
              getResolvedSecurable(
                  resolutionManifest, privilegeGrantIntent.grantee(), prependRootContainer));
    } else if (intent instanceof RootPrivilegeGrantAuthorizationIntent rootPrivilegeGrantIntent) {
      resolvedTargets = List.of(resolutionManifest.getResolvedRootContainerEntityAsPath());
      resolvedSecondaries =
          List.of(
              getResolvedSecurable(
                  resolutionManifest, rootPrivilegeGrantIntent.grantee(), prependRootContainer));
    } else {
      throw new IllegalStateException("Unsupported authorization intent: " + intent.getClass());
    }
    return new ResolvedIntent(resolvedTargets, resolvedSecondaries);
  }

  private static PolarisResolvedPathWrapper getResolvedSecurable(
      PolarisResolutionManifest resolutionManifest,
      PolarisSecurable securable,
      boolean prependRootContainer) {
    PolarisResolvedPathWrapper resolvedSecurable =
        securable.getLeaf().entityType().isTopLevel()
            ? resolutionManifest.getResolvedTopLevelEntity(
                securable.getLeaf().name(), securable.getLeaf().entityType())
            : resolutionManifest.getResolvedPath(
                ResolvedPathKey.of(
                    getPathNamesWithinCatalog(securable), securable.getLeaf().entityType()),
                prependRootContainer);
    Preconditions.checkState(
        resolvedSecurable != null,
        "Resolved path for securable is null for entityType=%s leaf=%s parents=%s",
        securable.getLeaf().entityType(),
        securable.getLeaf(),
        securable.getParents());
    return resolvedSecurable;
  }

  private static List<String> getPathNamesWithinCatalog(PolarisSecurable securable) {
    // Resolver path keys are scoped within the reference catalog, so the explicit catalog
    // path segment is omitted from the PolarisSecurable path before lookup.
    return securable.getPathSegments().stream()
        .filter(segment -> segment.entityType() != PolarisEntityType.CATALOG)
        .map(PathSegment::name)
        .toList();
  }
}
