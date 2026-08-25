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
package org.apache.polaris.service.catalog.policy;

import static org.apache.polaris.service.catalog.common.ExceptionUtils.noSuchNamespaceException;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.notFoundExceptionForTableLikeEntity;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public class PolicyCatalogUtils {

  /**
   * The single point at which a client-supplied policy type name is turned into a {@link
   * PolicyType}. Both the lookup and the list of names quoted back on rejection live here, so the
   * message cannot advertise a set that differs from the one actually accepted.
   *
   * <p>{@link PolicyType#fromName(String)} resolves against the predefined types today, but its
   * contract allows it to resolve registered custom types later. When that happens, only this
   * method needs to learn about them.
   *
   * @param policyType a non-null, non-empty policy type name
   * @return the resolved policy type
   * @throws BadRequestException if the value does not name a known policy type
   */
  private static PolicyType lookUpPolicyType(String policyType) {
    PolicyType resolved = PolicyType.fromName(policyType);
    if (resolved == null) {
      throw new BadRequestException(
          "Unknown policy type: '%s'. Valid values are: %s",
          policyType,
          Arrays.stream(PredefinedPolicyTypes.values())
              .map(PolicyType::getName)
              .collect(Collectors.joining(", ")));
    }
    return resolved;
  }

  /**
   * Resolves the optional {@code policyType} query parameter of the policy listing APIs into a
   * {@link PolicyType} filter.
   *
   * <p>The parameter is declared with {@code allowEmptyValue: true}, so both {@code null} and the
   * empty string mean "do not filter". Any other value must name a known policy type: an unknown
   * name is rejected rather than silently treated as "no filter", which would return policies of
   * every type and make a typo look like a successful, but wrong, filtered listing.
   *
   * @param policyType the raw {@code policyType} query parameter, may be {@code null} or empty
   * @return the resolved policy type, or {@code null} if no filter was requested
   * @throws BadRequestException if a non-empty value does not name a known policy type
   */
  public static @Nullable PolicyType resolvePolicyTypeFilter(@Nullable String policyType) {
    if (policyType == null || policyType.isEmpty()) {
      return null;
    }
    return lookUpPolicyType(policyType);
  }

  /**
   * Resolves a required policy type, as supplied in a create-policy request body.
   *
   * <p>Unlike {@link #resolvePolicyTypeFilter(String)}, no value here means "unspecified" and is
   * rejected: a policy cannot be created without a type.
   *
   * @param policyType the requested policy type name
   * @return the resolved policy type, never {@code null}
   * @throws BadRequestException if the value is absent, empty, or does not name a known policy type
   */
  public static PolicyType resolveRequiredPolicyType(@Nullable String policyType) {
    if (policyType == null || policyType.isEmpty()) {
      throw new BadRequestException("Policy type is required");
    }
    return lookUpPolicyType(policyType);
  }

  public static PolarisResolvedPathWrapper getResolvedPathWrapper(
      @NonNull PolarisResolutionManifestCatalogView resolutionManifest,
      @NonNull PolicyAttachmentTarget target) {
    return switch (target.getType()) {
      // get the current catalog entity, since policy cannot apply across catalog at this moment
      case CATALOG -> resolutionManifest.getResolvedReferenceCatalogEntity();
      case NAMESPACE -> {
        var namespace = Namespace.of(target.getPath().toArray(new String[0]));
        var resolvedTargetEntity =
            resolutionManifest.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
        if (resolvedTargetEntity == null) {
          throw noSuchNamespaceException(namespace);
        }
        yield resolvedTargetEntity;
      }
      case TABLE_LIKE -> {
        var tableIdentifier = TableIdentifier.of(target.getPath().toArray(new String[0]));
        // only Iceberg tables are supported
        var resolvedTableEntity =
            resolutionManifest.getResolvedPath(
                ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.ICEBERG_TABLE);
        if (resolvedTableEntity == null) {
          throw notFoundExceptionForTableLikeEntity(
              tableIdentifier, PolarisEntitySubType.ICEBERG_TABLE);
        }
        yield resolvedTableEntity;
      }
      default -> throw new IllegalArgumentException("Unsupported target type: " + target.getType());
    };
  }
}
