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
package org.apache.polaris.core.persistence;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import java.time.Clock;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.resolver.Resolvable;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.persistence.transactional.TransactionalMetaStoreManagerImpl;
import org.apache.polaris.core.persistence.transactional.TreeMapMetaStore;
import org.apache.polaris.core.persistence.transactional.TreeMapTransactionalPersistenceImpl;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class ResolverTest extends BaseResolverTest {

  private final Clock clock = Clock.systemUTC();
  private PolarisCallContext callCtx;
  private PolarisTestMetaStoreManager tm;
  private TransactionalMetaStoreManagerImpl metaStoreManager;

  @Override
  protected PolarisCallContext callCtx() {
    if (callCtx == null) {
      TreeMapMetaStore store = new TreeMapMetaStore(diagServices);
      TreeMapTransactionalPersistenceImpl metaStore =
          new TreeMapTransactionalPersistenceImpl(
              diagServices, store, Mockito.mock(), RANDOM_SECRETS);
      callCtx = new PolarisCallContext(() -> "testRealm", metaStore);
    }
    return callCtx;
  }

  @Override
  protected PolarisMetaStoreManager metaStoreManager() {
    if (metaStoreManager == null) {
      metaStoreManager = new TransactionalMetaStoreManagerImpl(clock, diagServices);
    }
    return metaStoreManager;
  }

  @Override
  protected PolarisTestMetaStoreManager tm() {
    if (tm == null) {
      // bootstrap the meta store with our test schema
      tm = new PolarisTestMetaStoreManager(metaStoreManager(), callCtx());
    }
    return tm;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testMissingPathsRetainAncestorsOnlyForAuthorization(boolean forAuthorization) {
    Resolver resolver = authorizationTestResolver("test");
    ResolverPath missingModel =
        new ResolverPath(List.of("N1", "missing-model"), PolarisEntityType.SEMANTIC_MODEL);
    resolver.addPath(missingModel);
    resolver.addPath(
        new ResolverPath(List.of("N5", "missing-ns", "model"), PolarisEntityType.SEMANTIC_MODEL));
    resolver.addPath(new ResolverPath(List.of("R1"), PolarisEntityType.CATALOG_ROLE));

    ResolverStatus status =
        forAuthorization ? resolver.resolveAllForAuthorization() : resolver.resolveAll();

    Assertions.assertThat(status.getStatus())
        .isEqualTo(ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED);
    Assertions.assertThat(status.getFailedToResolvePath()).isEqualTo(missingModel);
    if (forAuthorization) {
      var paths = resolver.getResolvedPaths();
      Assertions.assertThat(paths).hasSize(3);
      Assertions.assertThat(paths.get(0))
          .extracting(e -> e.getEntity().getName())
          .containsExactly("N1");
      Assertions.assertThat(paths.get(1))
          .extracting(e -> e.getEntity().getName())
          .containsExactly("N5");
      Assertions.assertThat(paths.get(2))
          .extracting(e -> e.getEntity().getName())
          .containsExactly("R1");
      Assertions.assertThat(resolver.getResolvedCatalogRoles()).isNotEmpty();
    } else {
      Assertions.assertThatThrownBy(resolver::getResolvedPaths)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("resolver_must_be_successful");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testMissingCatalogRetainsTopLevelEntitiesOnlyForAuthorization(
      boolean forAuthorization) {
    Resolver resolver = authorizationTestResolver("missing-catalog");
    resolver.addEntityByName(PolarisEntityType.PRINCIPAL, "P2");
    resolver.addPath(new ResolverPath(List.of("N1", "model"), PolarisEntityType.SEMANTIC_MODEL));

    ResolverStatus status =
        forAuthorization ? resolver.resolveAllForAuthorization() : resolver.resolveAll();

    Assertions.assertThat(status.getStatus())
        .isEqualTo(ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED);
    if (forAuthorization) {
      Assertions.assertThat(resolver.getResolvedReferenceCatalog()).isNull();
      Assertions.assertThat(resolver.getResolvedCallerPrincipalRoles()).isNotEmpty();
      Assertions.assertThat(resolver.getResolvedEntity(PolarisEntityType.PRINCIPAL, "P2"))
          .isNotNull();
    } else {
      Assertions.assertThatThrownBy(resolver::getResolvedReferenceCatalog)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("resolver_must_be_successful");
    }
  }

  private Resolver authorizationTestResolver(String catalogName) {
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "P1",
            Map.of(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, PrincipalEntity.of(P1)),
            Set.of("PR1"));
    return new Resolver(diagServices, callCtx(), metaStoreManager(), principal, null, catalogName);
  }

  @Test
  public void testResolveExternalCallerPrincipalIsSynthetic() {
    Resolver resolver =
        new Resolver(
            diagServices,
            callCtx(),
            metaStoreManager(),
            PolarisPrincipal.of(
                "ext-user",
                ImmutableAttributeMap.builder()
                    .put(PolarisPrincipalAttributes.EXTERNAL_PRINCIPAL_ATTRIBUTE_KEY, true)
                    .build(),
                Set.of("ext-role1", "ext-role2")),
            null,
            null);

    // The external principal and its roles do not exist in the metastore, yet resolution succeeds.
    ResolverStatus status = resolver.resolveAll();
    Assertions.assertThat(status.getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);

    ResolvedPolarisEntity principal = resolver.getResolvedCallerPrincipal();
    Assertions.assertThat(principal.getEntity().getName()).isEqualTo("ext-user");
    Assertions.assertThat(principal.getEntity().getType()).isEqualTo(PolarisEntityType.PRINCIPAL);
    Assertions.assertThat(principal.getGrantRecordsAsGrantee()).isEmpty();

    Assertions.assertThat(resolver.getResolvedCallerPrincipalRoles())
        .extracting(r -> r.getEntity().getName())
        .containsExactlyInAnyOrder("ext-role1", "ext-role2");
    Assertions.assertThat(resolver.getResolvedCallerPrincipalRoles())
        .allSatisfy(r -> Assertions.assertThat(r.getGrantRecordsAsGrantee()).isEmpty());
  }

  @Test
  public void testExternalCallerPrincipalNameDoesNotShadowStoredPrincipal() {
    // P1 is a stored principal created in setupTest(); use the same name for the external caller.
    Resolver resolver =
        new Resolver(
            diagServices,
            callCtx(),
            metaStoreManager(),
            PolarisPrincipal.of(
                "P1",
                ImmutableAttributeMap.builder()
                    .put(PolarisPrincipalAttributes.EXTERNAL_PRINCIPAL_ATTRIBUTE_KEY, true)
                    .build(),
                Set.of()),
            null,
            null);
    resolver.addEntityByName(PolarisEntityType.PRINCIPAL, "P1");

    ResolverStatus status = resolver.resolveAll();
    Assertions.assertThat(status.getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);

    // Must return the real stored P1, not the synthetic sentinel with id -1.
    ResolvedPolarisEntity resolved = resolver.getResolvedEntity(PolarisEntityType.PRINCIPAL, "P1");
    Assertions.assertThat(resolved).isNotNull();
    Assertions.assertThat(resolved.getEntity().getId()).isEqualTo(P1.getId());
    Assertions.assertThat(resolved.getEntity().getId()).isPositive();
  }

  @Test
  public void testExternalCallerRoleNameDoesNotShadowStoredPrincipalRole() {
    // PR1 is a stored principal role created in setupTest(); use the same name as an external role.
    Resolver resolver =
        new Resolver(
            diagServices,
            callCtx(),
            metaStoreManager(),
            PolarisPrincipal.of(
                "ext-user",
                ImmutableAttributeMap.builder()
                    .put(PolarisPrincipalAttributes.EXTERNAL_PRINCIPAL_ATTRIBUTE_KEY, true)
                    .build(),
                Set.of("PR1")),
            null,
            null);
    resolver.addOptionalEntityByName(PolarisEntityType.PRINCIPAL_ROLE, "PR1");

    ResolverStatus status = resolver.resolveAll();
    Assertions.assertThat(status.getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);

    // Must return the real stored PR1, not the synthetic sentinel with a negative id.
    ResolvedPolarisEntity resolved =
        resolver.getResolvedEntity(PolarisEntityType.PRINCIPAL_ROLE, "PR1");
    Assertions.assertThat(resolved).isNotNull();
    Assertions.assertThat(resolved.getEntity().getId()).isPositive();
  }

  @Test
  public void testResolveSelectionsSkipsCallerPrincipalForReferenceCatalog() {
    Resolver resolver =
        new Resolver(
            diagServices,
            callCtx(),
            metaStoreManager(),
            PolarisPrincipal.of("missing", AttributeMap.EMPTY, Set.of()),
            null,
            "test");
    ResolverStatus status = resolver.resolveSelections(Set.of(Resolvable.REFERENCE_CATALOG));
    Assertions.assertThat(status.getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);
  }

  @Test
  public void testResolveSelectionsSkipsCallerPrincipalForRequestedPaths() {
    Resolver pathResolver =
        new Resolver(
            diagServices,
            callCtx(),
            metaStoreManager(),
            PolarisPrincipal.of("missing", AttributeMap.EMPTY, Set.of()),
            null,
            "test");
    pathResolver.addPath(new ResolverPath(List.of("N1"), PolarisEntityType.NAMESPACE));
    ResolverStatus pathStatus = pathResolver.resolveSelections(Set.of(Resolvable.REQUESTED_PATHS));
    Assertions.assertThat(pathStatus.getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);
  }

  @Test
  public void testResolveSelectionsSkipsCallerPrincipalForRequestedTopLevelEntities() {
    Resolver entityResolver =
        new Resolver(
            diagServices,
            callCtx(),
            metaStoreManager(),
            PolarisPrincipal.of("missing", AttributeMap.EMPTY, Set.of()),
            null,
            null);
    entityResolver.addEntityByName(PolarisEntityType.PRINCIPAL, "P1");
    ResolverStatus entityStatus =
        entityResolver.resolveSelections(Set.of(Resolvable.REQUESTED_TOP_LEVEL_ENTITIES));
    Assertions.assertThat(entityStatus.getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);
  }

  @Test
  public void testResolveSelectionsRequiresCallerPrincipalForCallerCatalogRoles() {
    Resolver resolver =
        new Resolver(
            diagServices,
            callCtx(),
            metaStoreManager(),
            PolarisPrincipal.of(
                "missing",
                ImmutableAttributeMap.builder()
                    .put(
                        PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY,
                        new PrincipalEntity.Builder().setName("missing").build())
                    .build(),
                Set.of()),
            null,
            "test");
    ResolverStatus status = resolver.resolveSelections(Set.of(Resolvable.CALLER_CATALOG_ROLES));
    Assertions.assertThat(status.getStatus())
        .isEqualTo(ResolverStatus.StatusEnum.CALLER_PRINCIPAL_DOES_NOT_EXIST);
  }

  @Test
  public void
      testResolveSelectionsRequestedTopLevelEntitiesWithCatalogRoleResolvesReferenceCatalog() {
    Resolver resolver =
        new Resolver(
            diagServices,
            callCtx(),
            metaStoreManager(),
            PolarisPrincipal.of("missing", AttributeMap.EMPTY, Set.of()),
            null,
            "test");
    resolver.addOptionalEntityByName(PolarisEntityType.CATALOG_ROLE, "role1");
    ResolverStatus status =
        resolver.resolveSelections(Set.of(Resolvable.REQUESTED_TOP_LEVEL_ENTITIES));
    Assertions.assertThat(status.getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);
    ResolvedPolarisEntity resolvedCatalog = resolver.getResolvedReferenceCatalog();
    Assertions.assertThat(resolvedCatalog).isNotNull();
    Assertions.assertThat(resolvedCatalog.getEntity().getName()).isEqualTo("test");
  }

  @Test
  public void testResolveSelectionsRequiresCallerPrincipalForCallerPrincipal() {
    Resolver resolver =
        new Resolver(
            diagServices,
            callCtx(),
            metaStoreManager(),
            PolarisPrincipal.of(
                "missing",
                ImmutableAttributeMap.builder()
                    .put(
                        PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY,
                        new PrincipalEntity.Builder().setName("missing").build())
                    .build(),
                Set.of()),
            null,
            "test");
    ResolverStatus status = resolver.resolveSelections(Set.of(Resolvable.CALLER_PRINCIPAL));
    Assertions.assertThat(status.getStatus())
        .isEqualTo(ResolverStatus.StatusEnum.CALLER_PRINCIPAL_DOES_NOT_EXIST);
  }

  @Test
  public void testResolveSelectionsRequiresCallerPrincipalForCallerPrincipalRoles() {
    Resolver resolver =
        new Resolver(
            diagServices,
            callCtx(),
            metaStoreManager(),
            PolarisPrincipal.of(
                "missing",
                ImmutableAttributeMap.builder()
                    .put(
                        PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY,
                        new PrincipalEntity.Builder().setName("missing").build())
                    .build(),
                Set.of()),
            null,
            "test");
    ResolverStatus status = resolver.resolveSelections(Set.of(Resolvable.CALLER_PRINCIPAL_ROLES));
    Assertions.assertThat(status.getStatus())
        .isEqualTo(ResolverStatus.StatusEnum.CALLER_PRINCIPAL_DOES_NOT_EXIST);
  }

  @Test
  public void testResolveSelectionsThrowsOnGetResolvedCallerPrincipal() {
    Resolver resolver =
        new Resolver(
            diagServices,
            callCtx(),
            metaStoreManager(),
            PolarisPrincipal.of("missing", AttributeMap.EMPTY, Set.of()),
            null,
            "test");
    ResolverStatus status = resolver.resolveSelections(Set.of(Resolvable.REFERENCE_CATALOG));
    Assertions.assertThat(status.getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);
    Assertions.assertThatThrownBy(resolver::getResolvedCallerPrincipal)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("caller_principal_not_resolved");
  }
}
