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
package org.apache.polaris.extension.metrics.reports;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MetricsReportsService}.
 *
 * <p>The read path currently returns 501 Not Implemented pending the durable extension (#4756).
 * These tests cover authorization, resolution error paths, and input validation.
 */
class MetricsReportsServiceTest {

  private static final String CATALOG = "test-catalog";
  private static final String NAMESPACE = "dbschema";
  private static final String TABLE = "events";

  private PolarisAuthorizer authorizer;
  private PolarisResolutionManifest manifest;
  private PolarisPrincipal principal;
  private ResolutionManifestFactory factory;
  private MetricsReportsService service;
  private RealmContext realmContext;
  private SecurityContext securityContext;

  @BeforeEach
  void setUp() {
    authorizer = mock(PolarisAuthorizer.class);
    principal = mock(PolarisPrincipal.class);

    PolarisResolvedPathWrapper tableWrapper = mock(PolarisResolvedPathWrapper.class);
    manifest = mock(PolarisResolutionManifest.class);
    factory = mock(ResolutionManifestFactory.class);
    realmContext = mock(RealmContext.class);
    securityContext = mock(SecurityContext.class);

    when(manifest.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    when(manifest.getResolvedPath(
            any(ResolvedPathKey.class), eq(PolarisEntitySubType.ANY_SUBTYPE), eq(true)))
        .thenReturn(tableWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    when(factory.createResolutionManifest(eq(principal), eq(CATALOG))).thenReturn(manifest);
    doNothing()
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            any(Set.class),
            any(PolarisAuthorizableOperation.class),
            any(PolarisResolvedPathWrapper.class),
            (PolarisResolvedPathWrapper) isNull());

    service = new MetricsReportsService(authorizer, principal, factory);
  }

  @Test
  void authorizedRequestReturnsNotImplemented() {
    Response response =
        service.listTableMetrics(
            CATALOG,
            NAMESPACE,
            TABLE,
            "scan",
            null,
            10,
            null,
            null,
            null,
            null,
            realmContext,
            securityContext);

    assertThat(response.getStatus()).isEqualTo(Response.Status.NOT_IMPLEMENTED.getStatusCode());
  }

  @Test
  void unauthorizedRequestThrowsForbiddenException() {
    doThrow(new ForbiddenException("denied"))
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            any(Set.class),
            eq(PolarisAuthorizableOperation.LIST_TABLE_METRICS),
            any(PolarisResolvedPathWrapper.class),
            (PolarisResolvedPathWrapper) isNull());

    assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    NAMESPACE,
                    TABLE,
                    "scan",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void tableNotFoundThrowsNotFoundException() {
    when(manifest.getResolvedPath(
            any(ResolvedPathKey.class), eq(PolarisEntitySubType.ANY_SUBTYPE), eq(true)))
        .thenReturn(null);

    assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    NAMESPACE,
                    TABLE,
                    "scan",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining(TABLE);
  }

  @Test
  void catalogNotFoundPropagatesNotFoundException() {
    when(manifest.resolveAll()).thenReturn(new ResolverStatus(PolarisEntityType.CATALOG, CATALOG));

    assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    NAMESPACE,
                    TABLE,
                    "scan",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining(CATALOG);
  }

  @Test
  void pathNotFoundPropagatesNotFoundException() {
    ResolverPath failedPath = new ResolverPath(List.of(NAMESPACE), PolarisEntityType.NAMESPACE);
    when(manifest.resolveAll()).thenReturn(new ResolverStatus(failedPath, 0));

    assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    NAMESPACE,
                    TABLE,
                    "scan",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(NotFoundException.class);
  }

  @Test
  void multiLevelNamespaceIsSplitCorrectly() {
    // JAX-RS decodes %1F -> U+001F before injection; the service must split correctly.
    // "dbschema" represents namespace ["db", "schema"].
    String encodedTwoLevel = "dbschema";
    when(factory.createResolutionManifest(eq(principal), eq(CATALOG))).thenReturn(manifest);

    Response response =
        service.listTableMetrics(
            CATALOG,
            encodedTwoLevel,
            TABLE,
            "scan",
            null,
            10,
            null,
            null,
            null,
            null,
            realmContext,
            securityContext);

    assertThat(response.getStatus()).isEqualTo(Response.Status.NOT_IMPLEMENTED.getStatusCode());
  }
}
