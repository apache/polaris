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
package org.apache.polaris.service.catalog.semanticmodel;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.semanticmodel.types.UpdateSemanticModelRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link SemanticModelCatalogHandler} enforces authorization: the five operations are
 * gated behind {@code CATALOG_MANAGE_CONTENT} (see {@code RbacOperationSemantics}), so the
 * unprivileged {@code test-principal} that {@link TestServices} bootstraps is rejected with {@link
 * ForbiddenException}. This guards against silent regression of the gate. The allow path (an
 * authorized principal succeeds) is covered end-to-end by {@link
 * SemanticModelCatalogHandlerCrudTest} via the pass-through authorizer; the privilege-grant
 * plumbing needed to build an authorized principal is not reachable through the {@link
 * TestServices} surface.
 */
class SemanticModelCatalogHandlerAuthzTest extends AbstractSemanticModelCatalogHandlerTest {

  @BeforeEach
  void seedModel() {
    // Seed a model with a pass-through authorizer so load/update/drop reach the authz check
    // (rather than failing with "not found") when exercised by the unprivileged handler.
    passthroughHandler().createSemanticModel(NS, createRequest("m1", modelJson("ns1.t1")));
  }

  @Test
  void createDeniedWithoutManageContent() {
    assertThatThrownBy(
            () ->
                enforcingHandler()
                    .createSemanticModel(NS, createRequest("m2", modelJson("ns1.t1"))))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void listDeniedWithoutManageContent() {
    assertThatThrownBy(() -> enforcingHandler().listSemanticModels(NS, null, null))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void loadDeniedWithoutManageContent() {
    assertThatThrownBy(() -> enforcingHandler().loadSemanticModel(identifier("m1")))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void updateDeniedWithoutManageContent() {
    assertThatThrownBy(
            () ->
                enforcingHandler()
                    .updateSemanticModel(
                        identifier("m1"),
                        UpdateSemanticModelRequest.builder()
                            .setDocument(doc(modelJson("ns1.t1")))
                            .setEntityVersion("1")
                            .build()))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void dropDeniedWithoutManageContent() {
    assertThatThrownBy(() -> enforcingHandler().dropSemanticModel(identifier("m1")))
        .isInstanceOf(ForbiddenException.class);
  }

  /** Handler that actually enforces authorization for the unprivileged {@code test-principal}. */
  private SemanticModelCatalogHandler enforcingHandler() {
    return handler(new PolarisAuthorizerImpl(services.realmConfig()));
  }
}
