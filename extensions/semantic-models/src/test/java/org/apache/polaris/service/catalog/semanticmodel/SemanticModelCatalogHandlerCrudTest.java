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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.semantic.exceptions.NoSuchSemanticModelException;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.semanticmodel.types.ListSemanticModelsResponse;
import org.apache.polaris.service.catalog.semanticmodel.types.LoadSemanticModelResponse;
import org.apache.polaris.service.catalog.semanticmodel.types.UpdateSemanticModelRequest;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link SemanticModelCatalogHandler} end-to-end against the real resolution manifest via
 * {@link TestServices}. Unlike {@link SemanticModelCatalogTest} (pure Mockito), this drives the
 * handler that builds a strict {@code PolarisResolutionManifest} — the only way to catch the
 * passthrough-registration failures that broke create/update. Authorization is intentionally a
 * pass-through mock here; privilege enforcement is covered by {@link
 * SemanticModelCatalogHandlerAuthzTest}.
 */
class SemanticModelCatalogHandlerCrudTest extends AbstractSemanticModelCatalogHandlerTest {

  @Test
  void createThenLoadRoundTrips() {
    LoadSemanticModelResponse created =
        passthroughHandler().createSemanticModel(NS, createRequest("m1", modelJson("ns1.t1")));

    assertThat(created.getDocument().getSemanticModel()).isEqualTo(modelJson("ns1.t1"));

    LoadSemanticModelResponse loaded = passthroughHandler().loadSemanticModel(identifier("m1"));
    assertThat(loaded.getDocument().getSemanticModel()).isEqualTo(modelJson("ns1.t1"));
    assertThat(loaded.getEntityVersion()).isEqualTo(created.getEntityVersion());
  }

  @Test
  void fullLifecycleCreateListUpdateDrop() {
    passthroughHandler().createSemanticModel(NS, createRequest("m1", modelJson("ns1.t1")));

    ListSemanticModelsResponse listed = passthroughHandler().listSemanticModels(NS, null, null);
    assertThat(listed.getIdentifiers()).anySatisfy(id -> assertThat(id.getName()).isEqualTo("m1"));

    LoadSemanticModelResponse current = passthroughHandler().loadSemanticModel(identifier("m1"));
    String updatedModel = modelJson("ns1.t1").replace("\"d\"", "\"d_renamed\"");
    LoadSemanticModelResponse updated =
        passthroughHandler()
            .updateSemanticModel(
                identifier("m1"),
                UpdateSemanticModelRequest.builder()
                    .setDocument(doc(updatedModel))
                    .setEntityVersion(current.getEntityVersion())
                    .build());
    assertThat(updated.getDocument().getSemanticModel()).isEqualTo(updatedModel);

    passthroughHandler().dropSemanticModel(identifier("m1"));
    assertThatThrownBy(() -> passthroughHandler().loadSemanticModel(identifier("m1")))
        .isInstanceOf(NoSuchSemanticModelException.class);
  }

  @Test
  void createRejectsUnknownSource() {
    assertThatThrownBy(
            () ->
                passthroughHandler()
                    .createSemanticModel(NS, createRequest("m2", modelJson("ns1.missing"))))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("ns1.missing");
  }
}
