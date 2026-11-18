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
package org.apache.polaris.extension.auth.opa;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.util.List;
import org.apache.polaris.extension.auth.opa.model.ImmutableActor;
import org.apache.polaris.extension.auth.opa.model.ImmutableContext;
import org.apache.polaris.extension.auth.opa.model.ImmutableOpaBatchAuthorizationInput;
import org.apache.polaris.extension.auth.opa.model.ImmutableOpaBatchQuery;
import org.apache.polaris.extension.auth.opa.model.ImmutableOpaBatchRequest;
import org.apache.polaris.extension.auth.opa.model.ImmutableResource;
import org.apache.polaris.extension.auth.opa.model.ImmutableResourceEntity;
import org.apache.polaris.extension.auth.opa.model.OpaBatchRequest;
import org.junit.jupiter.api.Test;

class OpaBatchModelTest {

  private static final ObjectMapper MAPPER = JsonMapper.builder().build();

  @Test
  void serializesActorAndContextOnceWithOneEntryPerQuestion() throws Exception {
    OpaBatchRequest request =
        ImmutableOpaBatchRequest.builder()
            .input(
                ImmutableOpaBatchAuthorizationInput.builder()
                    .actor(
                        ImmutableActor.builder()
                            .principal("alice")
                            .addRoles("analyst")
                            .putAttribute("polaris.user.team", "finance")
                            .build())
                    .context(
                        ImmutableContext.builder().requestId("req-1").realm("test-realm").build())
                    .addRequests(query("0", "table1"))
                    .addRequests(query("1", "table2"))
                    .build())
            .build();

    JsonNode input = MAPPER.readTree(MAPPER.writeValueAsString(request)).path("input");

    assertThat(input.path("actor").path("principal").asText()).isEqualTo("alice");
    assertThat(input.path("actor").path("attributes").path("polaris.user.team").asText())
        .isEqualTo("finance");
    assertThat(input.path("context").path("request_id").asText()).isEqualTo("req-1");
    assertThat(input.path("requests")).hasSize(2);
    assertThat(input.path("requests").get(0).path("id").asText()).isEqualTo("0");
    assertThat(input.path("requests").get(0).path("action").asText()).isEqualTo("LIST_TABLE");
    assertThat(input.path("requests").get(1).path("id").asText()).isEqualTo("1");
    assertThat(
            input
                .path("requests")
                .get(1)
                .path("resource")
                .path("targets")
                .get(0)
                .path("name")
                .asText())
        .isEqualTo("table2");
  }

  private static ImmutableOpaBatchQuery query(String id, String tableName) {
    return ImmutableOpaBatchQuery.builder()
        .id(id)
        .action("LIST_TABLE")
        .resource(
            ImmutableResource.builder()
                .targets(
                    List.of(
                        ImmutableResourceEntity.builder()
                            .type("TABLE_LIKE")
                            .name(tableName)
                            .build()))
                .secondaries(List.of())
                .build())
        .build();
  }
}
