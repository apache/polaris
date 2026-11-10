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
package org.apache.polaris.service.types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

public class StorageMappingSerializationTest {

  private ObjectMapper mapper;

  @BeforeEach
  public void setUp() {
    mapper = JsonMapper.shared();
  }

  @Test
  public void testStorageMappingJsonFormat() {
    StorageMapping mapping =
        new StorageMapping(
            new StorageMappingTarget(
                StorageMappingTarget.TypeEnum.TABLE, List.of("finance", "ledger")),
            "finance",
            false);

    String json = mapper.writeValueAsString(mapping);

    assertThat(json)
        .isEqualTo(
            "{\"target\":{\"type\":\"table\",\"path\":[\"finance\",\"ledger\"]},"
                + "\"storage-configuration-name\":\"finance\","
                + "\"target-exists\":false}");
    assertThat(mapper.readValue(json, StorageMapping.class)).isEqualTo(mapping);
  }

  @Test
  public void testCreateAndRemoveRequestJsonFormat() {
    StorageMappingTarget target =
        new StorageMappingTarget(StorageMappingTarget.TypeEnum.NAMESPACE, List.of("eu", "gdpr"));
    CreateStorageMappingRequest create = new CreateStorageMappingRequest(target, "gdpr-vault");
    RemoveStorageMappingRequest remove = new RemoveStorageMappingRequest(target);

    String createJson = mapper.writeValueAsString(create);
    String removeJson = mapper.writeValueAsString(remove);

    assertThat(createJson)
        .isEqualTo(
            "{\"target\":{\"type\":\"namespace\",\"path\":[\"eu\",\"gdpr\"]},"
                + "\"storage-configuration-name\":\"gdpr-vault\"}");
    assertThat(removeJson)
        .isEqualTo("{\"target\":{\"type\":\"namespace\",\"path\":[\"eu\",\"gdpr\"]}}");
    assertThat(mapper.readValue(createJson, CreateStorageMappingRequest.class)).isEqualTo(create);
    assertThat(mapper.readValue(removeJson, RemoveStorageMappingRequest.class)).isEqualTo(remove);
  }

  @Test
  public void testListResponseJsonFormat() {
    StorageMapping dropped =
        new StorageMapping(
            new StorageMappingTarget(
                StorageMappingTarget.TypeEnum.TABLE, List.of("finance", "old")),
            "finance",
            false);

    ListStorageMappingsResponse empty = new ListStorageMappingsResponse(List.of());
    assertThat(
            mapper.readValue(mapper.writeValueAsString(empty), ListStorageMappingsResponse.class))
        .isEqualTo(empty);

    ListStorageMappingsResponse page = new ListStorageMappingsResponse("token-1", List.of(dropped));
    String json = mapper.writeValueAsString(page);

    assertThat(json)
        .isEqualTo(
            "{\"next-page-token\":\"token-1\",\"storage-mappings\":[{"
                + "\"target\":{\"type\":\"table\",\"path\":[\"finance\",\"old\"]},"
                + "\"storage-configuration-name\":\"finance\","
                + "\"target-exists\":false}]}");
    assertThat(mapper.readValue(json, ListStorageMappingsResponse.class)).isEqualTo(page);
  }

  @Test
  public void testListResponseWithoutPageToken() {
    ListStorageMappingsResponse response =
        mapper.readValue("{\"storage-mappings\":[]}", ListStorageMappingsResponse.class);

    assertThat(response.getNextPageToken()).isNull();
    assertThat(response.getStorageMappings()).isEmpty();
  }

  @Test
  public void testUnknownTargetTypeIsRejected() {
    assertThatThrownBy(
            () ->
                mapper.readValue(
                    "{\"type\":\"view\",\"path\":[\"finance\",\"v\"]}", StorageMappingTarget.class))
        .isNotNull();
  }

  @Test
  public void testMissingRequiredFieldIsRejected() {
    assertThatThrownBy(
            () ->
                mapper.readValue(
                    "{\"target\":{\"type\":\"table\",\"path\":[\"a\",\"b\"]}}",
                    CreateStorageMappingRequest.class))
        .isNotNull();
  }
}
