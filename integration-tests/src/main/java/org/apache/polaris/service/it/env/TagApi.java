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
package org.apache.polaris.service.it.env;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.LoadTagResponse;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.assertj.core.api.Assertions;

public class TagApi extends PolarisRestApi {
  TagApi(Client client, PolarisApiEndpoints endpoints, String authToken, URI uri) {
    super(client, endpoints, authToken, uri);
  }

  public void purge(String catalog) {
    // Plain drops: detach-all answers 501 until assignments land (revisit with the
    // assignment PR so purge stays effective once assignments exist).
    listTags(catalog).forEach(t -> dropTag(catalog, t.getName()));
  }

  /**
   * Cleanup-only variant of {@link #purge}. Exactly one status means "no tag surface here, nothing
   * to purge": 406, the disabled feature flag's response. Anything else non-OK, including 400, is a
   * regression and fails with the response body, so shared cleanup stays a usable test oracle.
   */
  public void purgeIfAvailable(String catalog) {
    List<TagIdentifier> tags;
    try (Response res = request("polaris/v1/{cat}/tags", Map.of("cat", catalog)).get()) {
      if (skipPurgeForStatus(res.getStatus(), () -> res.readEntity(String.class))) {
        return;
      }
      tags = res.readEntity(ListTagsResponse.class).getIdentifiers().stream().toList();
    }
    tags.forEach(t -> dropTag(catalog, t.getName()));
  }

  /**
   * Decides the cleanup posture for a tag-listing status: 406 (feature disabled) skips quietly; 200
   * proceeds; any other status fails with the response body.
   */
  static boolean skipPurgeForStatus(int status, Supplier<String> body) {
    if (status == Response.Status.NOT_ACCEPTABLE.getStatusCode()) {
      return true;
    }
    if (status != Response.Status.OK.getStatusCode()) {
      Assertions.fail(
          "Unexpected status %d listing tags for cleanup: %s".formatted(status, body.get()));
    }
    return false;
  }

  public List<TagIdentifier> listTags(String catalog) {
    try (Response res = request("polaris/v1/{cat}/tags", Map.of("cat", catalog)).get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(ListTagsResponse.class).getIdentifiers().stream().toList();
    }
  }

  public Tag createTag(
      String catalog,
      String tagName,
      String comment,
      List<String> allowedValues,
      List<TargetType> targetTypes) {
    CreateTagRequest request =
        CreateTagRequest.builder()
            .setName(tagName)
            .setComment(comment)
            .setAllowedValues(allowedValues)
            .setTargetTypes(targetTypes)
            .build();
    try (Response res =
        request("polaris/v1/{cat}/tags", Map.of("cat", catalog)).post(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(LoadTagResponse.class).getTag();
    }
  }

  public Tag loadTag(String catalog, String tagName) {
    try (Response res =
        request("polaris/v1/{cat}/tags/{tag}", Map.of("cat", catalog, "tag", tagName)).get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(LoadTagResponse.class).getTag();
    }
  }

  public Tag updateTag(String catalog, String tagName, UpdateTagRequest request) {
    try (Response res =
        request("polaris/v1/{cat}/tags/{tag}", Map.of("cat", catalog, "tag", tagName))
            .put(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(LoadTagResponse.class).getTag();
    }
  }

  public void dropTag(String catalog, String tagName) {
    dropTag(catalog, tagName, null);
  }

  public void dropTag(String catalog, String tagName, Boolean detachAll) {
    Map<String, String> queryParams = new HashMap<>();
    if (detachAll != null) {
      queryParams.put("detach-all", detachAll.toString());
    }
    try (Response res =
        request("polaris/v1/{cat}/tags/{tag}", Map.of("cat", catalog, "tag", tagName), queryParams)
            .delete()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
  }
}
