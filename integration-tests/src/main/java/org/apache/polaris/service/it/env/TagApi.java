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
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.RenameTagRequest;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.assertj.core.api.Assertions;
import org.jspecify.annotations.Nullable;

public class TagApi extends PolarisRestApi {
  TagApi(Client client, PolarisApiEndpoints endpoints, String authToken, URI uri) {
    super(client, endpoints, authToken, uri);
  }

  public void purge(String catalog) {
    // Plain drops are enough while no assignment can exist; revisit with the assignment change so
    // purge stays effective once they do.
    listTags(catalog).forEach(t -> dropTag(catalog, t.getName()));
  }

  /**
   * Cleanup-only variant of {@link #purge}. Exactly one status means "no tag surface here, nothing
   * to purge": 406, the disabled feature flag's response. Anything else non-OK, including 400, is a
   * regression and fails with the response body, so shared cleanup stays a usable test oracle.
   */
  public void purgeIfAvailable(String catalog) {
    List<TagIdentifier> tags;
    try (Response res =
        request("polaris/v1/{cat}/tags", Map.of("cat", catalog), Map.of("pagination", "false"))
            .get()) {
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
    return listTagsAll(catalog).getIdentifiers().stream().toList();
  }

  /**
   * Lists the complete collection in one response. Listings page by default, so a caller that wants
   * every definition has to say so; sending nothing would ask for the first page instead.
   */
  public ListTagsResponse listTagsAll(String catalog) {
    try (Response res =
        request("polaris/v1/{cat}/tags", Map.of("cat", catalog), Map.of("pagination", "false"))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(ListTagsResponse.class);
    }
  }

  /**
   * Lists in paged mode, which is what a request with no {@code pagination} flag gets, and returns
   * the whole response so a test can assert on next-page-token. {@link #listTags} discards it.
   */
  public ListTagsResponse listTagsPage(String catalog, String pageToken, Integer pageSize) {
    Map<String, String> queryParams = new HashMap<>();
    if (pageToken != null) {
      queryParams.put("pageToken", pageToken);
    }
    if (pageSize != null) {
      queryParams.put("pageSize", pageSize.toString());
    }
    try (Response res =
        request("polaris/v1/{cat}/tags", Map.of("cat", catalog), queryParams).get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(ListTagsResponse.class);
    }
  }

  /**
   * Lists with one query parameter supplied several times and returns the raw response. The
   * contract keys on the parameter appearing more than once, which a map of parameters cannot say.
   */
  public Response listTagsWithRepeatedParameter(String catalog, String name, String... values) {
    WebTarget target = target("polaris/v1/{cat}/tags", Map.of("cat", catalog));
    for (String value : values) {
      target = target.queryParam(name, value);
    }
    return request(target, defaultHeaders()).get();
  }

  /** Lists with a raw query string, so a test can send a value the typed helpers cannot express. */
  public Response listTagsRaw(String catalog, String query) {
    WebTarget target = target("polaris/v1/{cat}/tags", Map.of("cat", catalog));
    for (String pair : query.split("&")) {
      int eq = pair.indexOf('=');
      target = target.queryParam(pair.substring(0, eq), pair.substring(eq + 1));
    }
    return request(target, defaultHeaders()).get();
  }

  public Tag createTag(
      String catalog,
      String tagName,
      String description,
      List<String> values,
      List<TargetType> targetTypes) {
    CreateTagRequest request =
        CreateTagRequest.builder()
            .setName(tagName)
            .setDescription(description)
            .setValues(values)
            .setTargetTypes(targetTypes)
            .build();
    try (Response res =
        request("polaris/v1/{cat}/tags", Map.of("cat", catalog)).post(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(Tag.class);
    }
  }

  public Tag loadTag(String catalog, String tagName) {
    try (Response res =
        request("polaris/v1/{cat}/tags/{tag}", Map.of("cat", catalog, "tag", tagName)).get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(Tag.class);
    }
  }

  public Tag updateTag(String catalog, String tagName, UpdateTagRequest request) {
    try (Response res =
        request("polaris/v1/{cat}/tags/{tag}", Map.of("cat", catalog, "tag", tagName))
            .put(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(Tag.class);
    }
  }

  /**
   * Renames a definition and asserts the documented 204. A recognized retry answers the same 204,
   * so a caller cannot tell the two apart, which is the point of the guarantee.
   */
  public void renameTag(String catalog, String source, String destination, String currentVersion) {
    try (Response res = renameTagResponse(catalog, source, destination, currentVersion, null)) {
      Assertions.assertThat(res.getStatus())
          .as(() -> res.readEntity(String.class))
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
  }

  /**
   * Renames a definition and hands back the raw response, optionally under an idempotency key, so a
   * test can assert a status and an error type of its own.
   */
  public Response renameTagResponse(
      String catalog,
      String source,
      String destination,
      String currentVersion,
      @Nullable UUID idempotencyKey) {
    RenameTagRequest request =
        RenameTagRequest.builder()
            .setSource(source)
            .setDestination(destination)
            .setCurrentTagVersion(currentVersion)
            .build();
    return request("polaris/v1/{cat}/tags/rename", Map.of("cat", catalog), idempotencyKey)
        .post(Entity.json(request));
  }

  /** Creates a definition under an idempotency key, returning the raw response. */
  public Response createTagResponse(
      String catalog, CreateTagRequest request, @Nullable UUID idempotencyKey) {
    return request("polaris/v1/{cat}/tags", Map.of("cat", catalog), idempotencyKey)
        .post(Entity.json(request));
  }

  /** Updates a definition under an idempotency key, returning the raw response. */
  public Response updateTagResponse(
      String catalog, String tagName, UpdateTagRequest request, @Nullable UUID idempotencyKey) {
    return request(
            "polaris/v1/{cat}/tags/{tag}", Map.of("cat", catalog, "tag", tagName), idempotencyKey)
        .put(Entity.json(request));
  }

  /** Drops a definition under an idempotency key, returning the raw response. */
  public Response dropTagResponse(String catalog, String tagName, @Nullable UUID idempotencyKey) {
    return request(
            "polaris/v1/{cat}/tags/{tag}", Map.of("cat", catalog, "tag", tagName), idempotencyKey)
        .delete();
  }

  /**
   * A request carrying the shared idempotency header when one is supplied. The header is the only
   * way a client asks for a retry to be recognized, and no other tag helper needs to send it.
   */
  private Invocation.Builder request(
      String path, Map<String, String> templateValues, @Nullable UUID idempotencyKey) {
    Map<String, String> headers = new HashMap<>(defaultHeaders());
    if (idempotencyKey != null) {
      headers.put("Idempotency-Key", idempotencyKey.toString());
    }
    return request(target(path, templateValues), headers);
  }

  /**
   * A request carrying an arbitrary Idempotency-Key value, so a test can send one the shared filter
   * has to reject. Every well-formed key goes through the UUID-typed helpers above.
   */
  public Invocation.Builder requestWithRawIdempotencyKey(
      String path, Map<String, String> templateValues, String idempotencyKey) {
    Map<String, String> headers = new HashMap<>(defaultHeaders());
    headers.put("Idempotency-Key", idempotencyKey);
    return request(target(path, templateValues), headers);
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
