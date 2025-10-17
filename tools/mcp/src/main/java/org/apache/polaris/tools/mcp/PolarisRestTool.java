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

package org.apache.polaris.tools.mcp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Tool that issues HTTP requests to the Polaris REST APIs and returns the raw response payload. */
final class PolarisRestTool implements McpTool {
  private static final String HTTP = "http://";
  private static final String HTTPS = "https://";
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

  private final String name;
  private final String description;
  private final URI baseUri;
  private final String defaultPathPrefix;
  private final ObjectMapper mapper;
  private final HttpExecutor httpExecutor;
  private final AuthorizationProvider authorizationProvider;

  PolarisRestTool(
      String name,
      String description,
      URI baseUri,
      String defaultPathPrefix,
      ObjectMapper mapper,
      HttpExecutor httpExecutor,
      AuthorizationProvider authorizationProvider) {
    this.name = Objects.requireNonNull(name, "name must not be null");
    this.description = Objects.requireNonNull(description, "description must not be null");
    this.baseUri = ensureTrailingSlash(Objects.requireNonNull(baseUri, "baseUri must not be null"));
    this.defaultPathPrefix = normalizePrefix(defaultPathPrefix);
    this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    this.httpExecutor = Objects.requireNonNull(httpExecutor, "httpExecutor must not be null");
    this.authorizationProvider =
        authorizationProvider == null ? AuthorizationProvider.none() : authorizationProvider;
  }

  static PolarisRestTool general(
      String name,
      String description,
      URI baseUri,
      ObjectMapper mapper,
      HttpExecutor executor,
      AuthorizationProvider authorizationProvider) {
    return new PolarisRestTool(
        name, description, baseUri, "", mapper, executor, authorizationProvider);
  }

  static PolarisRestTool withPrefix(
      String name,
      String description,
      URI baseUri,
      String defaultPathPrefix,
      ObjectMapper mapper,
      HttpExecutor executor,
      AuthorizationProvider authorizationProvider) {
    return new PolarisRestTool(
        name, description, baseUri, defaultPathPrefix, mapper, executor, authorizationProvider);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String description() {
    return description;
  }

  @Override
  public ObjectNode inputSchema(ObjectMapper mapper) {
    ObjectNode schema = mapper.createObjectNode();
    schema.put("type", "object");

    ObjectNode properties = schema.putObject("properties");

    ObjectNode methodNode = mapper.createObjectNode();
    methodNode.put("type", "string");
    methodNode.put(
        "description",
        "HTTP method, e.g. GET, POST, PUT, DELETE, PATCH, HEAD or OPTIONS. Defaults to GET.");
    properties.set("method", methodNode);

    ObjectNode pathNode = mapper.createObjectNode();
    pathNode.put("type", "string");
    pathNode.put(
        "description",
        "Relative path under the Polaris base URL, such as /api/management/v1/catalogs. "
            + "Absolute URLs are also accepted.");
    properties.set("path", pathNode);

    ObjectNode queryNode = mapper.createObjectNode();
    queryNode.put("type", "object");
    queryNode.put(
        "description",
        "Optional query string parameters. Values can be strings or arrays of strings.");
    ObjectNode queryAdditional = queryNode.putObject("additionalProperties");
    ArrayNode queryAnyOf = queryAdditional.putArray("anyOf");
    queryAnyOf.addObject().put("type", "string");
    ObjectNode queryArray = queryAnyOf.addObject();
    queryArray.put("type", "array");
    queryArray.set("items", mapper.createObjectNode().put("type", "string"));
    properties.set("query", queryNode);

    ObjectNode headersNode = mapper.createObjectNode();
    headersNode.put("type", "object");
    headersNode.put(
        "description",
        "Optional request headers. Accept and Authorization headers are supplied automatically "
            + "when omitted.");
    ObjectNode headerAdditional = headersNode.putObject("additionalProperties");
    ArrayNode headerAnyOf = headerAdditional.putArray("anyOf");
    headerAnyOf.addObject().put("type", "string");
    ObjectNode headerArray = headerAnyOf.addObject();
    headerArray.put("type", "array");
    headerArray.set("items", mapper.createObjectNode().put("type", "string"));
    properties.set("headers", headersNode);

    ObjectNode bodyNode = mapper.createObjectNode();
    ArrayNode bodyType = bodyNode.putArray("type");
    bodyType.add("object");
    bodyType.add("array");
    bodyType.add("string");
    bodyType.add("number");
    bodyType.add("boolean");
    bodyType.add("null");
    bodyNode.put(
        "description",
        "Optional request body. Objects and arrays are serialized as JSON, strings are sent as-is.");
    properties.set("body", bodyNode);

    ArrayNode required = schema.putArray("required");
    required.add("path");

    return schema;
  }

  @Override
  public ToolExecutionResult call(ObjectMapper mapper, JsonNode arguments) throws Exception {
    if (!(arguments instanceof ObjectNode)) {
      throw new IllegalArgumentException("Tool arguments must be a JSON object.");
    }
    ObjectNode args = (ObjectNode) arguments;

    String method =
        args.has("method") ? args.get("method").asText("").trim().toUpperCase(Locale.ROOT) : "GET";
    if (method.isEmpty()) {
      method = "GET";
    }

    String path = requirePath(args);
    ObjectNode query = asObjectNode(args.get("query"));
    ObjectNode headers = asObjectNode(args.get("headers"));
    JsonNode bodyNode = args.get("body");

    URI targetUri = resolveTargetUri(path, query);

    List<HeaderValue> requestHeaders = buildHeaders(headers);
    boolean hasAuthorization =
        requestHeaders.stream().anyMatch(h -> "authorization".equalsIgnoreCase(h.name()));
    if (!hasAuthorization) {
      Optional<String> headerValue = authorizationProvider.authorizationHeader();
      if (headerValue.isPresent()) {
        requestHeaders.add(new HeaderValue("Authorization", headerValue.get()));
      }
    }

    boolean hasContentType =
        requestHeaders.stream().anyMatch(h -> "content-type".equalsIgnoreCase(h.name()));

    String outboundBody = serializeBody(bodyNode);
    if (outboundBody != null && !hasContentType) {
      requestHeaders.add(new HeaderValue("Content-Type", "application/json"));
    }

    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(targetUri).timeout(DEFAULT_TIMEOUT);
    for (HeaderValue header : requestHeaders) {
      requestBuilder.header(header.name(), header.value());
    }

    if (outboundBody != null) {
      requestBuilder.method(
          method, HttpRequest.BodyPublishers.ofString(outboundBody, StandardCharsets.UTF_8));
    } else {
      requestBuilder.method(method, HttpRequest.BodyPublishers.noBody());
    }

    HttpRequest request = requestBuilder.build();
    HttpResponse<String> response;
    try {
      response = httpExecutor.execute(request);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw interrupted;
    }

    String responseBody = response.body() == null ? "" : response.body();
    String renderedBody = renderBody(responseBody);

    StringBuilder message = new StringBuilder();
    message
        .append(request.method())
        .append(" ")
        .append(request.uri())
        .append(System.lineSeparator());
    message.append("Status: ").append(response.statusCode()).append(System.lineSeparator());
    response
        .headers()
        .map()
        .forEach(
            (key, values) -> {
              if (!values.isEmpty()) {
                message.append(key).append(": ").append(String.join(", ", values));
                message.append(System.lineSeparator());
              }
            });
    if (!renderedBody.isBlank()) {
      message.append(System.lineSeparator()).append(renderedBody);
    }

    ObjectNode metadata = buildMetadata(request, outboundBody, response, responseBody);
    boolean isError = response.statusCode() >= 400;
    return new ToolExecutionResult(message.toString(), isError, metadata);
  }

  private String requirePath(ObjectNode args) {
    JsonNode pathNode = args.get("path");
    if (pathNode == null || pathNode.asText().trim().isEmpty()) {
      throw new IllegalArgumentException(
          "The 'path' argument must be provided and must not be empty.");
    }
    return pathNode.asText();
  }

  private ObjectNode asObjectNode(JsonNode node) {
    return node instanceof ObjectNode ? (ObjectNode) node : null;
  }

  private List<HeaderValue> buildHeaders(ObjectNode headers) {
    List<HeaderValue> values = new ArrayList<>();
    values.add(new HeaderValue("Accept", "application/json"));
    if (headers != null) {
      Iterator<Map.Entry<String, JsonNode>> fields = headers.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        String name = entry.getKey();
        if (name == null || name.isBlank()) {
          continue;
        }
        JsonNode value = entry.getValue();
        if (value == null || value.isNull()) {
          continue;
        }
        if (value.isArray()) {
          for (JsonNode element : value) {
            if (!element.isNull()) {
              values.add(new HeaderValue(name, element.asText()));
            }
          }
        } else {
          values.add(new HeaderValue(name, value.asText()));
        }
      }
    }
    return values;
  }

  private String serializeBody(JsonNode bodyNode) throws JsonProcessingException {
    if (bodyNode == null || bodyNode.isNull()) {
      return null;
    }
    if (bodyNode.isTextual()) {
      return bodyNode.asText();
    }
    return mapper.writeValueAsString(bodyNode);
  }

  private String renderBody(String rawBody) {
    if (rawBody == null || rawBody.isBlank()) {
      return "";
    }
    try {
      JsonNode json = mapper.readTree(rawBody);
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
    } catch (JsonProcessingException ignored) {
      return rawBody;
    }
  }

  private ObjectNode buildMetadata(
      HttpRequest request, String requestBody, HttpResponse<String> response, String responseBody) {
    ObjectNode metadata = mapper.createObjectNode();
    metadata.put("method", request.method());
    metadata.put("url", request.uri().toString());
    metadata.put("status", response.statusCode());

    ObjectNode requestNode = metadata.putObject("request");
    requestNode.put("method", request.method());
    requestNode.put("url", request.uri().toString());
    ObjectNode requestHeaders = requestNode.putObject("headers");
    request
        .headers()
        .map()
        .forEach((key, values) -> requestHeaders.put(key, String.join(", ", values)));
    if (requestBody != null) {
      try {
        requestNode.set("body", mapper.readTree(requestBody));
      } catch (JsonProcessingException ex) {
        requestNode.put("bodyText", requestBody);
      }
    }

    ObjectNode responseNode = metadata.putObject("response");
    responseNode.put("status", response.statusCode());
    ObjectNode responseHeaders = responseNode.putObject("headers");
    response
        .headers()
        .map()
        .forEach((key, values) -> responseHeaders.put(key, String.join(", ", values)));
    if (responseBody != null && !responseBody.isBlank()) {
      try {
        responseNode.set("body", mapper.readTree(responseBody));
      } catch (JsonProcessingException ex) {
        responseNode.put("bodyText", responseBody);
      }
    }

    return metadata;
  }

  private URI resolveTargetUri(String path, ObjectNode query) {
    URI uri;
    if (path.startsWith(HTTP) || path.startsWith(HTTPS)) {
      uri = URI.create(path);
    } else {
      String normalizedPath = path.startsWith("/") ? path.substring(1) : path;
      StringBuilder relative = new StringBuilder();
      if (!defaultPathPrefix.isEmpty()) {
        relative.append(defaultPathPrefix);
      }
      relative.append(normalizedPath);
      uri = baseUri.resolve(relative.toString());
    }

    List<String> parameters = buildQueryParameters(query);
    if (parameters.isEmpty()) {
      return uri;
    }

    StringBuilder builder = new StringBuilder(uri.toString());
    if (uri.getQuery() == null || uri.getQuery().isEmpty()) {
      builder.append(uri.toString().contains("?") ? "&" : "?");
    } else {
      builder.append("&");
    }
    builder.append(String.join("&", parameters));
    return URI.create(builder.toString());
  }

  private List<String> buildQueryParameters(ObjectNode query) {
    List<String> parameters = new ArrayList<>();
    if (query == null) {
      return parameters;
    }

    Iterator<Map.Entry<String, JsonNode>> fields = query.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String key = entry.getKey();
      if (key == null || key.isBlank()) {
        continue;
      }
      JsonNode value = entry.getValue();
      if (value == null || value.isNull()) {
        continue;
      }
      if (value.isArray()) {
        for (JsonNode element : value) {
          if (!element.isNull()) {
            parameters.add(encode(key) + "=" + encode(element.asText()));
          }
        }
      } else {
        parameters.add(encode(key) + "=" + encode(value.asText()));
      }
    }
    return parameters;
  }

  private String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static URI ensureTrailingSlash(URI base) {
    String value = base.toString();
    if (!value.endsWith("/")) {
      value = value + "/";
    }
    return URI.create(value);
  }

  private static String normalizePrefix(String prefix) {
    if (prefix == null || prefix.isBlank()) {
      return "";
    }
    String trimmed = prefix.trim();
    if (trimmed.startsWith("/")) {
      trimmed = trimmed.substring(1);
    }
    if (!trimmed.isEmpty() && !trimmed.endsWith("/")) {
      trimmed = trimmed + "/";
    }
    return trimmed;
  }

  static HttpExecutor defaultExecutor() {
    HttpClient client =
        HttpClient.newBuilder()
            .connectTimeout(DEFAULT_TIMEOUT)
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    return request -> client.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static final class HeaderValue {
    private final String name;
    private final String value;

    HeaderValue(String name, String value) {
      this.name = Objects.requireNonNull(name, "name must not be null");
      this.value = Objects.requireNonNull(value, "value must not be null");
    }

    String name() {
      return name;
    }

    String value() {
      return value;
    }
  }
}
