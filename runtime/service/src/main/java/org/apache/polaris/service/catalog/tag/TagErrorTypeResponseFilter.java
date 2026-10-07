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
package org.apache.polaris.service.catalog.tag;

import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.container.ContainerResponseContext;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.service.catalog.api.PolarisCatalogTagApi;
import org.jboss.resteasy.reactive.server.ServerResponseFilter;
import org.jboss.resteasy.reactive.server.SimpleResourceInfo;

/**
 * Gives the Tag API the error-type strings its OpenAPI document specifies.
 *
 * <p>The Tag spec names types without the {@code Exception} suffix, such as {@code NoSuchTag} and
 * {@code BadRequest}. The exception mappers derive the type from {@link Class#getSimpleName()}, and
 * they are shared: the generic mapper serves every Iceberg REST error in the service, and the
 * Polaris one serves every {@code PolarisException} subclass. Tag also throws exceptions it does
 * not own, so no set of exception classes separates tag errors from the rest. Rewriting the type in
 * either mapper would therefore rename error types across unrelated APIs.
 *
 * <p>Scoping by resource class rather than by request path is deliberate. The generated tag
 * resource and the generated policy resource carry the same class-level {@code @Path}, and one tag
 * route sits at {@code /object-tags} rather than under {@code /tags}, so no path prefix identifies
 * tag requests exactly.
 *
 * <p>One case needs the status corrected as well, not only the type string: a query parameter the
 * platform cannot read is refused before any resource code runs, and the refusal arrives as 404.
 * See {@link #rewriteUnreadablePageSize}.
 *
 * <p>Java class names are unchanged, and so is every other API's payload.
 */
public class TagErrorTypeResponseFilter {

  private static final String EXCEPTION_SUFFIX = "Exception";

  /**
   * Wire types whose specified literal is not simply the exception class name without its suffix.
   * The contract fixes these strings, and an exception class name does not select them: a name
   * collision on a tag is the shared {@code AlreadyExists}, a body that is not valid JSON is {@code
   * InvalidJson}, and a request that fails authentication is {@code Unauthorized} rather than the
   * class's own spelling.
   */
  private static final Map<String, String> SPECIFIED_TYPES =
      Map.of(
          "AlreadyExistsException", "AlreadyExists",
          "JsonParseException", "InvalidJson",
          "NotAuthorizedException", "Unauthorized");

  /**
   * Every 400 literal the Tag contract names, whichever layer already sets it correctly: the tag
   * paths raise {@code BadRequest} and {@code TagInUse} themselves, and the shared idempotency
   * filter rejects a malformed {@code Idempotency-Key} header with {@code InvalidIdempotencyKey}
   * before any tag code runs. Those pass through untouched. Every other 400 on a tag route is a
   * shared schema failure, which the contract reports as {@code ValidationError}: a missing
   * required field, a field of the wrong type, or a {@code current-tag-version} that is absent,
   * null, empty or not a string. {@code InvalidJson} arrives through {@link #SPECIFIED_TYPES}
   * instead, because there the class name is what has to be translated.
   */
  private static final Set<String> CONTRACT_NAMED_400_TYPES =
      Set.of("BadRequest", "TagInUse", "InvalidIdempotencyKey");

  @ServerResponseFilter
  public void rewriteTagErrorType(
      ContainerResponseContext response, SimpleResourceInfo resourceInfo, Throwable thrown) {
    if (resourceInfo == null
        || resourceInfo.getResourceClass() != PolarisCatalogTagApi.class
        || !(response.getEntity() instanceof ErrorResponse error)) {
      return;
    }
    if (rewriteUnreadablePageSize(response, thrown)) {
      return;
    }
    String specifiedType = specifiedType(error.type(), error.code());
    if (Objects.equals(specifiedType, error.type())) {
      return;
    }
    ErrorResponse.Builder rewritten =
        ErrorResponse.builder()
            .responseCode(error.code())
            .withType(specifiedType)
            .withMessage(error.message());
    if (error.stack() != null) {
      rewritten.withStackTrace(error.stack());
    }
    response.setEntity(rewritten.build());
  }

  /**
   * Answers 400 for a page size the request sent in a form that cannot be read.
   *
   * <p>A query parameter that fails conversion is refused before any resource code runs, and that
   * refusal arrives as 404, which on these routes is the answer for a catalog or a definition that
   * is not there. The tag routes promise 400 for a page size that is not a positive integer, so
   * this corrects that one case. The response carries the exception that produced it, and a
   * conversion failure is what gives a not-found exception a numeric-parse cause; a 404 the tag
   * code raised itself has no such cause and is left exactly as it is. {@code pageSize} is the only
   * numeric query parameter these routes declare, so the message names it rather than guessing.
   *
   * <p>TODO: the proper fix is a parameter converter that answers 400 for every API instead of one
   * resource class. That belongs in a separate change, and this branch goes away when it lands.
   */
  private static boolean rewriteUnreadablePageSize(
      ContainerResponseContext response, Throwable thrown) {
    if (response.getStatus() != 404
        || !(thrown instanceof NotFoundException)
        || !(thrown.getCause() instanceof NumberFormatException)) {
      return false;
    }
    response.setStatus(400);
    response.setEntity(
        ErrorResponse.builder()
            .responseCode(400)
            .withType("BadRequest")
            .withMessage("The pageSize parameter must be a positive integer")
            .build());
    return true;
  }

  static String specifiedType(String type, Integer code) {
    if (type == null) {
      return null;
    }
    String specified = SPECIFIED_TYPES.get(type);
    if (specified != null) {
      return specified;
    }
    int status = code == null ? 0 : code;
    if (status >= 500) {
      // The contract treats a server failure as opaque, so it gets one literal rather than the
      // name of whatever failed.
      return status == 503 ? "ServiceUnavailable" : "InternalServerError";
    }
    if (status == 401) {
      return "Unauthorized";
    }
    if (status == 403) {
      return "Forbidden";
    }
    String stripped =
        type.endsWith(EXCEPTION_SUFFIX) && !type.equals(EXCEPTION_SUFFIX)
            ? type.substring(0, type.length() - EXCEPTION_SUFFIX.length())
            : type;
    if (status == 400 && !CONTRACT_NAMED_400_TYPES.contains(stripped)) {
      return "ValidationError";
    }
    return stripped;
  }
}
