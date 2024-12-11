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
package org.apache.polaris.service.auth;

import jakarta.ws.rs.core.Response;

/** Simple utility class to assist with OAuth operations */
public class OAuthUtils {
  public static final String POLARIS_ROLE_PREFIX = "PRINCIPAL_ROLE:";

  public static Response getResponseFromError(OAuthTokenErrorResponse.Error error) {
    return switch (error) {
      case unauthorized_client ->
          Response.status(Response.Status.UNAUTHORIZED)
              .entity(
                  new OAuthTokenErrorResponse(OAuthTokenErrorResponse.Error.unauthorized_client))
              .build();
      case invalid_client ->
          Response.status(Response.Status.BAD_REQUEST)
              .entity(new OAuthTokenErrorResponse(OAuthTokenErrorResponse.Error.invalid_client))
              .build();
      case invalid_grant ->
          Response.status(Response.Status.BAD_REQUEST)
              .entity(new OAuthTokenErrorResponse(OAuthTokenErrorResponse.Error.invalid_grant))
              .build();
      case unsupported_grant_type ->
          Response.status(Response.Status.BAD_REQUEST)
              .entity(
                  new OAuthTokenErrorResponse(OAuthTokenErrorResponse.Error.unsupported_grant_type))
              .build();
      case invalid_scope ->
          Response.status(Response.Status.BAD_REQUEST)
              .entity(new OAuthTokenErrorResponse(OAuthTokenErrorResponse.Error.invalid_scope))
              .build();
      default ->
          Response.status(Response.Status.BAD_REQUEST)
              .entity(new OAuthTokenErrorResponse(OAuthTokenErrorResponse.Error.invalid_request))
              .build();
    };
  }
}
