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

package org.apache.polaris.core.storage.aws.r2;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The validated endpoint of a Cloudflare R2 catalog, {@code
 * https://<accountId>[.<jurisdiction>].r2.cloudflarestorage.com}.
 *
 * <p>The whole string is matched: userinfo, a port, a path, a query or a fragment all fail. The
 * account id (32 lowercase hex characters) is the subject of vended credentials and the host is
 * their audience, so both are read from a string that has already been proven to be an R2 endpoint;
 * the endpoint is never parsed to detect R2, the issuer declares it.
 *
 * @param accountId the Cloudflare account id, the first host label
 * @param jurisdiction the optional jurisdiction label, one of {@link #KNOWN_JURISDICTIONS}
 * @param host the endpoint host, exactly as it appears in the endpoint
 */
public record CloudflareR2Endpoint(
    @NonNull String accountId, @Nullable String jurisdiction, @NonNull String host) {

  public static final String HOST_SUFFIX = "r2.cloudflarestorage.com";

  /** Jurisdictions documented at developers.cloudflare.com/r2/reference/data-location/. */
  public static final List<String> KNOWN_JURISDICTIONS = List.of("eu", "fedramp", "us");

  private static final Pattern ENDPOINT_PATTERN =
      Pattern.compile(
          "^https://(([0-9a-f]{32})(?:\\.("
              + String.join("|", KNOWN_JURISDICTIONS)
              + "))?\\."
              + Pattern.quote(HOST_SUFFIX)
              + ")$");

  /**
   * Parses and validates an endpoint string.
   *
   * @throws IllegalArgumentException when the endpoint is null or is not exactly an R2 endpoint
   */
  public static CloudflareR2Endpoint parse(@Nullable String endpoint) {
    if (endpoint == null) {
      throw new IllegalArgumentException(
          "endpoint is required for the CLOUDFLARE_R2 credential issuer; expected"
              + " https://<accountId>[.<jurisdiction>]."
              + HOST_SUFFIX);
    }
    Matcher matcher = ENDPOINT_PATTERN.matcher(endpoint);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          String.format(
              "endpoint '%s' is not a Cloudflare R2 endpoint; expected"
                  + " https://<accountId>[.<jurisdiction>].%s with a 32-character lowercase hex"
                  + " account id and a jurisdiction in %s, no userinfo, port, path, query or"
                  + " fragment",
              endpoint, HOST_SUFFIX, KNOWN_JURISDICTIONS));
    }
    return new CloudflareR2Endpoint(matcher.group(2), matcher.group(3), matcher.group(1));
  }
}
