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
package org.apache.polaris.core.storage.r2;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Mints Cloudflare R2 temporary credentials by local JWT signing, the path Cloudflare documents for
 * high-volume minting (developers.cloudflare.com/r2/api/s3/temporary-credentials/): sign a JWT with
 * the parent token's secret; the temporary secret is the SHA-256 hex of the signed JWT; the session
 * token is base64("jwt/" + JWT); the parent access key id is reused. Pure function: no I/O, no
 * clock of its own.
 */
public final class R2TemporaryCredentialSigner {

  public static final String SCOPE_OBJECT_READ_ONLY = "object-read-only";
  public static final String SCOPE_OBJECT_READ_WRITE = "object-read-write";
  public static final String SESSION_TOKEN_PREFIX = "jwt/";

  private R2TemporaryCredentialSigner() {}

  /**
   * @param prefixes normalized key prefixes (no leading slash, one trailing slash); empty means the
   *     whole bucket and omits the {@code paths} claim
   */
  public record Request(
      String parentKeyId,
      String parentSecret,
      String accountId,
      String audienceHost,
      String bucket,
      List<String> prefixes,
      String scope,
      Duration ttl,
      Instant now) {
    @Override
    public String toString() {
      return "Request{parentKeyId="
          + parentKeyId
          + ", parentSecret=<redacted>, accountId="
          + accountId
          + ", audienceHost="
          + audienceHost
          + ", bucket="
          + bucket
          + ", prefixes="
          + prefixes
          + ", scope="
          + scope
          + ", ttl="
          + ttl
          + ", now="
          + now
          + "}";
    }
  }

  public record Credential(
      String accessKeyId, String secretAccessKey, String sessionToken, Instant expiresAt) {
    @Override
    public String toString() {
      return "Credential{accessKeyId=" + accessKeyId + ", expiresAt=" + expiresAt + "}";
    }
  }

  public static Credential sign(Request request) {
    // java-jwt writes iat and exp as whole seconds, so truncate here and let the advertised
    // expiresAt match the exp claim exactly rather than carrying sub-second precision the token
    // does not have.
    Instant now = request.now().truncatedTo(ChronoUnit.SECONDS);
    Instant expiresAt = now.plus(request.ttl());
    JWTCreator.Builder builder =
        JWT.create()
            .withSubject(request.accountId())
            .withIssuer(request.parentKeyId())
            .withAudience(request.audienceHost())
            .withIssuedAt(now)
            .withExpiresAt(expiresAt)
            .withClaim("bucket", request.bucket())
            .withClaim("scope", request.scope());
    if (!request.prefixes().isEmpty()) {
      builder.withClaim(
          "paths", Map.of("prefixPaths", request.prefixes(), "objectPaths", List.of()));
    }
    String jwt = builder.sign(Algorithm.HMAC256(request.parentSecret()));
    String sessionToken =
        Base64.getEncoder()
            .encodeToString((SESSION_TOKEN_PREFIX + jwt).getBytes(StandardCharsets.UTF_8));
    return new Credential(request.parentKeyId(), sha256Hex(jwt), sessionToken, expiresAt);
  }

  private static String sha256Hex(String value) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}
