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
package org.apache.polaris.extension.storage.cloudflare.r2;

import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class CloudflareR2TemporaryCredentialSignerTest {

  private static final String ACCOUNT = "0123456789abcdef0123456789abcdef";
  private static final String PARENT_KEY = "parent-access-key-id";
  private static final String PARENT_SECRET = "not-a-real-secret-0123456789abcdef";
  private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.SECONDS);

  private static CloudflareR2TemporaryCredentialSigner.Request request(
      String host, List<String> prefixes, String scope) {
    return new CloudflareR2TemporaryCredentialSigner.Request(
        PARENT_KEY,
        PARENT_SECRET,
        ACCOUNT,
        host,
        "my-bucket",
        prefixes,
        scope,
        Duration.ofSeconds(3600),
        NOW);
  }

  private static DecodedJWT verify(String sessionToken) {
    String decoded = new String(Base64.getDecoder().decode(sessionToken), StandardCharsets.UTF_8);
    assertThat(decoded).startsWith(CloudflareR2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX);
    String jwt =
        decoded.substring(CloudflareR2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX.length());
    return JWT.require(Algorithm.HMAC256(PARENT_SECRET)).build().verify(jwt);
  }

  /** The JSON text of the token payload: the second part of the JWT, decoded from base64url. */
  private static String rawPayload(String sessionToken) {
    String decoded = new String(Base64.getDecoder().decode(sessionToken), StandardCharsets.UTF_8);
    assertThat(decoded).startsWith(CloudflareR2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX);
    String jwt =
        decoded.substring(CloudflareR2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX.length());
    String[] parts = jwt.split("\\.");
    assertThat(parts).hasSize(3);
    return new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
  }

  @Test
  void signsDefaultJurisdictionCredential() throws Exception {
    CloudflareR2TemporaryCredentialSigner.Credential cred =
        CloudflareR2TemporaryCredentialSigner.sign(
            request(
                ACCOUNT + ".r2.cloudflarestorage.com",
                List.of("warehouse/db/table/"),
                CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_WRITE));

    assertThat(cred.accessKeyId()).isEqualTo(PARENT_KEY);
    assertThat(cred.expiresAt()).isEqualTo(NOW.plusSeconds(3600));

    DecodedJWT jwt = verify(cred.sessionToken());
    assertThat(jwt.getAlgorithm()).isEqualTo("HS256");
    assertThat(jwt.getSubject()).isEqualTo(ACCOUNT);
    assertThat(jwt.getIssuer()).isEqualTo(PARENT_KEY);
    assertThat(jwt.getAudience()).containsExactly(ACCOUNT + ".r2.cloudflarestorage.com");
    assertThat(jwt.getIssuedAtAsInstant()).isEqualTo(NOW);
    assertThat(jwt.getExpiresAtAsInstant()).isEqualTo(NOW.plusSeconds(3600));
    assertThat(jwt.getClaim("bucket").asString()).isEqualTo("my-bucket");
    assertThat(jwt.getClaim("scope").asString()).isEqualTo("object-read-write");
    assertThat(jwt.getClaim("paths").asMap())
        .containsEntry("prefixPaths", List.of("warehouse/db/table/"))
        .containsEntry("objectPaths", List.of());

    String rawJwt =
        new String(Base64.getDecoder().decode(cred.sessionToken()), StandardCharsets.UTF_8)
            .substring(CloudflareR2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX.length());
    byte[] digest =
        MessageDigest.getInstance("SHA-256").digest(rawJwt.getBytes(StandardCharsets.UTF_8));
    assertThat(cred.secretAccessKey()).isEqualTo(HexFormat.of().formatHex(digest));
  }

  static List<String> knownJurisdictions() {
    return CloudflareR2Endpoint.KNOWN_JURISDICTIONS;
  }

  @ParameterizedTest
  @MethodSource("knownJurisdictions")
  void usesJurisdictionHostAsAudience(String jurisdiction) {
    String host = ACCOUNT + "." + jurisdiction + ".r2.cloudflarestorage.com";
    CloudflareR2TemporaryCredentialSigner.Credential cred =
        CloudflareR2TemporaryCredentialSigner.sign(
            request(
                host, List.of("p/"), CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY));
    DecodedJWT jwt = verify(cred.sessionToken());
    assertThat(jwt.getAudience()).containsExactly(host);
    assertThat(jwt.getClaim("scope").asString()).isEqualTo("object-read-only");
  }

  @Test
  void omitsPathsClaimForBucketRootGrant() {
    CloudflareR2TemporaryCredentialSigner.Credential cred =
        CloudflareR2TemporaryCredentialSigner.sign(
            request(
                ACCOUNT + ".r2.cloudflarestorage.com",
                List.of(),
                CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY));
    assertThat(verify(cred.sessionToken()).getClaim("paths").isMissing()).isTrue();
  }

  @Test
  void aSingleSlashPrefixStillEmitsThePathsClaim() {
    // Only an empty prefix list omits the claim, so any prefix, even "/", keeps the credential
    // scoped to the keys under it.
    CloudflareR2TemporaryCredentialSigner.Credential cred =
        CloudflareR2TemporaryCredentialSigner.sign(
            request(
                ACCOUNT + ".r2.cloudflarestorage.com",
                List.of("/"),
                CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY));
    assertThat(verify(cred.sessionToken()).getClaim("paths").asMap())
        .containsEntry("prefixPaths", List.of("/"))
        .containsEntry("objectPaths", List.of());
  }

  /**
   * Pins the raw payload of the session token: the claim names, the value of each claim, and the
   * shape of {@code paths}. R2 accepted this format in a live check on 2026-10-07. The test reads
   * the JSON text of the payload, not the decoded view of the JWT library, so a library change in a
   * claim name or in the way a claim is written fails here. {@code aud} is a JSON string, not an
   * array, as the signer writes it with one audience.
   */
  @Test
  void theRawTokenPayloadHasTheShapeThatR2Accepts() throws Exception {
    String host = ACCOUNT + ".r2.cloudflarestorage.com";
    ObjectMapper mapper = new ObjectMapper();

    JsonNode onePrefix =
        mapper.readTree(
            rawPayload(
                CloudflareR2TemporaryCredentialSigner.sign(
                        request(
                            host,
                            List.of("warehouse/db/table/"),
                            CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_WRITE))
                    .sessionToken()));
    JsonNode bucketRoot =
        mapper.readTree(
            rawPayload(
                CloudflareR2TemporaryCredentialSigner.sign(
                        request(
                            host,
                            List.of(),
                            CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY))
                    .sessionToken()));

    assertThat(onePrefix.fieldNames())
        .toIterable()
        .containsExactlyInAnyOrder("sub", "iss", "aud", "iat", "exp", "bucket", "scope", "paths");
    assertThat(onePrefix.get("aud").isTextual()).isTrue();
    assertThat(onePrefix)
        .isEqualTo(
            mapper.readTree(
                """
                {
                  "sub": "%s",
                  "iss": "%s",
                  "aud": "%s",
                  "iat": %d,
                  "exp": %d,
                  "bucket": "my-bucket",
                  "scope": "object-read-write",
                  "paths": {"prefixPaths": ["warehouse/db/table/"], "objectPaths": []}
                }
                """
                    .formatted(
                        ACCOUNT,
                        PARENT_KEY,
                        host,
                        NOW.getEpochSecond(),
                        NOW.plusSeconds(3600).getEpochSecond())));

    // A grant at the bucket root has no paths claim at all. The module scopes that credential to
    // the whole bucket.
    assertThat(bucketRoot.fieldNames())
        .toIterable()
        .containsExactlyInAnyOrder("sub", "iss", "aud", "iat", "exp", "bucket", "scope");
    assertThat(bucketRoot)
        .isEqualTo(
            mapper.readTree(
                """
                {
                  "sub": "%s",
                  "iss": "%s",
                  "aud": "%s",
                  "iat": %d,
                  "exp": %d,
                  "bucket": "my-bucket",
                  "scope": "object-read-only"
                }
                """
                    .formatted(
                        ACCOUNT,
                        PARENT_KEY,
                        host,
                        NOW.getEpochSecond(),
                        NOW.plusSeconds(3600).getEpochSecond())));
  }

  @Test
  void differentSecretsProduceDifferentDerivedSecrets() {
    CloudflareR2TemporaryCredentialSigner.Request a =
        request(
            ACCOUNT + ".r2.cloudflarestorage.com",
            List.of("p/"),
            CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY);
    CloudflareR2TemporaryCredentialSigner.Request b =
        new CloudflareR2TemporaryCredentialSigner.Request(
            a.parentKeyId(),
            "other-secret",
            a.accountId(),
            a.audienceHost(),
            a.bucket(),
            a.prefixes(),
            a.scope(),
            a.ttl(),
            a.now());
    assertThat(CloudflareR2TemporaryCredentialSigner.sign(a).secretAccessKey())
        .isNotEqualTo(CloudflareR2TemporaryCredentialSigner.sign(b).secretAccessKey());
  }

  @Test
  void parentTokenFingerprintIsStableAndShort() {
    String fp = CloudflareR2ParentToken.fingerprint(PARENT_SECRET);
    assertThat(fp).hasSize(8).matches("[0-9a-f]{8}");
    assertThat(fp).isEqualTo(CloudflareR2ParentToken.fingerprint(PARENT_SECRET));
    assertThat(fp).isNotEqualTo(CloudflareR2ParentToken.fingerprint("other"));
    assertThat(new CloudflareR2ParentToken(PARENT_KEY, PARENT_SECRET).toString())
        .doesNotContain(PARENT_SECRET);
  }

  @Test
  void requestToStringOmitsTheParentSecret() {
    CloudflareR2TemporaryCredentialSigner.Request request =
        request(
            ACCOUNT + ".r2.cloudflarestorage.com",
            List.of("p/"),
            CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY);
    assertThat(request.toString()).doesNotContain(PARENT_SECRET).contains(PARENT_KEY, "my-bucket");
  }

  @Test
  void credentialToStringOmitsTheDerivedSecretAndTheSessionToken() {
    CloudflareR2TemporaryCredentialSigner.Credential cred =
        CloudflareR2TemporaryCredentialSigner.sign(
            request(
                ACCOUNT + ".r2.cloudflarestorage.com",
                List.of("p/"),
                CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_WRITE));
    String rawJwt = verify(cred.sessionToken()).getToken();

    assertThat(cred.toString())
        .doesNotContain(cred.secretAccessKey())
        .doesNotContain(cred.sessionToken())
        .doesNotContain(rawJwt)
        .doesNotContain(PARENT_SECRET)
        .contains(PARENT_KEY, cred.expiresAt().toString());
  }

  @Test
  void advertisedExpiryMatchesTheExpClaimExactly() {
    // A now() with sub-second precision: java-jwt floors exp to whole seconds, so the credential
    // must advertise the truncated value rather than the caller's millisecond instant.
    Instant nowWithMillis = NOW.plusMillis(750);
    CloudflareR2TemporaryCredentialSigner.Credential cred =
        CloudflareR2TemporaryCredentialSigner.sign(
            new CloudflareR2TemporaryCredentialSigner.Request(
                PARENT_KEY,
                PARENT_SECRET,
                ACCOUNT,
                ACCOUNT + ".r2.cloudflarestorage.com",
                "my-bucket",
                List.of("p/"),
                CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY,
                Duration.ofSeconds(3600),
                nowWithMillis));

    DecodedJWT jwt = verify(cred.sessionToken());
    assertThat(cred.expiresAt().getNano()).isZero();
    assertThat(cred.expiresAt().getEpochSecond())
        .isEqualTo(jwt.getExpiresAtAsInstant().getEpochSecond());
    assertThat(cred.expiresAt()).isEqualTo(NOW.plusSeconds(3600));
    assertThat(jwt.getIssuedAtAsInstant()).isEqualTo(NOW);
  }

  @Test
  void missingTokenMessageNamesTheProperty() {
    assertThat(CloudflareR2ParentTokenResolver.missingTokenMessage(null))
        .isEqualTo(
            "No default Cloudflare R2 parent token is configured on the server"
                + " (polaris.storage.cloudflare-r2.access-key / secret-key)");
    assertThat(CloudflareR2ParentTokenResolver.missingTokenMessage("research"))
        .isEqualTo(
            "No Cloudflare R2 parent token is configured for storage name 'research'"
                + " (polaris.storage.cloudflare-r2.research.access-key / secret-key)");
  }
}
