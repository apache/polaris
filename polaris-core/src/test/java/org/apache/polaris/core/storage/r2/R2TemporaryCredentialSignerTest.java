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

import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
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

class R2TemporaryCredentialSignerTest {

  private static final String ACCOUNT = "0123456789abcdef0123456789abcdef";
  private static final String PARENT_KEY = "parent-access-key-id";
  private static final String PARENT_SECRET = "not-a-real-secret-0123456789abcdef";
  private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.SECONDS);

  private static R2TemporaryCredentialSigner.Request request(
      String host, List<String> prefixes, String scope) {
    return new R2TemporaryCredentialSigner.Request(
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
    assertThat(decoded).startsWith(R2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX);
    String jwt = decoded.substring(R2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX.length());
    return JWT.require(Algorithm.HMAC256(PARENT_SECRET)).build().verify(jwt);
  }

  @Test
  void signsDefaultJurisdictionCredential() throws Exception {
    R2TemporaryCredentialSigner.Credential cred =
        R2TemporaryCredentialSigner.sign(
            request(
                ACCOUNT + ".r2.cloudflarestorage.com",
                List.of("warehouse/db/table/"),
                R2TemporaryCredentialSigner.SCOPE_OBJECT_READ_WRITE));

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
            .substring(R2TemporaryCredentialSigner.SESSION_TOKEN_PREFIX.length());
    byte[] digest =
        MessageDigest.getInstance("SHA-256").digest(rawJwt.getBytes(StandardCharsets.UTF_8));
    assertThat(cred.secretAccessKey()).isEqualTo(HexFormat.of().formatHex(digest));
  }

  static List<String> knownJurisdictions() {
    return R2StorageConfigurationInfo.KNOWN_JURISDICTIONS;
  }

  @ParameterizedTest
  @MethodSource("knownJurisdictions")
  void usesJurisdictionHostAsAudience(String jurisdiction) {
    String host = ACCOUNT + "." + jurisdiction + ".r2.cloudflarestorage.com";
    R2TemporaryCredentialSigner.Credential cred =
        R2TemporaryCredentialSigner.sign(
            request(host, List.of("p/"), R2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY));
    DecodedJWT jwt = verify(cred.sessionToken());
    assertThat(jwt.getAudience()).containsExactly(host);
    assertThat(jwt.getClaim("scope").asString()).isEqualTo("object-read-only");
  }

  @Test
  void omitsPathsClaimForBucketRootGrant() {
    R2TemporaryCredentialSigner.Credential cred =
        R2TemporaryCredentialSigner.sign(
            request(
                ACCOUNT + ".r2.cloudflarestorage.com",
                List.of(),
                R2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY));
    assertThat(verify(cred.sessionToken()).getClaim("paths").isMissing()).isTrue();
  }

  @Test
  void aSingleSlashPrefixStillEmitsThePathsClaim() {
    // The prefix for a grant at s3://bucket// is "/", which is not the whole bucket. Only an empty
    // prefix list omits the claim, so this credential stays scoped to keys under the doubled slash.
    R2TemporaryCredentialSigner.Credential cred =
        R2TemporaryCredentialSigner.sign(
            request(
                ACCOUNT + ".r2.cloudflarestorage.com",
                List.of("/"),
                R2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY));
    assertThat(verify(cred.sessionToken()).getClaim("paths").asMap())
        .containsEntry("prefixPaths", List.of("/"))
        .containsEntry("objectPaths", List.of());
  }

  @Test
  void differentSecretsProduceDifferentDerivedSecrets() {
    R2TemporaryCredentialSigner.Request a =
        request(
            ACCOUNT + ".r2.cloudflarestorage.com",
            List.of("p/"),
            R2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY);
    R2TemporaryCredentialSigner.Request b =
        new R2TemporaryCredentialSigner.Request(
            a.parentKeyId(),
            "other-secret",
            a.accountId(),
            a.audienceHost(),
            a.bucket(),
            a.prefixes(),
            a.scope(),
            a.ttl(),
            a.now());
    assertThat(R2TemporaryCredentialSigner.sign(a).secretAccessKey())
        .isNotEqualTo(R2TemporaryCredentialSigner.sign(b).secretAccessKey());
  }

  @Test
  void parentTokenFingerprintIsStableAndShort() {
    String fp = R2ParentToken.fingerprint(PARENT_SECRET);
    assertThat(fp).hasSize(8).matches("[0-9a-f]{8}");
    assertThat(fp).isEqualTo(R2ParentToken.fingerprint(PARENT_SECRET));
    assertThat(fp).isNotEqualTo(R2ParentToken.fingerprint("other"));
    assertThat(new R2ParentToken(PARENT_KEY, PARENT_SECRET).toString())
        .doesNotContain(PARENT_SECRET);
  }

  @Test
  void noneResolverResolvesNothing() {
    assertThat(R2ParentTokenResolver.none().resolve(null)).isEmpty();
    assertThat(R2ParentTokenResolver.none().resolve("x")).isEmpty();
  }

  @Test
  void requestToStringOmitsTheParentSecret() {
    R2TemporaryCredentialSigner.Request request =
        request(
            ACCOUNT + ".r2.cloudflarestorage.com",
            List.of("p/"),
            R2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY);
    assertThat(request.toString()).doesNotContain(PARENT_SECRET).contains(PARENT_KEY, "my-bucket");
  }

  @Test
  void advertisedExpiryMatchesTheExpClaimExactly() {
    // A now() with sub-second precision: java-jwt floors exp to whole seconds, so the credential
    // must advertise the truncated value rather than the caller's millisecond instant.
    Instant nowWithMillis = NOW.plusMillis(750);
    R2TemporaryCredentialSigner.Credential cred =
        R2TemporaryCredentialSigner.sign(
            new R2TemporaryCredentialSigner.Request(
                PARENT_KEY,
                PARENT_SECRET,
                ACCOUNT,
                ACCOUNT + ".r2.cloudflarestorage.com",
                "my-bucket",
                List.of("p/"),
                R2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY,
                Duration.ofSeconds(3600),
                nowWithMillis));

    DecodedJWT jwt = verify(cred.sessionToken());
    assertThat(cred.expiresAt().getNano()).isZero();
    assertThat(cred.expiresAt().getEpochSecond())
        .isEqualTo(jwt.getExpiresAtAsInstant().getEpochSecond());
    assertThat(cred.expiresAt()).isEqualTo(NOW.plusSeconds(3600));
    assertThat(jwt.getIssuedAtAsInstant()).isEqualTo(NOW);
  }
}
