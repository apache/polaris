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
package org.apache.polaris.core.storage.gcp;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Produces a GCP federated {@link GoogleCredentials} whose identity carries {@code
 * <realm>/<principal>}, so that GCS Data Access audit logs attribute access to the requesting
 * Polaris principal. This is the GCP counterpart of AWS STS session tags.
 *
 * <p>The federated credential is an {@link IdentityPoolCredentials} backed by a programmatic
 * subject-token supplier: on each token refresh google-auth invokes the supplier, which mints a
 * short-lived RS256 JWT ({@code sub = <realm>/<principal>}, {@code realm} claim), and exchanges it
 * at the Workload Identity Pool provider's STS endpoint. The provider maps {@code google.subject =
 * assertion.sub} and {@code attribute.realm = assertion.realm}; per-realm {@code attribute.realm}
 * IAM bindings then enforce that a realm-A identity can only impersonate realm-A's service account.
 * The returned credential is intended to be used as the source for tenant service-account
 * impersonation (see {@link GcpCredentialsStorageIntegration}).
 *
 * <p>Network note: this performs an STS token exchange against {@code sts.googleapis.com} in
 * addition to the existing {@code iamcredentials.googleapis.com} and {@code storage.googleapis.com}
 * traffic.
 */
public class GcpFederatedCredentialsExchanger {

  static final String STS_TOKEN_URL = "https://sts.googleapis.com/v1/token";
  static final String SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt";
  static final String CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

  /** Attribution JWTs are single-purpose and short-lived. */
  static final Duration JWT_LIFETIME = Duration.ofMinutes(5);

  /**
   * JVM-wide cache of parsed signing keys, keyed by file path. The key file is a stable pod-mounted
   * secret; parsing it (disk read + {@link KeyFactory}) once per path amortizes across vends rather
   * than re-reading on every credential-cache miss. Key rotation is delivered by a process restart
   * (the secret is mounted at startup), which clears this cache. In practice the map is bounded by
   * the number of realms in the deployment — each realm has at most one signing-key path — so
   * memory growth is proportional to realm count.
   */
  private static final ConcurrentHashMap<Path, RSAPrivateKey> SIGNING_KEY_CACHE =
      new ConcurrentHashMap<>();

  private final String issuer;
  private final String wifAudience;
  private final Path signingKeyPath;
  private final String signingKeyId;
  private final HttpTransportFactory transportFactory;

  public GcpFederatedCredentialsExchanger(
      String issuer,
      String wifAudience,
      Path signingKeyPath,
      String signingKeyId,
      HttpTransportFactory transportFactory) {
    this.issuer = issuer;
    this.wifAudience = wifAudience;
    this.signingKeyPath = signingKeyPath;
    this.signingKeyId = signingKeyId;
    this.transportFactory = transportFactory;
  }

  /**
   * Builds a federated credential whose subject is {@code <realm>/<principal>}.
   *
   * @param subject the attribution subject, {@code <realm>/<principal>} (see {@link
   *     GcpAttributionSubjectBuilder})
   * @param realm the realm identifier, emitted as the {@code realm} claim for {@code
   *     attribute.realm} mapping
   * @return federated credentials suitable as the source for tenant-SA impersonation
   */
  public GoogleCredentials federatedCredentials(String subject, String realm) {
    return IdentityPoolCredentials.newBuilder()
        .setHttpTransportFactory(transportFactory)
        .setAudience(wifAudience)
        .setSubjectTokenType(SUBJECT_TOKEN_TYPE)
        .setTokenUrl(STS_TOKEN_URL)
        .setScopes(List.of(CLOUD_PLATFORM_SCOPE))
        .setSubjectTokenSupplier(context -> mintAttributionJwt(subject, realm))
        .build();
  }

  @VisibleForTesting
  String mintAttributionJwt(String subject, String realm) throws IOException {
    Instant now = Instant.now();
    JWTCreator.Builder builder =
        JWT.create()
            .withIssuer(issuer)
            .withSubject(subject)
            .withAudience(wifAudience)
            .withClaim("realm", realm)
            .withIssuedAt(Date.from(now))
            .withExpiresAt(Date.from(now.plus(JWT_LIFETIME)))
            .withJWTId(UUID.randomUUID().toString());
    // Set the kid header so the WIF provider can pick the right public key from its JWKS during
    // rotation (when the JWKS holds both the old and new keys). Omitted only for a single-key JWKS.
    if (signingKeyId != null && !signingKeyId.isEmpty()) {
      builder.withKeyId(signingKeyId);
    }
    // null public key: sign-only; verification is the WIF provider's responsibility
    return builder.sign(Algorithm.RSA256(null, loadSigningKey()));
  }

  private RSAPrivateKey loadSigningKey() throws IOException {
    try {
      return SIGNING_KEY_CACHE.computeIfAbsent(
          signingKeyPath,
          path -> {
            try {
              return readPkcs8PrivateKey(path);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  /** Reads an RSA private key from a PKCS#8 PEM file. */
  @VisibleForTesting
  static RSAPrivateKey readPkcs8PrivateKey(Path pemPath) throws IOException {
    String pem = Files.readString(pemPath);
    if (pem.contains("BEGIN RSA PRIVATE KEY")) {
      throw new IOException(
          "PKCS#1 key format (BEGIN RSA PRIVATE KEY) is not supported for GCS attribution from "
              + pemPath
              + "; convert to PKCS#8 with: openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem");
    }
    String base64 =
        pem.replaceAll("-----BEGIN [A-Z ]+-----", "")
            .replaceAll("-----END [A-Z ]+-----", "")
            .replaceAll("\\s", "");
    try {
      byte[] der = Base64.getDecoder().decode(base64);
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      return (RSAPrivateKey) keyFactory.generatePrivate(new PKCS8EncodedKeySpec(der));
    } catch (Exception e) {
      throw new IOException(
          "Unable to read PKCS#8 RSA private key for GCS attribution from " + pemPath, e);
    }
  }
}
