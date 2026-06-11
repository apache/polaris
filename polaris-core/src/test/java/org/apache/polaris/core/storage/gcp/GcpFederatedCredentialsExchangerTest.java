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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentials;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GcpFederatedCredentialsExchangerTest {

  private static final String ISSUER = "https://catalog.example.org/attribution";
  private static final String AUDIENCE =
      "//iam.googleapis.com/projects/123456/locations/global/workloadIdentityPools/polaris/providers/catalog";
  private static final String KEY_ID = "attribution-key-1";
  private static final HttpTransportFactory TRANSPORT = NetHttpTransport::new;

  @TempDir Path tempDir;
  private Path keyFile;

  @BeforeEach
  void writeSigningKey() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    KeyPair keyPair = generator.generateKeyPair();
    String pem =
        "-----BEGIN PRIVATE KEY-----\n"
            + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8))
                .encodeToString(keyPair.getPrivate().getEncoded())
            + "\n-----END PRIVATE KEY-----\n";
    keyFile = tempDir.resolve("attribution-key.pem");
    Files.writeString(keyFile, pem);
  }

  private GcpFederatedCredentialsExchanger exchanger(String keyId) {
    return new GcpFederatedCredentialsExchanger(ISSUER, AUDIENCE, keyFile, keyId, TRANSPORT);
  }

  @Test
  void attributionJwtCarriesExpectedClaimsAndKid() throws IOException {
    DecodedJWT jwt =
        JWT.decode(exchanger(KEY_ID).mintAttributionJwt("tenant1/etl_writer", "tenant1"));

    assertThat(jwt.getIssuer()).isEqualTo(ISSUER);
    assertThat(jwt.getSubject()).isEqualTo("tenant1/etl_writer");
    assertThat(jwt.getAudience()).containsExactly(AUDIENCE);
    assertThat(jwt.getClaim("realm").asString()).isEqualTo("tenant1");
    assertThat(jwt.getAlgorithm()).isEqualTo("RS256");
    assertThat(jwt.getKeyId()).isEqualTo(KEY_ID);
    assertThat(jwt.getId()).isNotBlank();
    assertThat(jwt.getExpiresAt()).isAfter(jwt.getIssuedAt());
  }

  @Test
  void emptyKeyIdOmitsKidHeader() throws IOException {
    DecodedJWT jwt = JWT.decode(exchanger("").mintAttributionJwt("tenant1/p", "tenant1"));
    assertThat(jwt.getKeyId()).isNull();
  }

  @Test
  void federatedCredentialsConfiguredForStsExchange() {
    GoogleCredentials credentials = exchanger(KEY_ID).federatedCredentials("tenant1/p", "tenant1");
    assertThat(credentials).isInstanceOf(IdentityPoolCredentials.class);
    IdentityPoolCredentials idp = (IdentityPoolCredentials) credentials;
    assertThat(idp.getAudience()).isEqualTo(AUDIENCE);
    assertThat(idp.getSubjectTokenType())
        .isEqualTo(GcpFederatedCredentialsExchanger.SUBJECT_TOKEN_TYPE);
  }

  @Test
  void missingKeyFileFails() {
    GcpFederatedCredentialsExchanger exchanger =
        new GcpFederatedCredentialsExchanger(
            ISSUER, AUDIENCE, tempDir.resolve("nope.pem"), KEY_ID, TRANSPORT);
    assertThatThrownBy(() -> exchanger.mintAttributionJwt("tenant1/p", "tenant1"))
        .isInstanceOf(IOException.class);
  }

  @Test
  void signingKeyIsCachedAcrossInstances() throws IOException {
    // First use parses and caches the key for this (unique) path.
    exchanger(KEY_ID).mintAttributionJwt("tenant1/p", "tenant1");
    // After the file is gone, a second instance on the same path still mints from the JVM cache,
    // proving the parse is amortized rather than re-read per vend.
    Files.delete(keyFile);
    assertThatNoException()
        .isThrownBy(() -> exchanger(KEY_ID).mintAttributionJwt("tenant1/p", "tenant1"));
  }

  @Test
  void readPkcs8PrivateKey() throws IOException {
    assertThat(GcpFederatedCredentialsExchanger.readPkcs8PrivateKey(keyFile)).isNotNull();
  }
}
